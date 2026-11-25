from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from restaurant_db import Database
from restaurant_db.errors import (AlreadyOpen, CorruptJournal, IdempotencyConflict,
                                 OwnershipError, StorageUnavailable, ValidationError)
from restaurant_db.journal import canonical_json
from tests.support import test_directory


def slot(slot_id="s1", table_id="t1", start="2030-01-01T18:00:00+02:00",
         end="2030-01-01T20:00:00+02:00"):
    return {"slot_id": slot_id, "table_id": table_id, "starts_at": start, "ends_at": end}


def prepare(db, slots=None):
    db.execute("table", {"op": "add_table", "table_id": "t1", "name": "Window"})
    return db.execute("batch", {"op": "prepare_batch", "batch_id": "b1",
                                "slots": [slot()] if slots is None else slots})


def publish(db):
    return db.execute("publish", {"op": "publish_batch", "batch_id": "b1"})


def reserve(user="alice", slot_id="s1"):
    return {"op": "reserve", "slot_id": slot_id, "user_id": user}


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self)

    def test_draft_is_hidden_and_cannot_be_reserved(self):
        with Database(self.path) as db:
            prepare(db)
            self.assertEqual([], db.list_slots())
            self.assertEqual("batch_not_published", db.execute("too-early", reserve())["code"])
            publish(db)
            self.assertTrue(db.list_slots()[0]["available"])
            # A NEW attempt is required: the earlier rejection remains stable.
            self.assertEqual("batch_not_published", db.execute("too-early", reserve())["code"])
            self.assertEqual("reserved", db.execute("after-publication", reserve())["code"])

    def test_one_winner_and_independent_slots(self):
        with Database(self.path) as db:
            prepare(db, [slot(), slot("s2", start="2030-01-01T20:00:00+02:00",
                                     end="2030-01-01T22:00:00+02:00")])
            publish(db)
            first = db.execute("alice", reserve())
            loser = db.execute("bob", reserve("bob"))
            other = db.execute("second-slot", reserve("alice", "s2"))
            self.assertEqual("reserved", first["code"])
            self.assertEqual("slot_taken", loser["code"])
            self.assertEqual("reserved", other["code"])
            self.assertEqual(2, len(db.snapshot()["reservations"]))

    def test_same_request_is_not_written_twice_and_survives_restart(self):
        with Database(self.path) as db:
            prepare(db)
            publish(db)
            original = db.execute("book", reserve())
            before = (self.path / "transactions.jsonl").read_bytes()
            self.assertEqual(original, db.execute("book", reserve()))
            self.assertEqual(before, (self.path / "transactions.jsonl").read_bytes())
            # Mutating a returned object cannot mutate the idempotency cache.
            changed = db.execute("book", reserve())
            changed["reservation"]["user_id"] = "changed"
            self.assertEqual(original, db.execute("book", reserve()))
        with Database(self.path) as db:
            self.assertEqual(original, db.execute("book", reserve()))
            self.assertEqual(1, len(db.snapshot()["reservations"]))
            with self.assertRaises(IdempotencyConflict):
                db.execute("book", reserve("someone-else"))

    def test_failed_batch_does_not_leave_partial_slots(self):
        with Database(self.path) as db:
            db.execute("table", {"op": "add_table", "table_id": "t1", "name": "Window"})
            result = db.execute("bad-batch", {"op": "prepare_batch", "batch_id": "bad", "slots": [
                slot(), slot("s2", table_id="missing-table")]})
            self.assertEqual("table_not_found", result["code"])
            self.assertEqual([], db.snapshot()["slots"])
            self.assertEqual([], db.snapshot()["batches"])
        with Database(self.path) as db:
            self.assertEqual([], db.snapshot()["slots"])

    def test_overlaps_are_rejected_within_and_across_batches(self):
        with Database(self.path) as db:
            result = prepare(db, [slot(), slot("s2", start="2030-01-01T19:00:00+02:00")])
            self.assertEqual("slots_overlap", result["code"])
            self.assertEqual([], db.snapshot()["slots"])
            db.execute("good", {"op": "prepare_batch", "batch_id": "good", "slots": [slot()]})
            result = db.execute("overlap", {"op": "prepare_batch", "batch_id": "other", "slots": [
                slot("s3", start="2030-01-01T17:00:00Z", end="2030-01-01T19:00:00Z")]})
            self.assertEqual("slots_overlap", result["code"])
            self.assertEqual(1, len(db.snapshot()["slots"]))

    def test_invalid_input_never_reaches_the_journal(self):
        with Database(self.path) as db:
            before = (self.path / "transactions.jsonl").read_bytes()
            cases = [
                {"op": "cancel"},
                {"op": "reserve", "slot_id": "s1", "user_id": ""},
                {"op": "reserve", "slot_id": "s1", "user_id": "\ud800"},
                {"op": "reserve", "slot_id": "s1", "user_id": "alice", "typo": True},
                {"op": "prepare_batch", "batch_id": "b", "slots": [slot(start="2030-01-01T18:00:00")]},
                {"op": "prepare_batch", "batch_id": "b", "slots": [slot(end="2030-01-01T17:00:00+02:00")]},
            ]
            for command in cases:
                with self.subTest(command=repr(command)):
                    with self.assertRaises(ValidationError):
                        db.execute("bad", command)
            self.assertEqual(before, (self.path / "transactions.jsonl").read_bytes())
            self.assertTrue(db.execute("good", {"op": "add_table", "table_id": "t1", "name": "OK"})["ok"])

    def test_equivalent_timezones_and_slot_order_share_a_request(self):
        with Database(self.path) as db:
            first = slot()
            second = slot("s2", start="2030-01-01T20:00:00+02:00", end="2030-01-01T22:00:00+02:00")
            result = prepare(db, [first, second])
            equivalent = deepcopy(first)
            equivalent.update(starts_at="2030-01-01T16:00:00Z", ends_at="2030-01-01T18:00:00Z")
            self.assertEqual(result, db.execute("batch", {"op": "prepare_batch", "batch_id": "b1",
                                                         "slots": [second, equivalent]}))

    def test_nonowner_thread_is_rejected(self):
        with Database(self.path) as db, ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(db.execute, "book", reserve())
            with self.assertRaises(OwnershipError):
                future.result()
            self.assertEqual(0, db.snapshot()["request_count"])

    def test_second_owner_is_rejected_and_lock_is_released(self):
        with Database(self.path):
            with self.assertRaises(AlreadyOpen):
                Database(self.path)
            attempt = subprocess.run([sys.executable, "-c",
                "from restaurant_db import Database; import sys; Database(sys.argv[1])", str(self.path)],
                capture_output=True, text=True, timeout=10)
            self.assertNotEqual(0, attempt.returncode)
            self.assertIn("AlreadyOpen", attempt.stderr)
        with Database(self.path) as db:
            self.assertEqual([], db.list_slots())


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self)

    def test_publication_is_all_or_nothing_at_every_byte_boundary(self):
        source = self.path / "source"
        with Database(source) as db:
            prepare(db, [slot(), slot("s2", start="2030-01-01T20:00:00+02:00",
                                     end="2030-01-01T22:00:00+02:00")])
            publish(db)
        lines = (source / "transactions.jsonl").read_bytes().splitlines(keepends=True)
        prefix, publication = b"".join(lines[:-1]), lines[-1]
        target = self.path / "recovered"
        target.mkdir()
        for cut in range(len(publication) + 1):
            with self.subTest(cut=cut):
                (target / "transactions.jsonl").write_bytes(prefix + publication[:cut])
                with Database(target) as db:
                    expected = 2 if cut == len(publication) else 0
                    self.assertEqual(expected, len(db.list_slots()))
                    self.assertEqual(2, len(db.snapshot()["slots"]))
                    self.assertEqual(cut if cut < len(publication) else 0, db.recovered_tail_bytes)

    def test_partial_reservation_is_discarded_and_can_be_retried(self):
        with Database(self.path) as db:
            prepare(db)
            publish(db)
            db.execute("book", reserve())
        path = self.path / "transactions.jsonl"
        lines = path.read_bytes().splitlines(keepends=True)
        path.write_bytes(b"".join(lines[:-1]) + lines[-1][:-1])
        with Database(self.path) as db:
            self.assertTrue(db.list_slots()[0]["available"])
            self.assertEqual("reserved", db.execute("book", reserve())["code"])

    def test_complete_corrupt_record_is_not_silently_truncated(self):
        with Database(self.path) as db:
            prepare(db)
        path = self.path / "transactions.jsonl"
        envelope = json.loads(path.read_bytes().splitlines()[-1])
        envelope["transaction"]["command"]["batch_id"] = "corrupted"
        lines = path.read_bytes().splitlines(keepends=True)
        damaged = b"".join(lines[:-1]) + canonical_json(envelope) + b"\n"
        path.write_bytes(damaged)
        with self.assertRaises(CorruptJournal):
            Database(self.path)
        self.assertEqual(damaged, path.read_bytes())

    def test_checksummed_but_invalid_transaction_is_rejected(self):
        transaction = {"version": 2, "request_id": "future", "at": "2030-01-01T00:00:00+00:00",
                       "command": reserve(), "result": {"ok": True}}
        envelope = {"sha256": hashlib.sha256(canonical_json(transaction)).hexdigest(),
                    "transaction": transaction}
        (self.path / "transactions.jsonl").write_bytes(canonical_json(envelope) + b"\n")
        with self.assertRaises(CorruptJournal):
            Database(self.path)

    def test_fsync_failure_poisoning_and_unknown_outcome(self):
        with Database(self.path) as db:
            prepare(db)
            publish(db)
            with patch("restaurant_db.journal.os.fsync", side_effect=OSError("disk failure")):
                with self.assertRaises(StorageUnavailable):
                    db.execute("book", reserve())
            with self.assertRaises(StorageUnavailable):
                db.execute("other", reserve("bob"))
            with self.assertRaises(StorageUnavailable):
                db.list_slots()
        # flush completed before fsync failed, so this particular outcome exists.
        with Database(self.path) as db:
            self.assertEqual("reserved", db.execute("book", reserve())["code"])
            self.assertEqual(1, len(db.snapshot()["reservations"]))

    def test_process_death_after_durable_publication_before_memory_update(self):
        with Database(self.path) as db:
            prepare(db, [slot(), slot("s2", start="2030-01-01T20:00:00+02:00",
                                     end="2030-01-01T22:00:00+02:00")])
        code = """
import os, sys
from restaurant_db import Database
db = Database(sys.argv[1])
def stop_before_apply(command, result):
    os._exit(23)
db._apply = stop_before_apply
db.execute('publish', {'op': 'publish_batch', 'batch_id': 'b1'})
"""
        child = subprocess.run([sys.executable, "-c", code, str(self.path)],
                               capture_output=True, text=True, timeout=10)
        self.assertEqual(23, child.returncode, child.stderr)
        with Database(self.path) as db:
            self.assertEqual(2, len(db.list_slots()))
            self.assertEqual("batch_published", publish(db)["code"])

    def test_killing_process_preserves_acknowledged_reservation_and_releases_lock(self):
        with Database(self.path) as db:
            prepare(db)
            publish(db)
        code = """
import json, sys
from restaurant_db import Database
db = Database(sys.argv[1])
print(json.dumps(db.execute('book', {'op':'reserve','slot_id':'s1','user_id':'alice'})), flush=True)
sys.stdin.readline()
"""
        child = subprocess.Popen([sys.executable, "-c", code, str(self.path)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        try:
            line = child.stdout.readline()
            self.assertTrue(line, "child exited before acknowledging the transaction")
            acknowledged = json.loads(line)
            child.kill()
            child.wait(timeout=10)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            child.communicate(timeout=10)
        with Database(self.path) as db:
            self.assertEqual(acknowledged, db.execute("book", reserve()))
            self.assertFalse(db.list_slots()[0]["available"])


if __name__ == "__main__":
    unittest.main()

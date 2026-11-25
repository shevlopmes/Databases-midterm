import json
import subprocess
import sys
import unittest
from unittest.mock import patch

from restaurant_db import Database
from restaurant_db.errors import ReadOnlyError, StorageUnavailable, ValidationError
from tests.support import test_directory
from tests.test_engine import prepare, publish, reserve


def cancel(reservation_id=1, user_id="alice"):
    return {"op": "cancel", "reservation_id": reservation_id, "user_id": user_id}


class CancellationTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self)
        with Database(self.path) as db:
            prepare(db)
            publish(db)
            self.booked = db.execute("book", reserve())

    def test_cancel_frees_slot_and_retains_history(self):
        with Database(self.path) as db:
            result = db.execute("cancel", cancel())
            self.assertEqual("cancelled", result["code"])
            self.assertTrue(result["ok"])
            self.assertEqual(1, result["reservation_id"])
            self.assertTrue(db.list_slots()[0]["available"])
            state = db.snapshot()
            self.assertEqual({}, state["active_reservations"])
            self.assertEqual(1, len(state["reservations"]))
            self.assertEqual(result["cancelled_at"], state["reservations"][0]["cancelled_at"])
            next_booking = db.execute("next", reserve("bob"))
            self.assertEqual(2, next_booking["reservation"]["reservation_id"])
            self.assertEqual({"s1": 2}, db.snapshot()["active_reservations"])

    def test_only_owner_can_cancel_and_missing_reservation_is_rejected(self):
        with Database(self.path) as db:
            self.assertEqual("reservation_not_owner", db.execute("wrong-owner", cancel(user_id="bob"))["code"])
            self.assertEqual("reservation_not_found", db.execute("missing", cancel(99))["code"])
            self.assertFalse(db.list_slots()[0]["available"])
            self.assertIsNone(db.snapshot()["reservations"][0]["cancelled_at"])
            self.assertEqual("cancelled", db.execute("owner", cancel())["code"])

    def test_old_cancellations_never_release_a_new_owners_reservation(self):
        with Database(self.path) as db:
            first = db.execute("cancel", cancel())
            journal = self.path / "transactions.jsonl"
            before = journal.read_bytes()
            self.assertEqual(first, db.execute("cancel", cancel()))
            self.assertEqual(before, journal.read_bytes())
            db.execute("bob", reserve("bob"))
            self.assertEqual("reservation_already_cancelled", db.execute("late-new-key", cancel())["code"])
            self.assertEqual(first, db.execute("cancel", cancel()))
            self.assertEqual({"s1": 2}, db.snapshot()["active_reservations"])
        with Database(self.path) as db:
            self.assertEqual(first, db.execute("cancel", cancel()))
            self.assertEqual({"s1": 2}, db.snapshot()["active_reservations"])

    def test_old_booking_retries_do_not_reactivate_or_claim_a_freed_slot(self):
        with Database(self.path) as db:
            rejected = db.execute("loser", reserve("bob"))
            db.execute("cancel", cancel())
            self.assertEqual(self.booked, db.execute("book", reserve()))
            self.assertEqual(rejected, db.execute("loser", reserve("bob")))
            self.assertTrue(db.list_slots()[0]["available"])
            self.assertEqual("reserved", db.execute("new-attempt", reserve("bob"))["code"])
            self.assertEqual(self.booked, db.execute("book", reserve()))
            self.assertEqual({"s1": 2}, db.snapshot()["active_reservations"])

    def test_cancellation_survives_restart_and_check_counts_it(self):
        with Database(self.path) as db:
            result = db.execute("cancel", cancel())
        report = Database.check(self.path)
        self.assertEqual("ok", report["status"])
        self.assertEqual(1, report["state"]["reservations"])
        self.assertEqual(0, report["state"]["active_reservations"])
        self.assertEqual(1, report["state"]["cancelled_reservations"])
        with Database(self.path) as db:
            self.assertEqual(result, db.execute("cancel", cancel()))
            self.assertTrue(db.list_slots()[0]["available"])
            self.assertEqual(2, db.execute("next", reserve())["reservation"]["reservation_id"])

    def test_invalid_cancellation_never_reaches_journal(self):
        with Database(self.path) as db:
            journal = self.path / "transactions.jsonl"
            before = journal.read_bytes()
            cases = [cancel(True), cancel(False), cancel(1.0), cancel("1"), cancel(0), cancel(-1),
                     cancel(user_id=""), {"op": "cancel", "slot_id": "s1", "user_id": "alice"},
                     {"op": "cancel", "reservation_id": 1}]
            for command in cases:
                with self.subTest(command=command):
                    with self.assertRaises(ValidationError):
                        db.execute("bad", command)
            self.assertEqual(before, journal.read_bytes())
            self.assertFalse(db.list_slots()[0]["available"])
            self.assertEqual("cancelled", db.execute("good", cancel())["code"])

    def test_diagnostic_mode_cannot_cancel(self):
        with Database(self.path, read_only=True) as db:
            with self.assertRaises(ReadOnlyError):
                db.execute("cancel", cancel())
            self.assertFalse(db.list_slots()[0]["available"])

    def test_existing_v1_booking_records_and_cached_responses_remain_compatible(self):
        records = [json.loads(line)["transaction"]
                   for line in (self.path / "transactions.jsonl").read_bytes().splitlines()]
        self.assertTrue(all(record["version"] == 1 for record in records))
        self.assertEqual({"reservation_id", "slot_id", "user_id", "created_at"},
                         set(records[-1]["result"]["reservation"]))
        with Database(self.path) as db:
            self.assertEqual(self.booked, db.execute("book", reserve()))
            db.execute("cancel", cancel())
        with Database(self.path) as db:
            self.assertEqual(self.booked, db.execute("book", reserve()))
            self.assertTrue(db.list_slots()[0]["available"])


class CancellationRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self)
        with Database(self.path) as db:
            prepare(db)
            publish(db)
            db.execute("book", reserve())

    def test_cancellation_is_atomic_at_every_byte_boundary(self):
        with Database(self.path) as db:
            db.execute("cancel", cancel())
        lines = (self.path / "transactions.jsonl").read_bytes().splitlines(keepends=True)
        prefix, cancellation = b"".join(lines[:-1]), lines[-1]
        recovered = self.path / "recovered"
        recovered.mkdir()
        for cut in range(len(cancellation) + 1):
            with self.subTest(cut=cut):
                (recovered / "transactions.jsonl").write_bytes(prefix + cancellation[:cut])
                with Database(recovered) as db:
                    complete = cut == len(cancellation)
                    self.assertEqual(complete, db.list_slots()[0]["available"])
                    state = db.snapshot()
                    self.assertEqual(1, len(state["reservations"]))
                    self.assertEqual(complete, state["reservations"][0]["cancelled_at"] is not None)
                    self.assertEqual({} if complete else {"s1": 1}, state["active_reservations"])

    def test_cancel_fsync_failure_is_uncertain_until_recovery(self):
        with Database(self.path) as db:
            with patch("restaurant_db.journal.os.fsync", side_effect=OSError("disk failure")):
                with self.assertRaises(StorageUnavailable):
                    db.execute("cancel", cancel())
            with self.assertRaises(StorageUnavailable):
                db.list_slots()
        # flush completed, so this uncertain outcome happens to be a full record.
        with Database(self.path) as db:
            self.assertEqual("cancelled", db.execute("cancel", cancel())["code"])
            self.assertTrue(db.list_slots()[0]["available"])
            self.assertEqual("reserved", db.execute("next", reserve("bob"))["code"])

    def test_process_death_after_durable_cancel_before_memory_update(self):
        code = """
import os, sys
from restaurant_db import Database
db = Database(sys.argv[1])
def stop_before_apply(command, result):
    os._exit(23)
db._apply = stop_before_apply
db.execute('cancel', {'op': 'cancel', 'reservation_id': 1, 'user_id': 'alice'})
"""
        child = subprocess.run([sys.executable, "-c", code, str(self.path)],
                               capture_output=True, text=True, timeout=10)
        self.assertEqual(23, child.returncode, child.stderr)
        with Database(self.path) as db:
            self.assertTrue(db.list_slots()[0]["available"])
            self.assertEqual("cancelled", db.execute("cancel", cancel())["code"])
            self.assertEqual(2, db.execute("next", reserve("bob"))["reservation"]["reservation_id"])

    def test_cli_cancel_retry_inspect_and_check(self):
        command_file = self.path / "cancel.json"
        command_file.write_text(json.dumps(cancel()), encoding="utf-8")
        args = [sys.executable, "-m", "restaurant_db", "command", "--data", str(self.path),
                "--request-id", "cancel", "--file", str(command_file)]
        first = subprocess.run(args, capture_output=True, text=True, timeout=10)
        self.assertEqual(0, first.returncode, first.stderr)
        self.assertEqual("cancelled", json.loads(first.stdout)["code"])
        retry = subprocess.run(args, capture_output=True, text=True, timeout=10)
        self.assertEqual(0, retry.returncode, retry.stderr)
        self.assertEqual(json.loads(first.stdout), json.loads(retry.stdout))
        inspected = subprocess.run([sys.executable, "-m", "restaurant_db", "inspect", "--data", str(self.path)],
                                   capture_output=True, text=True, timeout=10)
        self.assertEqual(0, inspected.returncode, inspected.stderr)
        self.assertIsNotNone(json.loads(inspected.stdout)["reservations"][0]["cancelled_at"])
        checked = subprocess.run([sys.executable, "-m", "restaurant_db", "check", "--data", str(self.path)],
                                 capture_output=True, text=True, timeout=10)
        self.assertEqual(0, checked.returncode, checked.stderr)
        self.assertEqual(0, json.loads(checked.stdout)["state"]["active_reservations"])
        self.assertEqual(1, json.loads(checked.stdout)["state"]["cancelled_reservations"])


if __name__ == "__main__":
    unittest.main()

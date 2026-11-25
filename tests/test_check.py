import hashlib
import json
import subprocess
import sys
import unittest
from unittest.mock import patch

from restaurant_db import Database
from restaurant_db.errors import AlreadyOpen, CorruptJournal, ReadOnlyError
from restaurant_db.journal import canonical_json
from tests.support import test_directory
from tests.test_engine import prepare, publish, reserve


class CheckTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self)
        with Database(self.path) as db:
            prepare(db)
            publish(db)
            self.booked = db.execute("book", reserve())
            db.execute("loser", reserve("bob"))
        self.journal = self.path / "transactions.jsonl"

    def file_contents(self):
        return {path.name: (path.read_bytes(), path.stat().st_mtime_ns)
                for path in self.path.iterdir()}

    def run_check(self, directory=None):
        return subprocess.run([sys.executable, "-m", "restaurant_db", "check", "--data",
                               str(directory or self.path)],
                              capture_output=True, text=True, timeout=10)

    def test_valid_journal_report_does_not_modify_or_sync_files(self):
        before = self.file_contents()
        with patch("restaurant_db.journal.os.fsync", side_effect=AssertionError("read-only operation")):
            report = Database.check(self.path)
        self.assertEqual("ok", report["status"])
        self.assertTrue(report["read_only"])
        self.assertEqual(5, report["journal"]["complete_records"])
        self.assertEqual(self.journal.stat().st_size, report["journal"]["valid_bytes"])
        self.assertEqual(0, report["journal"]["incomplete_tail_bytes"])
        self.assertEqual({"tables": 1, "batches": 1, "published_batches": 1, "slots": 1,
                          "reservations": 1, "cancelled_reservations": 0,
                          "active_reservations": 1, "requests": 5}, report["state"])
        self.assertEqual(before, self.file_contents())

    def test_incomplete_tail_is_reported_before_explicit_writable_recovery(self):
        prefix = self.journal.read_bytes()
        tail = b'{"transaction":{"request_id":"interrupted'
        self.journal.write_bytes(prefix + tail)
        before = self.file_contents()
        report = Database.check(self.path)
        self.assertEqual("incomplete_tail", report["status"])
        self.assertEqual(len(prefix), report["journal"]["valid_bytes"])
        self.assertEqual(len(prefix) + len(tail), report["journal"]["total_bytes"])
        self.assertEqual(len(tail), report["journal"]["incomplete_tail_bytes"])
        self.assertEqual(before, self.file_contents())
        with Database(self.path) as db:
            self.assertEqual(len(tail), db.recovered_tail_bytes)
            self.assertEqual(self.booked, db.execute("book", reserve()))
        self.assertEqual(prefix, self.journal.read_bytes())

    def test_readonly_handle_cannot_execute_even_a_cached_request(self):
        before = self.file_contents()
        with Database(self.path, read_only=True) as db:
            self.assertFalse(db.list_slots()[0]["available"])
            self.assertEqual(0, db.recovered_tail_bytes)
            with self.assertRaises(ReadOnlyError):
                db.execute("book", reserve())
            with self.assertRaises(ReadOnlyError):
                db.execute("new", reserve("bob"))
        self.assertEqual(before, self.file_contents())

    def test_check_does_not_create_missing_files_or_directories(self):
        missing = self.path / "missing"
        with self.assertRaises(FileNotFoundError):
            Database.check(missing)
        self.assertFalse(missing.exists())
        empty = self.path / "empty"
        empty.mkdir()
        with self.assertRaises(FileNotFoundError):
            Database.check(empty)
        self.assertEqual([], list(empty.iterdir()))

    def test_check_refuses_to_race_a_live_owner(self):
        with Database(self.path):
            with self.assertRaises(AlreadyOpen):
                Database.check(self.path)
        self.assertEqual("ok", Database.check(self.path)["status"])

    def test_complete_corruption_is_reported_without_changing_files_or_leaking_lock(self):
        original = self.journal.read_bytes()
        lines = original.splitlines(keepends=True)
        bad = json.loads(lines[-1])
        bad["transaction"]["request_id"] = "damaged"
        self.journal.write_bytes(b"".join(lines[:-1]) + canonical_json(bad) + b"\n")
        before = self.file_contents()
        with self.assertRaises(CorruptJournal):
            Database.check(self.path)
        self.assertEqual(before, self.file_contents())
        self.journal.write_bytes(original)
        self.assertEqual("ok", Database.check(self.path)["status"])

    def test_replay_checks_json_types_as_well_as_python_equality(self):
        lines = self.journal.read_bytes().splitlines(keepends=True)
        # The fourth transaction creates reservation 1. True == 1 in Python,
        # but a boolean is not a valid persisted reservation identifier.
        bad = json.loads(lines[3])
        bad["transaction"]["result"]["reservation"]["reservation_id"] = True
        bad["sha256"] = hashlib.sha256(canonical_json(bad["transaction"])).hexdigest()
        lines[3] = canonical_json(bad) + b"\n"
        self.journal.write_bytes(b"".join(lines))
        before = self.file_contents()
        with self.assertRaises(CorruptJournal):
            Database.check(self.path)
        with self.assertRaises(CorruptJournal):
            Database(self.path)
        self.assertEqual(before, self.file_contents())

    def test_cli_exit_codes_distinguish_valid_tail_and_corruption(self):
        valid = self.run_check()
        self.assertEqual(0, valid.returncode, valid.stderr)
        self.assertEqual("ok", json.loads(valid.stdout)["status"])
        original = self.journal.read_bytes()
        self.journal.write_bytes(original + b'{"partial":')
        incomplete = self.run_check()
        self.assertEqual(1, incomplete.returncode, incomplete.stderr)
        self.assertEqual("incomplete_tail", json.loads(incomplete.stdout)["status"])
        self.assertEqual(original + b'{"partial":', self.journal.read_bytes())
        self.journal.write_bytes(original + b'not-a-valid-record\n')
        corrupt = self.run_check()
        self.assertEqual(2, corrupt.returncode)
        self.assertIn("CorruptJournal", corrupt.stderr)
        missing = self.run_check(self.path / "missing")
        self.assertEqual(2, missing.returncode)
        self.assertFalse((self.path / "missing").exists())


if __name__ == "__main__":
    unittest.main()

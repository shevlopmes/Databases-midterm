import json
import subprocess
import sys
import unittest

from restaurant_db import Database
from tests.support import test_directory


class VideoDemoTests(unittest.TestCase):
    def test_real_demo_runs_end_to_end_and_keeps_results_for_inspection(self):
        root = test_directory(self) / "recording"
        result = subprocess.run([sys.executable, "scripts/video_demo.py", "--auto", "--output-root", str(root)],
                                capture_output=True, text=True, encoding="utf-8", timeout=45)
        self.assertEqual(0, result.returncode, result.stderr)
        directories = list(root.iterdir())
        self.assertEqual(1, len(directories))
        directory = directories[0]
        report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
        self.assertEqual({"requests": 20, "reserved": 1, "slot_taken": 19}, report["race"])
        self.assertEqual(403, report["foreign_cancel_status"])
        self.assertEqual(200, report["owner_cancel_status"])
        self.assertTrue(report["idempotent_retry"])
        self.assertEqual("ok", report["journal_check"])
        self.assertEqual(1, report["recovery"]["active_reservations"])
        # No server is left holding the files; there is a real, valid journal.
        self.assertEqual("ok", Database.check(directory / "data")["status"])
        with Database(directory / "data") as db:
            self.assertEqual(2, len(db.snapshot()["reservations"]))
        self.assertNotIn("Bearer ", result.stdout + result.stderr)
        self.assertNotIn('"token":', result.stdout + result.stderr)
        users = json.loads((directory / "auth" / "users.json").read_text(encoding="utf-8"))["users"]
        self.assertTrue(all(set(user) == {"user_id", "token_sha256"} for user in users))

if __name__ == "__main__":
    unittest.main()

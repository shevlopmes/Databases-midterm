import hashlib
import json
import secrets
import subprocess
import sys
import unittest
from unittest.mock import patch

from restaurant_db import Database
from restaurant_db.auth import AuthStore, Principal, issue_token, revoke_token
from restaurant_db.errors import (AlreadyOpen, AuthenticationError, AuthConfigurationError,
                                 AuthorizationError, CorruptJournal, IdempotencyConflict)
from restaurant_db.errors import ValidationError
from restaurant_db.journal import DirectoryLock, canonical_json
from restaurant_db.service import CommandService
from tests.support import test_directory
from tests.test_engine import prepare, publish, reserve


class TokenTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self) / "auth" / "users.json"

    def test_issue_authenticate_and_hash_only_storage(self):
        token = issue_token(self.path, "alice")
        self.assertEqual(43, len(token))
        self.assertNotIn(token.encode(), self.path.read_bytes())
        store = AuthStore.load(self.path)
        self.assertEqual(Principal("alice"), store.authenticate(token))
        self.assertEqual(Principal("alice"), store.authenticate_headers([f"bearer {token}"]))
        with self.assertRaises(AuthenticationError):
            store.authenticate(secrets.token_urlsafe(32))

    def test_rotation_and_revocation_require_a_reloaded_snapshot(self):
        old = issue_token(self.path, "alice")
        running = AuthStore.load(self.path)
        new = issue_token(self.path, "alice")
        self.assertNotEqual(old, new)
        self.assertEqual("alice", running.authenticate(old).user_id)
        reloaded = AuthStore.load(self.path)
        with self.assertRaises(AuthenticationError):
            reloaded.authenticate(old)
        self.assertEqual("alice", reloaded.authenticate(new).user_id)
        revoke_token(self.path, "alice")
        with self.assertRaises(AuthenticationError):
            AuthStore.load(self.path).authenticate(new)
        # An empty registry grants nobody access; it never disables checking.
        self.assertEqual([], json.loads(self.path.read_bytes())["users"])

    def test_missing_and_invalid_registry_never_get_overwritten(self):
        with self.assertRaises(FileNotFoundError):
            AuthStore.load(self.path)
        with self.assertRaises(FileNotFoundError):
            revoke_token(self.path, "alice")
        self.assertFalse(self.path.parent.exists())
        self.path.parent.mkdir()
        invalid = b'{"raw_secret":"must-not-appear-in-errors"}'
        self.path.write_bytes(invalid)
        with self.assertRaises(AuthConfigurationError) as error:
            issue_token(self.path, "alice")
        self.assertNotIn("must-not-appear", str(error.exception))
        self.assertEqual(invalid, self.path.read_bytes())

    def test_duplicate_users_verifiers_and_unsupported_formats_are_rejected(self):
        self.path.parent.mkdir()
        digest = hashlib.sha256(b"test-secret").hexdigest()
        entry = {"user_id": "alice", "token_sha256": digest}
        cases = [
            {"version": True, "users": []}, {"version": 2, "users": []},
            {"version": 1, "users": [entry, entry]},
            {"version": 1, "users": [entry, {**entry, "user_id": "bob"}]},
            {"version": 1, "users": [{**entry, "token_sha256": "invalid"}]},
            {"version": 1, "users": [{**entry, "token": "raw-secret"}]},
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                self.path.write_bytes(canonical_json(payload))
                with self.assertRaises(AuthConfigurationError):
                    AuthStore.load(self.path)

    def test_missing_duplicate_and_malformed_authorization_is_rejected(self):
        token = issue_token(self.path, "alice")
        store = AuthStore.load(self.path)
        for headers in ([], [f"Bearer {token}", f"Bearer {token}"], [token],
                        [f"Basic {token}"], [f"Bearer {token} extra"], ["Bearer x"],
                        [f"Bearer\t{token}"], [f"Bearer {token}\n"]):
            with self.subTest(header_count=len(headers)):
                with self.assertRaises(AuthenticationError):
                    store.authenticate_headers(headers)

    def test_failed_atomic_replace_keeps_previous_registry_and_cleans_temp(self):
        token = issue_token(self.path, "alice")
        original = self.path.read_bytes()
        with patch("restaurant_db.auth.os.replace", side_effect=OSError("replace failure")):
            with self.assertRaises(OSError):
                issue_token(self.path, "alice")
        self.assertEqual(original, self.path.read_bytes())
        self.assertEqual("alice", AuthStore.load(self.path).authenticate(token).user_id)
        self.assertEqual([], list(self.path.parent.glob("*.tmp")))

    def test_concurrent_registry_writer_is_rejected(self):
        token = issue_token(self.path, "alice")
        lock = DirectoryLock(self.path.parent, filename=f"{self.path.name}.lock")
        try:
            with self.assertRaises(AlreadyOpen):
                issue_token(self.path, "bob")
        finally:
            lock.close()
        self.assertEqual("alice", AuthStore.load(self.path).authenticate(token).user_id)

    def test_cli_issue_rotate_and_revoke(self):
        def run(action):
            return subprocess.run([sys.executable, "-m", "restaurant_db", action,
                                   "--auth-file", str(self.path), "--user-id", "alice"],
                                  capture_output=True, text=True, timeout=10)
        first = run("auth-issue")
        self.assertEqual(0, first.returncode, first.stderr)
        old = json.loads(first.stdout)["token"]
        self.assertEqual("alice", AuthStore.load(self.path).authenticate(old).user_id)
        second = run("auth-issue")
        self.assertEqual(0, second.returncode, second.stderr)
        new = json.loads(second.stdout)["token"]
        self.assertNotEqual(old, new)
        with self.assertRaises(AuthenticationError):
            AuthStore.load(self.path).authenticate(old)
        revoked = run("auth-revoke")
        self.assertEqual(0, revoked.returncode, revoked.stderr)
        self.assertTrue(json.loads(revoked.stdout)["restart_required"])
        with self.assertRaises(AuthenticationError):
            AuthStore.load(self.path).authenticate(new)
        self.assertNotIn(old, first.stderr + second.stderr + revoked.stdout + revoked.stderr)
        self.assertNotIn(new, second.stderr + revoked.stdout + revoked.stderr)


class AuthenticatedCommandTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self)
        with Database(self.path) as db:
            prepare(db)
            publish(db)

    def test_public_command_cannot_choose_identity_or_admin_operation(self):
        with Database(self.path) as db:
            service = CommandService(db)
            before = (self.path / "transactions.jsonl").read_bytes()
            with self.assertRaises(ValidationError):
                service.execute(Principal("alice"), "spoof", reserve("bob"))
            with self.assertRaises(AuthorizationError):
                service.execute(Principal("alice"), "admin", {"op": "publish_batch", "batch_id": "b1"})
            with self.assertRaises(AuthorizationError):
                db.execute_user("alice", "forged", reserve("bob"))
            self.assertEqual(before, (self.path / "transactions.jsonl").read_bytes())

    def test_user_scoped_retries_legacy_namespace_and_mixed_replay(self):
        with Database(self.path) as db:
            # The same request ID can exist in all three independent namespaces.
            legacy = db.execute("attempt", reserve("legacy"))
            alice = db.execute_user("alice", "attempt", reserve("alice"))
            bob = db.execute_user("bob", "attempt", reserve("bob"))
            self.assertEqual("reserved", legacy["code"])
            self.assertEqual("slot_taken", alice["code"])
            self.assertEqual("slot_taken", bob["code"])
            with self.assertRaises(IdempotencyConflict):
                db.execute_user("alice", "attempt", reserve("alice", "other-slot"))
        records = [json.loads(line)["transaction"] for line in (self.path / "transactions.jsonl").read_bytes().splitlines()]
        self.assertEqual([1, 1, 1, 1, 2, 2], [record["version"] for record in records])
        self.assertEqual(["alice", "bob"], [record["actor_id"] for record in records if record["version"] == 2])
        self.assertEqual("ok", Database.check(self.path)["status"])
        with Database(self.path) as db:
            self.assertEqual(legacy, db.execute("attempt", reserve("legacy")))
            self.assertEqual(alice, db.execute_user("alice", "attempt", reserve("alice")))
            self.assertEqual(bob, db.execute_user("bob", "attempt", reserve("bob")))

    def test_cancel_replay_does_not_depend_on_current_credentials(self):
        with Database(self.path) as db:
            db.execute_user("alice", "book", reserve())
            cancelled = db.execute_user("alice", "cancel", {"op": "cancel", "reservation_id": 1, "user_id": "alice"})
        # No credentials file is needed for check, CLI or journal recovery.
        self.assertEqual(1, Database.check(self.path)["state"]["cancelled_reservations"])
        with Database(self.path) as db:
            self.assertTrue(db.list_slots()[0]["available"])
            self.assertEqual(cancelled, db.execute_user("alice", "cancel", {
                "op": "cancel", "reservation_id": 1, "user_id": "alice"}))

    def test_authenticated_owner_can_cancel_a_legacy_booking_without_changing_its_cached_result(self):
        with Database(self.path) as db:
            legacy = db.execute("book", reserve())
        with Database(self.path) as db:
            # The new user-scoped key is independent even with the same string.
            cancelled = CommandService(db).execute(Principal("alice"), "book", {
                "op": "cancel", "reservation_id": 1})
            self.assertEqual("cancelled", cancelled["code"])
            self.assertEqual(legacy, db.execute("book", reserve()))
            self.assertTrue(db.list_slots()[0]["available"])
        with Database(self.path) as db:
            self.assertEqual(cancelled, CommandService(db).execute(Principal("alice"), "book", {
                "op": "cancel", "reservation_id": 1}))
            self.assertEqual(legacy, db.execute("book", reserve()))

    def test_forged_actor_in_checksummed_record_is_rejected(self):
        with Database(self.path) as db:
            db.execute_user("alice", "book", reserve())
        journal = self.path / "transactions.jsonl"
        lines = journal.read_bytes().splitlines(keepends=True)
        envelope = json.loads(lines[-1])
        envelope["transaction"]["actor_id"] = "bob"
        envelope["sha256"] = hashlib.sha256(canonical_json(envelope["transaction"])).hexdigest()
        journal.write_bytes(b"".join(lines[:-1]) + canonical_json(envelope) + b"\n")
        with self.assertRaises(CorruptJournal):
            Database.check(self.path)

    def test_torn_version_two_cancel_keeps_atomicity_and_user_scoped_retry(self):
        with Database(self.path) as db:
            db.execute_user("alice", "book", reserve())
            db.execute_user("alice", "cancel", {"op": "cancel", "reservation_id": 1, "user_id": "alice"})
        lines = (self.path / "transactions.jsonl").read_bytes().splitlines(keepends=True)
        prefix, last = b"".join(lines[:-1]), lines[-1]
        recovered = self.path / "recovered"
        recovered.mkdir()
        for cut in (0, 1, len(last) // 2, len(last) - 1, len(last)):
            with self.subTest(cut=cut):
                (recovered / "transactions.jsonl").write_bytes(prefix + last[:cut])
                with Database(recovered) as db:
                    self.assertEqual(cut == len(last), db.list_slots()[0]["available"])
                    result = db.execute_user("alice", "cancel", {
                        "op": "cancel", "reservation_id": 1, "user_id": "alice"})
                    self.assertEqual("cancelled", result["code"])
                    self.assertTrue(db.list_slots()[0]["available"])

    def test_process_death_after_authenticated_commit_recovers_without_credentials(self):
        code = """
import os, sys
from restaurant_db import Database
db = Database(sys.argv[1])
def stop_before_apply(command, result):
    os._exit(23)
db._apply = stop_before_apply
db.execute_user('alice', 'book', {'op': 'reserve', 'slot_id': 's1', 'user_id': 'alice'})
"""
        child = subprocess.run([sys.executable, "-c", code, str(self.path)],
                               capture_output=True, text=True, timeout=10)
        self.assertEqual(23, child.returncode, child.stderr)
        with Database(self.path) as db:
            result = db.execute_user("alice", "book", reserve())
            self.assertEqual("reserved", result["code"])
            self.assertEqual(1, len(db.snapshot()["reservations"]))


if __name__ == "__main__":
    unittest.main()

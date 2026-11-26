from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import socket
import subprocess
import sys
import threading
import unittest

from restaurant_db import Database
from restaurant_db.auth import AuthStore
from restaurant_db.auth import issue_token, revoke_token
from restaurant_db.server import ReservationServer
from tests.test_cancel import cancel
from tests.test_engine import prepare, publish
from tests.support import test_directory, write_credentials


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self)
        with Database(self.path) as db:
            prepare(db)
            publish(db)
        self.auth_file = self.path / "users.json"
        users = ["alice", "bob"] + [f"user-{i}" for i in range(24)] + [f"new-user-{i}" for i in range(16)]
        self.tokens = write_credentials(self.auth_file, users)
        self.start_server()
        self.addCleanup(self.stop_server)

    def start_server(self):
        self.process = subprocess.Popen(
            [sys.executable, "-m", "restaurant_db", "serve", "--data", str(self.path), "--port", "0",
             "--auth-file", str(self.auth_file)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        ready = self.process.stdout.readline()
        self.assertTrue(ready, "Server failed before startup")
        self.port = int(json.loads(ready)["listening"].rsplit(":", 1)[1])

    def stop_server(self):
        if self.process.poll() is None:
            self.process.kill()
        return self.process.communicate(timeout=10)

    def post(self, payload):
        # Translate the old internal-command test fixtures into the public API:
        # pick that user's real credential and REMOVE identity from the body.
        command = dict(payload["command"])
        user_id = command.pop("user_id", "alice")
        return self.raw_post({**payload, "command": command}, self.tokens[user_id])

    def raw_post(self, payload, token=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
        try:
            headers = {"Content-Type": "application/json"}
            if token is not None:
                headers["Authorization"] = f"Bearer {token}"
            connection.request("POST", "/commands", json.dumps(payload).encode(),
                               headers)
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def slots(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
        try:
            connection.request("GET", "/slots")
            response = connection.getresponse()
            self.assertEqual(200, response.status)
            return json.loads(response.read())["slots"]
        finally:
            connection.close()

    def book_alice(self):
        status, result = self.post({"request_id": "book", "command": {
            "op": "reserve", "slot_id": "s1", "user_id": "alice"}})
        self.assertEqual(200, status)
        return result

    def test_missing_or_invalid_token_cannot_execute_or_read_cached_results(self):
        payload = {"request_id": "book", "command": {"op": "reserve", "slot_id": "s1"}}
        self.book_alice()
        for token in (None, "x" * 43, self.tokens["alice"] + "extra"):
            with self.subTest(has_token=token is not None):
                status, result = self.raw_post(payload, token)
                self.assertEqual(401, status)
                self.assertEqual("unauthenticated", result["code"])
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(4, db.snapshot()["request_count"])

    def test_identity_in_json_is_rejected_instead_of_trusted(self):
        payload = {"request_id": "spoof", "command": {
            "op": "reserve", "slot_id": "s1", "user_id": "bob"}}
        status, result = self.raw_post(payload, self.tokens["alice"])
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", result["code"])
        self.assertTrue(self.slots()[0]["available"])
        result = self.book_alice()
        self.assertEqual("alice", result["reservation"]["user_id"])
        status, result = self.raw_post({"request_id": "cancel", "command": {
            "op": "cancel", "reservation_id": 1}}, self.tokens["bob"])
        self.assertEqual(403, status)
        self.assertEqual("reservation_not_owner", result["code"])
        self.assertFalse(self.slots()[0]["available"])

    def test_admin_commands_are_forbidden_even_with_a_valid_token_or_legacy_cache(self):
        for command in ({"op": "add_table", "table_id": "bad", "name": "Bad"},
                        {"op": "prepare_batch", "batch_id": "bad", "slots": []},
                        {"op": "publish_batch", "batch_id": "b1"}):
            with self.subTest(op=command["op"]):
                status, result = self.raw_post({"request_id": "publish", "command": command}, self.tokens["alice"])
                self.assertEqual(403, status)
                self.assertEqual("forbidden", result["code"])
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(3, db.snapshot()["request_count"])
            self.assertEqual(1, len(db.snapshot()["tables"]))

    def test_duplicate_authorization_headers_are_rejected(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
        try:
            connection.putrequest("POST", "/commands")
            connection.putheader("Authorization", f"Bearer {self.tokens['alice']}")
            connection.putheader("Authorization", f"Bearer {self.tokens['bob']}")
            connection.putheader("Content-Length", "2")
            connection.endheaders(b"{}")
            response = connection.getresponse()
            self.assertEqual(401, response.status)
            self.assertEqual("Bearer", response.getheader("WWW-Authenticate"))
            self.assertEqual("unauthenticated", json.loads(response.read())["code"])
        finally:
            connection.close()

    def test_rotated_then_revoked_token_cannot_bypass_authentication_using_cache(self):
        original = self.book_alice()
        payload = {"request_id": "book", "command": {"op": "reserve", "slot_id": "s1"}}
        old = self.tokens["alice"]
        new = issue_token(self.auth_file, "alice")
        # A running server holds a snapshot: rotation applies after restart.
        self.assertEqual(200, self.raw_post(payload, old)[0])
        first_logs = self.stop_server()
        self.start_server()
        self.assertEqual(401, self.raw_post(payload, old)[0])
        self.assertEqual((200, original), self.raw_post(payload, new))
        revoke_token(self.auth_file, "alice")
        second_logs = self.stop_server()
        self.start_server()
        self.assertEqual(401, self.raw_post(payload, new)[0])
        third_logs = self.stop_server()
        journal = (self.path / "transactions.jsonl").read_bytes()
        for token in (old, new):
            self.assertNotIn(token.encode(), journal)
            self.assertNotIn(token, "".join(first_logs + second_logs + third_logs))
        with Database(self.path) as db:
            self.assertEqual(4, db.snapshot()["request_count"])
            self.assertFalse(db.list_slots()[0]["available"])

    def test_changed_command_with_same_user_and_key_is_a_conflict(self):
        self.book_alice()
        status, result = self.raw_post({"request_id": "book", "command": {
            "op": "reserve", "slot_id": "another-slot"}}, self.tokens["alice"])
        self.assertEqual(409, status)
        self.assertEqual("idempotency_conflict", result["code"])

    def test_race_script_uses_issued_credentials_without_leaking_them(self):
        tokens_file = self.path / "client.tokens.json"
        tokens_file.write_text(json.dumps({"alice": self.tokens["alice"], "bob": self.tokens["bob"]}), encoding="utf-8")
        result = subprocess.run([sys.executable, "scripts/race.py", "--url", f"http://127.0.0.1:{self.port}",
                                 "--tokens-file", str(tokens_file), "--clients", "20"],
                                capture_output=True, text=True, timeout=40)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual({"reserved": 1, "slot_taken": 19}, json.loads(result.stdout)["results"])
        self.assertNotIn(self.tokens["alice"], result.stdout + result.stderr)
        self.assertNotIn(self.tokens["bob"], result.stdout + result.stderr)
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(1, len(db.snapshot()["reservations"]))
            self.assertIn(db.snapshot()["reservations"][0]["user_id"], ("alice", "bob"))

    def test_cancel_http_checks_owner_and_allows_rebooking(self):
        self.book_alice()
        status, result = self.post({"request_id": "wrong-owner", "command": cancel(user_id="bob")})
        self.assertEqual(403, status)
        self.assertEqual("reservation_not_owner", result["code"])
        status, result = self.post({"request_id": "invalid", "command": cancel(True)})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", result["code"])
        self.assertFalse(self.slots()[0]["available"])
        status, result = self.post({"request_id": "cancel", "command": cancel()})
        self.assertEqual(200, status)
        self.assertEqual("cancelled", result["code"])
        self.assertTrue(self.slots()[0]["available"])
        status, result = self.post({"request_id": "next", "command": {
            "op": "reserve", "slot_id": "s1", "user_id": "bob"}})
        self.assertEqual(200, status)
        self.assertEqual(2, result["reservation"]["reservation_id"])
        self.assertFalse(self.slots()[0]["available"])

    def test_simultaneous_cancellation_retries_return_one_saved_result(self):
        self.book_alice()
        clients = 12
        barrier = threading.Barrier(clients)
        payload = {"request_id": "cancel", "command": cancel()}

        def retry(_):
            barrier.wait()
            return self.post(payload)

        with ThreadPoolExecutor(max_workers=clients) as pool:
            results = list(pool.map(retry, range(clients)))
        self.assertTrue(all(status == 200 for status, _ in results))
        self.assertTrue(all(result == results[0][1] for _, result in results))
        self.assertTrue(self.slots()[0]["available"])
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(5, db.snapshot()["request_count"])
            self.assertEqual({}, db.snapshot()["active_reservations"])
            self.assertIsNotNone(db.snapshot()["reservations"][0]["cancelled_at"])

    def test_stale_cancels_competing_with_new_bookings_never_free_the_winners_slot(self):
        self.book_alice()
        self.assertEqual(200, self.post({"request_id": "cancel", "command": cancel()})[0])
        competitors = 8  # Half stale cancellations, half new users: 16 clients total.
        barrier = threading.Barrier(competitors * 2)

        def compete(index):
            command = cancel() if index < competitors else {
                "op": "reserve", "slot_id": "s1", "user_id": f"new-user-{index}"}
            barrier.wait()
            return self.post({"request_id": f"mixed-{index}", "command": command})

        with ThreadPoolExecutor(max_workers=competitors * 2) as pool:
            results = list(pool.map(compete, range(competitors * 2)))
        self.assertEqual(Counter({"reservation_already_cancelled": competitors,
                                  "reserved": 1, "slot_taken": competitors - 1}),
                         Counter(result["code"] for _, result in results))
        self.assertFalse(self.slots()[0]["available"])
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual({"s1": 2}, db.snapshot()["active_reservations"])

    def test_lost_cancellation_response_can_be_retried_after_disconnect(self):
        self.book_alice()
        payload = {"request_id": "lost-cancel", "command": {"op": "cancel", "reservation_id": 1}}
        body = json.dumps(payload).encode()
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as connection:
            header = (f"POST /commands HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\n"
                      f"Authorization: Bearer {self.tokens['alice']}\r\n"
                      f"Content-Length: {len(body)}\r\n\r\n").encode()
            connection.sendall(header + body)
            connection.shutdown(socket.SHUT_WR)
        first = self.post(payload)
        self.assertEqual(200, first[0])
        self.assertEqual("cancelled", first[1]["code"])
        self.assertEqual(first, self.post(payload))
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(first[1], db.execute_user("alice", "lost-cancel", cancel()))
            self.assertTrue(db.list_slots()[0]["available"])

    def test_simultaneous_clients_have_exactly_one_winner(self):
        clients = 24  # A repeatable local experiment, not a production capacity claim.
        barrier = threading.Barrier(clients)

        def compete(index):
            barrier.wait()
            return self.post({"request_id": f"race-{index}", "command": {
                "op": "reserve", "slot_id": "s1", "user_id": f"user-{index}"}})

        with ThreadPoolExecutor(max_workers=clients) as pool:
            results = list(pool.map(compete, range(clients)))
        self.assertEqual(Counter({200: 1, 409: clients - 1}), Counter(status for status, _ in results))
        self.assertEqual(Counter({"reserved": 1, "slot_taken": clients - 1}),
                         Counter(result["code"] for _, result in results))
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(1, len(db.snapshot()["reservations"]))

    def test_simultaneous_retries_get_the_same_reservation(self):
        clients = 12
        barrier = threading.Barrier(clients)
        payload = {"request_id": "same-attempt", "command": {
            "op": "reserve", "slot_id": "s1", "user_id": "alice"}}

        def retry(_):
            barrier.wait()
            return self.post(payload)

        with ThreadPoolExecutor(max_workers=clients) as pool:
            results = list(pool.map(retry, range(clients)))
        self.assertTrue(all(status == 200 for status, _ in results))
        self.assertTrue(all(result == results[0][1] for _, result in results))
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(1, len(db.snapshot()["reservations"]))
            self.assertEqual(4, db.snapshot()["request_count"])

    def test_disconnecting_before_response_can_be_retried(self):
        payload = {"request_id": "lost-response", "command": {
            "op": "reserve", "slot_id": "s1"}}
        body = json.dumps(payload).encode()
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as connection:
            header = (f"POST /commands HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\n"
                      f"Authorization: Bearer {self.tokens['alice']}\r\n"
                      f"Content-Length: {len(body)}\r\n\r\n").encode()
            connection.sendall(header + body)
            connection.shutdown(socket.SHUT_WR)
            # Deliberately never read the reply.
        first = self.post(payload)
        second = self.post(payload)
        self.assertEqual(200, first[0])
        self.assertEqual(first, second)
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(1, len(db.snapshot()["reservations"]))

    def test_same_request_id_for_another_user_has_an_independent_result(self):
        payload = {"request_id": "attempt", "command": {
            "op": "reserve", "slot_id": "s1", "user_id": "alice"}}
        self.assertEqual(200, self.post(payload)[0])
        payload["command"]["user_id"] = "bob"
        status, result = self.post(payload)
        self.assertEqual(409, status)
        self.assertEqual("slot_taken", result["code"])

    def test_invalid_json_does_not_poison_the_engine(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            connection.request("POST", "/commands", b'{"request_id":"a","request_id":"b"}',
                               {"Content-Type": "application/json", "Authorization": f"Bearer {self.tokens['alice']}"})
            response = connection.getresponse()
            self.assertEqual(400, response.status)
            response.read()
        finally:
            connection.close()
        self.assertEqual(200, self.post({"request_id": "valid", "command": {
            "op": "reserve", "slot_id": "s1", "user_id": "alice"}})[0])

    def test_malformed_request_target_gets_json_error_and_server_keeps_working(self):
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as connection:
            # Use a raw request: high-level clients may reject the bad URL first.
            connection.sendall(b"POST http://[ HTTP/1.0\r\nHost: localhost\r\n"
                               b"Content-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
            response = http.client.HTTPResponse(connection)
            response.begin()
            self.assertEqual(400, response.status)
            self.assertEqual("invalid_request", json.loads(response.read())["code"])
            response.close()
        self.assertTrue(self.slots()[0]["available"])


class ServerConfigurationTests(unittest.TestCase):
    def test_missing_or_invalid_auth_file_stops_startup_before_data_creation(self):
        root = test_directory(self)
        auth_file = root / "users.json"
        for invalid in (None, b'{"token":"secret-that-must-not-leak"}'):
            if invalid is not None:
                auth_file.write_bytes(invalid)
            result = subprocess.run(
                [sys.executable, "-m", "restaurant_db", "serve", "--data", str(root / "data"),
                 "--port", "0", "--auth-file", str(auth_file)], capture_output=True, text=True, timeout=10)
            self.assertEqual(2, result.returncode)
            self.assertEqual("", result.stdout)
            self.assertFalse((root / "data").exists())
            self.assertNotIn("secret-that-must-not-leak", result.stderr)

    def test_nonfinite_request_timeouts_are_rejected_before_startup(self):
        path = test_directory(self)
        auth_file = path / "users.json"
        write_credentials(auth_file, ["alice"])
        with Database(path) as db:
            for timeout in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(timeout=repr(timeout)):
                    with self.assertRaises(ValueError):
                        with ReservationServer(("127.0.0.1", 0), db, AuthStore.load(auth_file), request_timeout=timeout):
                            pass

    def test_nonfinite_cli_timeout_exits_with_configuration_error(self):
        root = test_directory(self)
        auth_file = root / "users.json"
        write_credentials(auth_file, ["alice"])
        for timeout in ("nan", "inf"):
            with self.subTest(timeout=timeout):
                result = subprocess.run(
                    [sys.executable, "-m", "restaurant_db", "serve", "--data", str(root / timeout),
                     "--port", "0", "--request-timeout", timeout, "--auth-file", str(auth_file)],
                    capture_output=True, text=True, timeout=10)
                self.assertEqual(2, result.returncode)
                self.assertEqual("", result.stdout)
                self.assertIn("request_timeout must be finite", result.stderr)
                self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()

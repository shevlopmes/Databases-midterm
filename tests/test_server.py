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
from tests.test_cancel import cancel
from tests.test_engine import prepare, publish
from tests.support import test_directory


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.path = test_directory(self)
        with Database(self.path) as db:
            prepare(db)
            publish(db)
        self.process = subprocess.Popen(
            [sys.executable, "-m", "restaurant_db", "serve", "--data", str(self.path), "--port", "0"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self.stop_server)
        ready = self.process.stdout.readline()
        self.assertTrue(ready, "Server failed before startup")
        self.port = int(json.loads(ready)["listening"].rsplit(":", 1)[1])

    def stop_server(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate(timeout=10)

    def post(self, payload):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
        try:
            connection.request("POST", "/commands", json.dumps(payload).encode(),
                               {"Content-Type": "application/json"})
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

    def test_cancel_http_checks_owner_and_allows_rebooking(self):
        self.book_alice()
        status, result = self.post({"request_id": "wrong-owner", "command": cancel(user_id="bob")})
        self.assertEqual(409, status)
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
        payload = {"request_id": "lost-cancel", "command": cancel()}
        body = json.dumps(payload).encode()
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as connection:
            header = (f"POST /commands HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\n"
                      f"Content-Length: {len(body)}\r\n\r\n").encode()
            connection.sendall(header + body)
            connection.shutdown(socket.SHUT_WR)
        first = self.post(payload)
        self.assertEqual(200, first[0])
        self.assertEqual("cancelled", first[1]["code"])
        self.assertEqual(first, self.post(payload))
        self.stop_server()
        with Database(self.path) as db:
            self.assertEqual(first[1], db.execute("lost-cancel", cancel()))
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
            "op": "reserve", "slot_id": "s1", "user_id": "alice"}}
        body = json.dumps(payload).encode()
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as connection:
            header = (f"POST /commands HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\n"
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

    def test_reusing_request_id_for_another_user_is_a_conflict(self):
        payload = {"request_id": "attempt", "command": {
            "op": "reserve", "slot_id": "s1", "user_id": "alice"}}
        self.assertEqual(200, self.post(payload)[0])
        payload["command"]["user_id"] = "bob"
        status, result = self.post(payload)
        self.assertEqual(409, status)
        self.assertEqual("idempotency_conflict", result["code"])

    def test_invalid_json_does_not_poison_the_engine(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            connection.request("POST", "/commands", b'{"request_id":"a","request_id":"b"}',
                               {"Content-Type": "application/json"})
            response = connection.getresponse()
            self.assertEqual(400, response.status)
            response.read()
        finally:
            connection.close()
        self.assertEqual(200, self.post({"request_id": "valid", "command": {
            "op": "reserve", "slot_id": "s1", "user_id": "alice"}})[0])


if __name__ == "__main__":
    unittest.main()

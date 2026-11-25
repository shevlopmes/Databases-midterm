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

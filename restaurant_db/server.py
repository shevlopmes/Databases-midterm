"""A local HTTP adapter. HTTPServer intentionally handles one request at a time."""

from http.server import BaseHTTPRequestHandler, HTTPServer
import socket
from typing import Any
from urllib.parse import urlsplit

from .engine import Database
from .errors import IdempotencyConflict, StorageUnavailable, ValidationError
from .journal import canonical_json, decode_json
from .model import fields


class ReservationServer(HTTPServer):
    # Use the operating system's listen-backlog recommendation, not TCPServer's
    # small default. This backlog is not a durable or fair business queue.
    request_queue_size = socket.SOMAXCONN

    def __init__(self, address: tuple[str, int], database: Database,
                 request_timeout: float = 5.0, max_body_bytes: int = 1024 * 1024):
        if request_timeout <= 0 or max_body_bytes <= 0:
            raise ValueError("request_timeout and max_body_bytes must be positive")
        self.database = database
        self.request_timeout = request_timeout
        self.max_body_bytes = max_body_bytes
        super().__init__(address, Handler)

    def get_request(self) -> tuple[socket.socket, Any]:
        connection, address = super().get_request()
        # An idle connection otherwise blocks the only worker indefinitely.
        connection.settimeout(self.request_timeout)
        return connection, address


class Handler(BaseHTTPRequestHandler):
    server: ReservationServer
    protocol_version = "HTTP/1.0"  # Close connections after a response; no keep-alive.
    server_version = "RestaurantDB/0.1"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Keep the educational demo quiet; results are returned to clients.

    def _send(self, status: int, value: Any) -> None:
        body = canonical_json(value)
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            # A disconnected client does not undo an already committed command.
            pass
        self.close_connection = True

    def do_GET(self) -> None:
        if self.path != "/slots":
            self._send(404, {"ok": False, "code": "endpoint_not_found"})
            return
        try:
            self._send(200, {"slots": self.server.database.list_slots()})
        except StorageUnavailable as exc:
            self._send(503, {"ok": False, "code": "recovery_required", "message": str(exc)})

    def do_POST(self) -> None:
        if urlsplit(self.path).path != "/commands" or "?" in self.path:
            self._send(404, {"ok": False, "code": "endpoint_not_found"})
            return
        if self.headers.get("Transfer-Encoding") is not None:
            self._send(400, {"ok": False, "code": "transfer_encoding_not_supported"})
            return
        lengths = self.headers.get_all("Content-Length", [])
        if not lengths:
            self._send(411, {"ok": False, "code": "content_length_required"})
            return
        try:
            if len(lengths) != 1:
                raise ValidationError("exactly one Content-Length is required")
            length = int(lengths[0])
            if length <= 0:
                raise ValidationError("Content-Length must be positive")
            if length > self.server.max_body_bytes:
                self._send(413, {"ok": False, "code": "body_too_large"})
                return
            if self.headers.get_content_type() != "application/json":
                self._send(415, {"ok": False, "code": "json_required"})
                return
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValidationError("incomplete request body")
            payload = decode_json(raw)
            fields(payload, {"request_id", "command"}, "request")
            result = self.server.database.execute(payload["request_id"], payload["command"])
            self._send(200 if result["ok"] else 409, result)
        except IdempotencyConflict as exc:
            self._send(409, {"ok": False, "code": "idempotency_conflict", "message": str(exc)})
        except StorageUnavailable as exc:
            self._send(503, {"ok": False, "code": "recovery_required",
                             "message": str(exc), "outcome_may_be_unknown": True})
        except (ValidationError, ValueError, UnicodeError, RecursionError) as exc:
            self._send(400, {"ok": False, "code": "invalid_request", "message": str(exc)})
        except TimeoutError:
            self._send(408, {"ok": False, "code": "request_timeout"})

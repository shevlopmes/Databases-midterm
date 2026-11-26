"""Append-only transactions. A newline terminates one complete record.

Scanning never changes bytes. After validation, writable recovery can discard
an incomplete final line and must sync the validated prefix before serving.
A complete record with a bad checksum is corruption: opening must fail.
"""

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterator

from .errors import AlreadyOpen, CorruptJournal, ReadOnlyError, StorageUnavailable


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False,
                      sort_keys=True, separators=(",", ":")).encode("utf-8")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def decode_json(data: bytes) -> Any:
    return json.loads(data, object_pairs_hook=_unique_object,
                      parse_constant=_reject_constant)


class DirectoryLock:
    """An OS lock, released even when the process dies. Never unlink its file."""

    def __init__(self, directory: Path, *, must_exist: bool = False, filename: str = "owner.lock"):
        # Diagnostic mode locks an existing sidecar without creating or writing
        # it. Windows still needs a writable descriptor for the exclusive lock.
        self._file = open(directory / filename, "r+b" if must_exist else "a+b")
        try:
            if not must_exist:
                self._file.seek(0, os.SEEK_END)
                if self._file.tell() == 0:
                    self._file.write(b"\0")
                    self._file.flush()
            self._file.seek(0)
            if os.name == "nt":
                import msvcrt
                # A single byte is sufficient: every owner locks byte zero.
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._file.close()
            raise AlreadyOpen(f"Cannot acquire database ownership: {directory}") from exc

    def close(self) -> None:
        if not self._file.closed:
            # Closing the descriptor releases the OS lock.
            self._file.close()


class Journal:
    def __init__(self, directory: Path, *, read_only: bool = False):
        if not read_only:
            directory.mkdir(parents=True, exist_ok=True)
        self._read_only = read_only
        self._lock = DirectoryLock(directory, must_exist=read_only)
        self.total_bytes = 0
        self.valid_bytes = 0
        self.valid_records = 0
        self.incomplete_tail_bytes = 0
        self.recovered_tail_bytes = 0
        self._failed = False
        self._scanned = False
        self._ready = False
        try:
            self._file = open(directory / "transactions.jsonl", "rb" if read_only else "a+b")
            self.total_bytes = os.fstat(self._file.fileno()).st_size
        except BaseException:
            self._lock.close()
            raise

    def transactions(self) -> Iterator[dict[str, Any]]:
        if self._scanned or self._ready:
            raise RuntimeError("A journal can only be scanned once per open")
        self._file.seek(0)
        line_number = 0
        while True:
            offset = self._file.tell()
            line = self._file.readline()
            if not line:
                self._scanned = True
                return
            line_number += 1
            if not line.endswith(b"\n"):
                self.incomplete_tail_bytes = len(line)
                self._scanned = True
                return
            try:
                envelope = decode_json(line)
                if not isinstance(envelope, dict) or set(envelope) != {"sha256", "transaction"}:
                    raise ValueError("invalid envelope")
                transaction = envelope["transaction"]
                if not isinstance(transaction, dict):
                    raise ValueError("transaction is not an object")
                digest = hashlib.sha256(canonical_json(transaction)).hexdigest()
                if envelope["sha256"] != digest:
                    raise ValueError("checksum mismatch")
            except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
                raise CorruptJournal(f"Invalid journal record at line {line_number}: {exc}") from exc
            self.valid_records += 1
            self.valid_bytes = offset + len(line)
            yield transaction

    def finish_recovery(self) -> None:
        """Run only after ALL records pass both storage and domain validation."""
        if self._read_only:
            raise ReadOnlyError("Diagnostic mode cannot recover a journal")
        if not self._scanned:
            raise RuntimeError("Validate the whole journal before recovery")
        try:
            if self.incomplete_tail_bytes:
                self._file.seek(self.valid_bytes)
                self._file.truncate()
            # A complete record may have survived a crash before its original
            # fsync. Cached replies must not bypass this new durability barrier.
            self._file.flush()
            os.fsync(self._file.fileno())
        except OSError as exc:
            self._failed = True
            raise StorageUnavailable("Recovery sync failed; database cannot serve requests") from exc
        self.recovered_tail_bytes = self.incomplete_tail_bytes
        self._ready = True

    def append(self, transaction: dict[str, Any]) -> None:
        if self._read_only:
            raise ReadOnlyError("Diagnostic mode cannot append transactions")
        if self._failed:
            raise StorageUnavailable("Journal failed; close and reopen the database")
        if not self._ready:
            raise StorageUnavailable("Journal recovery has not completed")
        payload = canonical_json(transaction)
        line = canonical_json({"sha256": hashlib.sha256(payload).hexdigest(),
                               "transaction": transaction}) + b"\n"
        try:
            self._file.seek(0, os.SEEK_END)
            remaining = memoryview(line)
            while remaining:
                count = self._file.write(remaining)
                if count is None or count <= 0:
                    raise OSError("short journal write")
                remaining = remaining[count:]
            self._file.flush()
            os.fsync(self._file.fileno())
        except OSError as exc:
            # The bytes may already exist: never report a definite rejection.
            self._failed = True
            raise StorageUnavailable("Commit outcome unknown; reopen and retry the same request_id") from exc

    def close(self) -> None:
        try:
            self._file.close()
        finally:
            self._lock.close()

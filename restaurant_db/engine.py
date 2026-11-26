"""The only place that decides whether a reservation can be committed."""

from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timezone
import os
from pathlib import Path
import threading
from typing import Any

from .errors import (AuthorizationError, CorruptJournal, IdempotencyConflict, OwnershipError,
                     ReadOnlyError, StorageUnavailable, ValidationError)
from .journal import Journal, canonical_json
from .model import Batch, Reservation, Slot, Table, normalize_command, text, timestamp


def success(code: str, **data: Any) -> dict[str, Any]:
    return {"ok": True, "code": code, **data}


def rejection(code: str, **data: Any) -> dict[str, Any]:
    return {"ok": False, "code": code, **data}


class Database:
    """Single process, single owner thread, sequential reads and writes.

    There is deliberately no mutex around execute(): callers must not use the
    engine from concurrent threads. The HTTP adapter uses a synchronous server.
    """

    def __init__(self, directory: str | Path, *, read_only: bool = False):
        self._owner = (os.getpid(), threading.get_ident())
        self._read_only = read_only
        self._closed = False
        self._failed = False
        self._tables: dict[str, Table] = {}
        self._slots: dict[str, Slot] = {}
        self._batches: dict[str, Batch] = {}
        self._reservations: dict[int, Reservation] = {}
        # Creation history stays immutable; cancellation records its own time.
        self._cancelled: dict[int, str] = {}
        self._active: dict[str, int] = {}
        # None is the legacy/local namespace; authenticated actors get their own.
        self._requests: dict[tuple[str | None, str], dict[str, Any]] = {}
        self._next_reservation_id = 1
        self._journal = Journal(Path(directory), read_only=read_only)
        try:
            for record in self._journal.transactions():
                self._replay(record)
            if not read_only:
                self._journal.finish_recovery()
        except BaseException:
            self._journal.close()
            raise

    @classmethod
    def check(cls, directory: str | Path) -> dict[str, Any]:
        """Validate a stable journal without creating, truncating or syncing it.

        This acquires the ordinary exclusive ownership lock, so the live server
        must be stopped. A full replay also checks business-state consistency.
        """
        with cls(directory, read_only=True) as db:
            journal = db._journal
            return {
                "status": "incomplete_tail" if journal.incomplete_tail_bytes else "ok",
                "read_only": True,
                "journal": {"total_bytes": journal.total_bytes,
                            "valid_bytes": journal.valid_bytes,
                            "complete_records": journal.valid_records,
                            "incomplete_tail_bytes": journal.incomplete_tail_bytes},
                "state": {"tables": len(db._tables), "batches": len(db._batches),
                          "published_batches": sum(batch.published for batch in db._batches.values()),
                          "slots": len(db._slots), "reservations": len(db._reservations),
                          "cancelled_reservations": len(db._cancelled),
                          "active_reservations": len(db._active), "requests": len(db._requests)},
            }

    @property
    def recovered_tail_bytes(self) -> int:
        return self._journal.recovered_tail_bytes

    def _check_owner(self) -> None:
        if (os.getpid(), threading.get_ident()) != self._owner:
            raise OwnershipError("Use the database only from its owner process and thread")
        if self._closed:
            raise StorageUnavailable("Database is closed")
        if self._failed:
            raise StorageUnavailable("Database needs recovery; close and reopen it")

    def execute(self, request_id: str, command: dict[str, Any]) -> dict[str, Any]:
        """Trusted local entry point; preserves version-1 request semantics."""
        return self._execute(request_id, command, actor_id=None)

    def execute_user(self, actor_id: str, request_id: str, command: dict[str, Any]) -> dict[str, Any]:
        """Called by the trusted service AFTER authentication, not a token check."""
        self._check_owner()
        actor_id = text(actor_id, "actor_id")
        command = normalize_command(command)
        if command["op"] not in ("reserve", "cancel") or command["user_id"] != actor_id:
            raise AuthorizationError("Command does not belong to the authenticated user")
        return self._execute(request_id, command, actor_id=actor_id)

    def _execute(self, request_id: str, command: dict[str, Any], *, actor_id: str | None) -> dict[str, Any]:
        self._check_owner()
        if self._read_only:
            raise ReadOnlyError("Diagnostic mode cannot execute commands, including retries")
        request_id = text(request_id, "request_id")
        command = normalize_command(command)
        key = (actor_id, request_id)
        previous = self._requests.get(key)
        if previous is not None:
            if previous["command"] != command:
                raise IdempotencyConflict("request_id was already used for a different command")
            return deepcopy(previous["result"])

        at = datetime.now(timezone.utc).isoformat()
        result = self._plan(command, at)
        record = {"version": 1, "request_id": request_id, "at": at,
                  "command": command, "result": result}
        if actor_id is not None:
            record.update(version=2, actor_id=actor_id)
        try:
            # No reader can run between planning, durability and applying state.
            self._journal.append(record)
            self._apply(command, result)
            self._requests[key] = record
        except BaseException:
            self._failed = True
            raise
        return deepcopy(result)

    def _plan(self, command: dict[str, Any], at: str) -> dict[str, Any]:
        op = command["op"]
        if op == "add_table":
            if command["table_id"] in self._tables:
                return rejection("table_exists")
            return success("table_added", table={"table_id": command["table_id"],
                                                 "name": command["name"]})
        if op == "prepare_batch":
            return self._plan_batch(command)
        if op == "publish_batch":
            batch = self._batches.get(command["batch_id"])
            if batch is None:
                return rejection("batch_not_found")
            return success("batch_published", batch_id=batch.batch_id,
                           slot_count=len(batch.slot_ids))
        if op == "cancel":
            return self._plan_cancel(command, at)

        slot = self._slots.get(command["slot_id"])
        if slot is None:
            return rejection("slot_not_found")
        if not self._batches[slot.batch_id].published:
            return rejection("batch_not_published")
        if slot.slot_id in self._active:
            return rejection("slot_taken")
        reservation = Reservation(self._next_reservation_id, slot.slot_id,
                                  command["user_id"], at)
        return success("reserved", reservation=asdict(reservation))

    def _plan_cancel(self, command: dict[str, Any], at: str) -> dict[str, Any]:
        reservation = self._reservations.get(command["reservation_id"])
        if reservation is None:
            return rejection("reservation_not_found")
        if reservation.user_id != command["user_id"]:
            return rejection("reservation_not_owner")
        if reservation.reservation_id in self._cancelled:
            return rejection("reservation_already_cancelled")
        return success("cancelled", reservation_id=reservation.reservation_id,
                       slot_id=reservation.slot_id, cancelled_at=at)

    def _plan_batch(self, command: dict[str, Any]) -> dict[str, Any]:
        if command["batch_id"] in self._batches:
            return rejection("batch_exists")
        new_ids: set[str] = set()
        intervals: dict[str, list[tuple[datetime, datetime, str]]] = {}
        for slot in self._slots.values():
            intervals.setdefault(slot.table_id, []).append(
                (datetime.fromisoformat(slot.starts_at), datetime.fromisoformat(slot.ends_at), slot.slot_id))
        for raw in command["slots"]:
            if raw["slot_id"] in self._slots or raw["slot_id"] in new_ids:
                return rejection("slot_exists", slot_id=raw["slot_id"])
            if raw["table_id"] not in self._tables:
                return rejection("table_not_found", table_id=raw["table_id"])
            new_ids.add(raw["slot_id"])
            intervals.setdefault(raw["table_id"], []).append(
                (datetime.fromisoformat(raw["starts_at"]),
                 datetime.fromisoformat(raw["ends_at"]), raw["slot_id"]))
        for items in intervals.values():
            items.sort()
            for left, right in zip(items, items[1:]):
                # Half-open intervals: ending at 20:00 and starting at 20:00 is OK.
                if right[0] < left[1]:
                    return rejection("slots_overlap", slot_ids=[left[2], right[2]])
        return success("batch_prepared", batch_id=command["batch_id"],
                       slot_count=len(command["slots"]))

    def _apply(self, command: dict[str, Any], result: dict[str, Any]) -> None:
        if not result["ok"]:
            return
        op = command["op"]
        if op == "add_table":
            table = Table(**result["table"])
            self._tables[table.table_id] = table
        elif op == "prepare_batch":
            batch_id = command["batch_id"]
            for raw in command["slots"]:
                slot = Slot(batch_id=batch_id, **raw)
                self._slots[slot.slot_id] = slot
            self._batches[batch_id] = Batch(batch_id, tuple(raw["slot_id"] for raw in command["slots"]))
        elif op == "publish_batch":
            batch_id = command["batch_id"]
            self._batches[batch_id] = replace(self._batches[batch_id], published=True)
        elif op == "reserve":
            reservation = Reservation(**result["reservation"])
            self._reservations[reservation.reservation_id] = reservation
            self._active[reservation.slot_id] = reservation.reservation_id
            self._next_reservation_id += 1
        elif op == "cancel":
            reservation = self._reservations[command["reservation_id"]]
            if self._active.get(reservation.slot_id) != reservation.reservation_id:
                raise CorruptJournal("Cannot cancel a reservation that does not own its slot")
            self._cancelled[reservation.reservation_id] = result["cancelled_at"]
            del self._active[reservation.slot_id]

    def _replay(self, record: dict[str, Any]) -> None:
        try:
            version = record.get("version")
            if type(version) is not int or version not in (1, 2):
                raise ValueError("unsupported transaction version")
            expected = {"version", "request_id", "at", "command", "result"}
            if version == 2:
                expected.add("actor_id")
            if set(record) != expected:
                raise ValueError("invalid transaction fields")
            request_id = text(record["request_id"], "request_id")
            actor_id = text(record["actor_id"], "actor_id") if version == 2 else None
            at = timestamp(record["at"], "at")
            command = normalize_command(record["command"])
            if actor_id is not None and (command["op"] not in ("reserve", "cancel")
                                         or command["user_id"] != actor_id):
                raise ValueError("invalid authenticated command")
            if command != record["command"] or at != record["at"]:
                raise ValueError("noncanonical transaction")
            key = (actor_id, request_id)
            if key in self._requests:
                raise ValueError("duplicate request_id in journal")
            # Python considers True == 1. Stored results must also have the
            # correct JSON types, not merely compare equal as Python objects.
            if canonical_json(self._plan(command, at)) != canonical_json(record["result"]):
                raise ValueError("result contradicts database state")
            self._apply(command, record["result"])
            self._requests[key] = record
        except (ValueError, KeyError, TypeError, ValidationError) as exc:
            raise CorruptJournal(f"Invalid transaction: {exc}") from exc

    def list_slots(self) -> list[dict[str, Any]]:
        self._check_owner()
        return [dict(asdict(slot), available=slot.slot_id not in self._active)
                for slot in sorted(self._slots.values(), key=lambda item: item.slot_id)
                if self._batches[slot.batch_id].published]

    def snapshot(self) -> dict[str, Any]:
        """An administrative copy, including drafts; not a public availability API."""
        self._check_owner()
        return {"tables": [asdict(item) for item in self._tables.values()],
                "batches": [asdict(item) for item in self._batches.values()],
                "slots": [asdict(item) for item in self._slots.values()],
                "reservations": [dict(asdict(item), cancelled_at=self._cancelled.get(item.reservation_id))
                                 for item in self._reservations.values()],
                "active_reservations": dict(self._active),
                "request_count": len(self._requests)}

    def close(self) -> None:
        if (os.getpid(), threading.get_ident()) != self._owner:
            raise OwnershipError("Only the owner can close the database")
        if not self._closed:
            self._closed = True
            self._journal.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

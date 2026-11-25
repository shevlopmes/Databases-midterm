"""The only place that decides whether a reservation can be committed."""

from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timezone
import os
from pathlib import Path
import threading
from typing import Any

from .errors import (CorruptJournal, IdempotencyConflict, OwnershipError,
                     StorageUnavailable, ValidationError)
from .journal import Journal
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

    def __init__(self, directory: str | Path):
        self._owner = (os.getpid(), threading.get_ident())
        self._closed = False
        self._failed = False
        self._tables: dict[str, Table] = {}
        self._slots: dict[str, Slot] = {}
        self._batches: dict[str, Batch] = {}
        self._reservations: dict[int, Reservation] = {}
        # History and active ownership are separate so cancellation can be added.
        self._active: dict[str, int] = {}
        self._requests: dict[str, dict[str, Any]] = {}
        self._next_reservation_id = 1
        self._journal = Journal(Path(directory))
        try:
            for record in self._journal.transactions():
                self._replay(record)
        except BaseException:
            self._journal.close()
            raise

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
        self._check_owner()
        request_id = text(request_id, "request_id")
        command = normalize_command(command)
        previous = self._requests.get(request_id)
        if previous is not None:
            if previous["command"] != command:
                raise IdempotencyConflict("request_id was already used for a different command")
            return deepcopy(previous["result"])

        at = datetime.now(timezone.utc).isoformat()
        result = self._plan(command, at)
        record = {"version": 1, "request_id": request_id, "at": at,
                  "command": command, "result": result}
        try:
            # No reader can run between planning, durability and applying state.
            self._journal.append(record)
            self._apply(command, result)
            self._requests[request_id] = record
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

    def _replay(self, record: dict[str, Any]) -> None:
        try:
            if set(record) != {"version", "request_id", "at", "command", "result"}:
                raise ValueError("invalid transaction fields")
            if type(record["version"]) is not int or record["version"] != 1:
                raise ValueError("unsupported transaction version")
            request_id = text(record["request_id"], "request_id")
            at = timestamp(record["at"], "at")
            command = normalize_command(record["command"])
            if command != record["command"] or at != record["at"]:
                raise ValueError("noncanonical transaction")
            if request_id in self._requests:
                raise ValueError("duplicate request_id in journal")
            if self._plan(command, at) != record["result"]:
                raise ValueError("result contradicts database state")
            self._apply(command, record["result"])
            self._requests[request_id] = record
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
                "reservations": [asdict(item) for item in self._reservations.values()],
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

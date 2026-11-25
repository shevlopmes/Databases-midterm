"""Domain records and input validation, independent of files and HTTP."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .errors import ValidationError


@dataclass(frozen=True)
class Table:
    table_id: str
    name: str


@dataclass(frozen=True)
class Slot:
    slot_id: str
    table_id: str
    batch_id: str
    starts_at: str
    ends_at: str


@dataclass(frozen=True)
class Batch:
    batch_id: str
    slot_ids: tuple[str, ...]
    published: bool = False


@dataclass(frozen=True)
class Reservation:
    reservation_id: int
    slot_id: str
    user_id: str
    created_at: str


def text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValidationError(f"{field} must be a nonempty string without outer whitespace")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ValidationError(f"{field} must contain valid Unicode") from exc
    return value


def timestamp(value: Any, field: str) -> str:
    value = text(value, field)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"{field} must be an ISO 8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{field} must include a UTC offset")
    try:
        return parsed.astimezone(timezone.utc).isoformat()
    except (OverflowError, ValueError) as exc:
        raise ValidationError(f"{field} is outside the supported date range") from exc


def fields(value: Any, expected: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValidationError(f"{name} must contain exactly: {', '.join(sorted(expected))}")
    return value


def normalize_command(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError("command must be an object")
    op = value.get("op")
    if op == "add_table":
        fields(value, {"op", "table_id", "name"}, "add_table")
        return {"op": op, "table_id": text(value["table_id"], "table_id"),
                "name": text(value["name"], "name")}
    if op == "prepare_batch":
        fields(value, {"op", "batch_id", "slots"}, "prepare_batch")
        batch_id = text(value["batch_id"], "batch_id")
        if not isinstance(value["slots"], list) or not value["slots"]:
            raise ValidationError("slots must be a nonempty list")
        slots = []
        for raw in value["slots"]:
            fields(raw, {"slot_id", "table_id", "starts_at", "ends_at"}, "slot")
            slot = {"slot_id": text(raw["slot_id"], "slot_id"),
                    "table_id": text(raw["table_id"], "table_id"),
                    "starts_at": timestamp(raw["starts_at"], "starts_at"),
                    "ends_at": timestamp(raw["ends_at"], "ends_at")}
            if datetime.fromisoformat(slot["starts_at"]) >= datetime.fromisoformat(slot["ends_at"]):
                raise ValidationError("starts_at must be earlier than ends_at")
            slots.append(slot)
        # Order inside a batch has no meaning; normalize it for idempotency.
        slots.sort(key=lambda item: item["slot_id"])
        return {"op": op, "batch_id": batch_id, "slots": slots}
    if op == "publish_batch":
        fields(value, {"op", "batch_id"}, "publish_batch")
        return {"op": op, "batch_id": text(value["batch_id"], "batch_id")}
    if op == "reserve":
        fields(value, {"op", "slot_id", "user_id"}, "reserve")
        return {"op": op, "slot_id": text(value["slot_id"], "slot_id"),
                "user_id": text(value["user_id"], "user_id")}
    if op == "cancel":
        fields(value, {"op", "reservation_id", "user_id"}, "cancel")
        reservation_id = value["reservation_id"]
        # bool is a subclass of int in Python; True must not mean reservation 1.
        if type(reservation_id) is not int or reservation_id <= 0:
            raise ValidationError("reservation_id must be a positive integer")
        return {"op": op, "reservation_id": reservation_id,
                "user_id": text(value["user_id"], "user_id")}
    raise ValidationError("unknown op; expected add_table, prepare_batch, publish_batch, reserve or cancel")

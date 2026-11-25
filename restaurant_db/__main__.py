"""Run with python -m restaurant_db. No installation or third-party packages."""

import argparse
import json
from pathlib import Path
import sys

from .engine import Database
from .errors import DatabaseError
from .journal import decode_json
from .server import ReservationServer


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=True, sort_keys=True), flush=True)


def seed(db: Database) -> None:
    commands = [
        ("seed.table1", {"op": "add_table", "table_id": "t1", "name": "Window table"}),
        ("seed.table2", {"op": "add_table", "table_id": "t2", "name": "Terrace table"}),
        ("seed.batch", {"op": "prepare_batch", "batch_id": "evening", "slots": [
            {"slot_id": "s1", "table_id": "t1", "starts_at": "2030-01-01T18:00:00+02:00",
             "ends_at": "2030-01-01T20:00:00+02:00"},
            {"slot_id": "s2", "table_id": "t2", "starts_at": "2030-01-01T18:00:00+02:00",
             "ends_at": "2030-01-01T20:00:00+02:00"}]}),
        ("seed.publish", {"op": "publish_batch", "batch_id": "evening"}),
    ]
    for request_id, command in commands:
        result = db.execute(request_id, command)
        emit(result)
        if not result["ok"]:
            raise ValueError("Seed IDs conflict with existing data; use a separate data directory")


def main() -> int:
    parser = argparse.ArgumentParser(description="A single-writer restaurant database")
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("seed", "demo", "inspect", "check", "command", "serve"):
        item = sub.add_parser(name)
        item.add_argument("--data", type=Path, default=Path("data"))
        if name == "command":
            item.add_argument("--request-id", required=True)
            item.add_argument("--file", type=Path, help="Command JSON; omitted means stdin")
        elif name == "serve":
            item.add_argument("--port", type=int, default=8080)
            item.add_argument("--request-timeout", type=float, default=5.0)
            item.add_argument("--max-body-bytes", type=int, default=1024 * 1024)
    args = parser.parse_args()
    try:
        if args.action == "check":
            report = Database.check(args.data)
            emit(report)
            return 0 if report["status"] == "ok" else 1
        with Database(args.data) as db:
            if db.recovered_tail_bytes:
                print(f"Recovered incomplete journal tail: {db.recovered_tail_bytes} bytes", file=sys.stderr)
            if args.action in {"seed", "demo"}:
                seed(db)
                if args.action == "demo":
                    command = {"op": "reserve", "slot_id": "s1", "user_id": "alice"}
                    first = db.execute("demo.alice", command)
                    emit({"alice": first})
                    emit({"bob": db.execute("demo.bob", {"op": "reserve", "slot_id": "s1", "user_id": "bob"})})
                    emit({"same_request_same_result": first == db.execute("demo.alice", command)})
            elif args.action == "inspect":
                emit(db.snapshot())
            elif args.action == "command":
                raw = args.file.read_bytes() if args.file else sys.stdin.buffer.read()
                result = db.execute(args.request_id, decode_json(raw))
                emit(result)
                return 0 if result["ok"] else 1
            elif args.action == "serve":
                with ReservationServer(("127.0.0.1", args.port), db,
                                       args.request_timeout, args.max_body_bytes) as server:
                    emit({"listening": f"http://127.0.0.1:{server.server_port}", "single_writer": True})
                    try:
                        server.serve_forever()
                    except KeyboardInterrupt:
                        pass
        if args.action == "demo":
            with Database(args.data) as reopened:
                emit({"after_restart": reopened.snapshot()})
        return 0
    except (DatabaseError, OSError, ValueError, UnicodeError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

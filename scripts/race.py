"""Concurrent clients, NOT concurrent database writers. Run against a fresh slot."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import re
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--slot-id", default="s1")
    parser.add_argument("--tokens-file", type=Path, required=True,
                        help="Private JSON object mapping user labels to issued tokens")
    parser.add_argument("--clients", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.clients <= 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("clients must be positive; timeout must be finite and positive")
    try:
        credentials = json.loads(args.tokens_file.read_bytes())
        if not isinstance(credentials, dict) or not credentials:
            raise ValueError("empty credentials")
        if any(not isinstance(user, str) or not user or not isinstance(token, str)
               or not re.fullmatch(r"[A-Za-z0-9_-]{43}", token)
               for user, token in credentials.items()):
            raise ValueError("invalid credential")
        tokens = [token for _, token in sorted(credentials.items())]
    except (OSError, ValueError, UnicodeError):
        parser.error("tokens-file must contain a nonempty JSON object of issued tokens")
    barrier = threading.Barrier(args.clients)
    run_id = uuid.uuid4().hex

    def reserve(index: int) -> tuple[str, float]:
        body = json.dumps({"request_id": f"{run_id}.{index}", "command": {
            "op": "reserve", "slot_id": args.slot_id}}).encode()
        request = Request(args.url.rstrip("/") + "/commands", data=body,
                          headers={"Content-Type": "application/json",
                                   "Authorization": f"Bearer {tokens[index % len(tokens)]}"})
        barrier.wait()
        start = time.perf_counter()
        try:
            try:
                response = urlopen(request, timeout=args.timeout)
            except HTTPError as exc:
                response = exc
            with response:
                result = json.load(response)
            return result["code"], time.perf_counter() - start
        except (URLError, OSError, ValueError, KeyError) as exc:
            return f"transport_error:{type(exc).__name__}", time.perf_counter() - start

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.clients) as pool:
        results = list(pool.map(reserve, range(args.clients)))
    elapsed = time.perf_counter() - started
    counts = Counter(code for code, _ in results)
    durations = sorted(duration for _, duration in results)
    print(json.dumps({"clients": args.clients, "results": dict(counts),
                      "elapsed_seconds": round(elapsed, 4),
                      "max_response_seconds": round(durations[-1], 4),
                      "completed_requests_per_second": round(args.clients / elapsed, 2)}, indent=2))
    correct = counts["reserved"] == 1 and counts["slot_taken"] == args.clients - 1
    if not correct:
        print("Expected exactly one winner. Use a fresh published slot; inspect transport errors separately.")
    return 0 if correct else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Load simulation for the 5v5 matchmaker service.

Injects thousands of players concurrently through the real HTTP API:
  1. POST /signup  (persists to Postgres, returns id)
  2. POST /queue/{id}  (publishes an ingest event to Redis)
Then polls /metrics until the in-memory pool drains, and prints throughput.

Pure standard library: no external dependencies.
Usage:  python3 simulate.py [num_players] [concurrency] [base_url]
"""

import sys
import json
import time
import random
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor

NUM_PLAYERS = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
CONCURRENCY = int(sys.argv[2]) if len(sys.argv) > 2 else 64
BASE = sys.argv[3] if len(sys.argv) > 3 else "http://127.0.0.1:8080"


def post(path, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else b""
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(BASE + path, data=data, method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"{}")


def get(path, token=None):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(BASE + path, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"{}")


def triangular_rating():
    # most players mid-skill, few at the extremes (where relaxation earns its keep)
    return int((random.randint(0, 2999) + random.randint(0, 2999)) / 2)


def signup_and_queue(_i):
    rating = triangular_rating()
    res = post("/signup", {"name": f"user{_i}", "rating": rating})
    uid = res["id"]
    token = res["token"]  # JWT issued at signup
    # /queue returns immediately with {"status":"processing"} — fire and forget.
    post(f"/queue/{uid}", token=token)
    return uid, token


def poll_until_matched(uid, token, max_polls=20, interval=0.5):
    """Poll /status every `interval`s until matched or polls exhausted."""
    for _ in range(max_polls):
        s = get(f"/status/{uid}", token=token)
        if s.get("status") == "matched":
            return s
        time.sleep(interval)
    return None


def main():
    print(f"injecting {NUM_PLAYERS} players at concurrency {CONCURRENCY} -> {BASE}")
    start = time.time()

    done = 0
    samples = []  # keep a handful of (uid, token) to demo polling
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        for uid, token in ex.map(signup_and_queue, range(NUM_PLAYERS)):
            with lock:
                done += 1
                if len(samples) < 3:
                    samples.append((uid, token))
    inject_elapsed = time.time() - start
    print(f"injected {done} players in {inject_elapsed:.2f}s "
          f"({done / inject_elapsed:.0f} players/sec through HTTP)")

    # Demonstrate the async poll pattern on a few sample users.
    print("\npolling sample users (every 500ms) until matched:")
    for uid, token in samples:
        result = poll_until_matched(uid, token)
        if result:
            print(f"  user {uid}: matched in match {result['match_id']}")
        else:
            print(f"  user {uid}: still queued after polling window")

    # Poll metrics until the pool stops draining.
    last = -1
    stable = 0
    while stable < 5:
        m = get("/metrics")
        waiting = m["pool_waiting"]
        c = m["counters"]
        print(f"  pool_waiting={waiting:<6} matches_formed={c['matches_formed']:<6} "
              f"committed={c['matches_committed']:<6} avg_gap={c['avg_team_gap']}")
        if c["matches_formed"] == last:
            stable += 1
        else:
            stable = 0
        last = c["matches_formed"]
        time.sleep(0.5)

    total = time.time() - start
    m = get("/metrics")["counters"]
    print("\n=== summary ===")
    print(f"matches formed    : {m['matches_formed']}")
    print(f"matches committed : {m['matches_committed']}  (written to Postgres)")
    print(f"avg queue time    : {m['avg_queue_ms']} ms/player")
    print(f"avg team gap      : {m['avg_team_gap']} rating points")
    print(f"total wall time   : {total:.2f}s")


if __name__ == "__main__":
    main()

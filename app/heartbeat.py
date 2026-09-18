"""
Presence heartbeat: keeps a user's presence key alive with a refreshing TTL.

Usage:
    python heartbeat.py <username> [--interval 5] [--ttl 15]

Run this and leave it running to simulate a connected user. Kill it
(Ctrl+C, or just close the terminal) to simulate a disconnect/crash —
no explicit "offline" message is ever sent; the key just stops being
refreshed and expires on its own.
"""
import sys
import time
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


def run(username: str, interval: int, ttl: int):
    key = f"user:{username}:online"
    print(f"Heartbeating '{key}' every {interval}s (TTL {ttl}s). Ctrl+C to stop.")

    try:
        while True:
            r.set(key, "1", ex=ttl)
            remaining = r.ttl(key)
            print(f"heartbeat sent -> {key} (ttl now {remaining}s)")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped sending heartbeats. Key will expire naturally in up to "
              f"{ttl}s — no manual 'offline' write is made.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    username = sys.argv[1]
    interval = 5
    ttl = 15

    if "--interval" in sys.argv:
        interval = int(sys.argv[sys.argv.index("--interval") + 1])
    if "--ttl" in sys.argv:
        ttl = int(sys.argv[sys.argv.index("--ttl") + 1])

    run(username, interval, ttl)

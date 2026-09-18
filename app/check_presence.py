"""
Check whether a user is currently online.

Usage:
    python check_presence.py <username>
    python check_presence.py <username> --watch   (re-check every second)
"""
import sys
import time
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


def check(username: str):
    key = f"user:{username}:online"
    online = r.exists(key) == 1
    ttl = r.ttl(key)
    status = "ONLINE" if online else "OFFLINE"
    print(f"{username}: {status}  (ttl={ttl})")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    username = sys.argv[1]
    watch = "--watch" in sys.argv

    if watch:
        try:
            while True:
                check(username)
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    else:
        check(username)

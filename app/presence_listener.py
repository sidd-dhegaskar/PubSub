"""
Real-time offline detection via Redis keyspace notifications.

Instead of polling EXISTS/TTL in a loop, this subscribes to Redis's
built-in "key expired" event channel and gets pushed a notification
the instant any presence key dies of its TTL.

Requires:
    CONFIG SET notify-keyspace-events Ex
(this script sets it automatically on startup)

Usage:
    python presence_listener.py
"""
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

PRESENCE_PREFIX = "user:"
PRESENCE_SUFFIX = ":online"


def main():
    r.config_set("notify-keyspace-events", "Ex")
    print("Enabled keyspace notifications (Ex). Listening for expired keys...")

    pubsub = r.pubsub()
    pubsub.subscribe("__keyevent@0__:expired")
    print("Subscribed to __keyevent@0__:expired. Waiting... (Ctrl+C to quit)\n")

    for message in pubsub.listen():
        if message["type"] != "message":
            continue

        expired_key = message["data"]

        if expired_key.startswith(PRESENCE_PREFIX) and expired_key.endswith(PRESENCE_SUFFIX):
            username = expired_key[len(PRESENCE_PREFIX):-len(PRESENCE_SUFFIX)]
            print(f"[EVENT] {username} went OFFLINE (key '{expired_key}' expired)")
        else:
            print(f"[EVENT] key expired: {expired_key} (not a presence key, ignoring)")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped listening.")

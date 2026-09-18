"""
Basic Pub/Sub chat.

Usage:
    python chat.py sub <channel>
    python chat.py pub <channel> <message>
"""
import sys
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


def subscribe(channel: str):
    pubsub = r.pubsub()
    pubsub.subscribe(channel)
    print(f"Subscribed to '{channel}'. Waiting for messages... (Ctrl+C to quit)")

    for message in pubsub.listen():
        if message["type"] != "message":
            continue
        print(f"[{channel}] {message['data']}")


def publish(channel: str, text: str):
    receivers = r.publish(channel, text)
    print(f"Published to '{channel}', delivered to {receivers} subscriber(s)")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    mode = sys.argv[1]
    channel = sys.argv[2]

    if mode == "sub":
        subscribe(channel)
    elif mode == "pub":
        text = " ".join(sys.argv[3:]) or "(empty message)"
        publish(channel, text)
    else:
        print(__doc__)
        sys.exit(1)

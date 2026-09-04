# Project 4: Pub/Sub Chat + Presence Tracker

`PUBLISH`/`SUBSCRIBE`, TTL-based presence, sets up the pub/sub vs. streams contrast for project 5.

## Why This Project

Pub/sub is fire-and-forget: a disconnected subscriber never sees a message, no persistence, no replay. Presence via TTL is a good pattern to internalize — track "online" as a key's existence refreshed by heartbeat, not a boolean you flip; absence of a heartbeat becomes "offline" for free via expiry.

## Build Steps

1. Implement basic chat with `PUBLISH`/`SUBSCRIBE`.
2. Demonstrate the fire-and-forget limitation: disconnect a subscriber and show the missed message is gone for good.
3. Implement presence tracking as a heartbeat-refreshed key with TTL, not a boolean flag.
4. Show that absence of a heartbeat becomes "offline" automatically via expiry.

## Resources

Runs on a single `redis:7-alpine` container.

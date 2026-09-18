# Project 4 Concepts: Pub/Sub, Presence, and Keyspace Notifications

## 1. Pub/Sub Messaging Model

Redis pub/sub works like a **live radio broadcast**, not a mailbox.

- `PUBLISH channel message` — broadcast a message to a named channel.
- `SUBSCRIBE channel` — listen for messages on that channel.
- `PSUBSCRIBE pattern` — subscribe to a pattern of channels (e.g. `chat.*`).
- `UNSUBSCRIBE` — stop listening.

**Key property:** if nobody is subscribed at the moment `PUBLISH` runs, the message is gone forever. There is no queue, no history, no replay.

> Redis pub/sub = a live announcement, not a mailbox. If you're not listening at that instant, it's gone.

### Why there's no persistence (architecture level)

A Redis "channel" is not a key — it doesn't live in the keyspace you'd see with `KEYS *`. Internally:

- Redis keeps an in-memory dict: `channel name -> [subscribed client sockets]`.
- `PUBLISH` looks up the channel, and if there are subscribers, writes the message **directly into each subscriber's socket output buffer** — synchronously, in the same event loop as normal command replies.
- If there are zero subscribers, Redis returns `0` and the message is discarded instantly.
- The message is never written anywhere else. It never touches RDB snapshots or the AOF log, because those only know how to serialize the *keyspace* (keys/values) — and a pub/sub message was never a key.

This is a deliberate design (not a missing feature): no backlog, no consumer offsets, no ack/redelivery. That entire problem space is what **Streams** (project 5) exist to solve — Streams are stored as an actual data type in the keyspace, so they persist and support replay.

### Fire-and-forget demo (Step 2 of the project)

Sequence: subscribe → disconnect → someone else publishes → reconnect → the message sent while disconnected is unrecoverable. This proves the limitation experimentally rather than just stating it.

### The deep-dive mental model: what Redis, clients, and channels actually are

#### 1. First: what is Redis actually?

Forget commands like `SET`, `GET`, `PUBLISH`, etc. for a moment. When you start `redis-server`, you are starting a server program.

Conceptually:

```
                    Redis process
                         │
          ┌──────────────┼──────────────┐
          │              │              │
       Keyspace      Client info    Pub/Sub info
          │              │              │
      user:alice       Client A       chat:room1
      user:bob         Client B       chat:room2
```

Redis is a program running in memory. It maintains various data structures internally to keep track of things.

#### 2. How does your terminal talk to Redis?

When you run `redis-cli`, the CLI creates a TCP connection to Redis. Think of TCP as a pipe between your terminal and Redis:

```
Terminal A                         Redis

redis-cli
   │
   │  TCP connection
   ├──────────────────────────────►
   │
   │  "SUBSCRIBE chat:room1"
   ├──────────────────────────────►
   │
```

Redis now knows: "There is a client connected on this socket." Let's call it `client_42`.

So internally, Redis has something conceptually like:

```
Connected clients

client_42 ─────► TCP socket ─────► Terminal A
client_99 ─────► TCP socket ─────► Terminal B
```

The exact internal implementation is more complicated, but this mental model is excellent for understanding Pub/Sub.

#### 3. What does "single-threaded Redis" mean?

Redis has a central command-processing loop. Very roughly:

```
          Redis

       ┌───────────┐
       │   Event   │
       │   Loop    │
       └─────┬─────┘
             │
       receive command
             │
             ▼
       execute command
             │
             ▼
       send response
             │
             ▼
       receive next command
```

For core command execution, Redis processes commands one at a time. Imagine Redis receives:

```
Client A → SET x 10
Client B → GET x
Client C → PUBLISH room hello
```

It might process them like:

```
1. SET x 10
       ↓
2. GET x
       ↓
3. PUBLISH room hello
       ↓
4. next command...
```

Not:

```
Thread 1 → Client A
Thread 2 → Client B
Thread 3 → Client C
```

for the core command execution.

Why is this useful? Suppose Redis is executing `PUBLISH chat:room1 "hello"`. Another Redis command isn't simultaneously modifying the Pub/Sub subscription data structure in the middle of that command's execution. So you can think:

```
PUBLISH starts
      ↓
Redis handles the whole operation
      ↓
PUBLISH finishes
      ↓
next command
```

This makes the mental model much easier.

Small caveat: modern Redis does use multiple threads for some networking/I/O and other background work, so "Redis is single-threaded" is a useful simplification specifically for understanding core command execution — not a literal statement that the entire Redis process has only one thread.

#### 4. Now let's understand the word "client"

This is important. When you have `Terminal A` running `redis-cli`, the terminal isn't directly "inside Redis." Instead:

```
redis-cli
    │
    │ TCP
    ▼
Redis
```

Redis maintains information about the connected client. For example, conceptually:

```
client_42
 ├── TCP socket
 ├── input buffer
 ├── output buffer
 └── subscription information
```

So Redis can say: "Client 42 is connected through this socket."

#### 5. What is an output buffer?

This is the next important concept. Suppose you run `SET name Alice`. Redis executes it and needs to send `+OK` back to your terminal. Redis doesn't necessarily magically make those bytes appear on your screen.

Conceptually:

```
Redis
  │
  │ put response into output buffer
  ▼
[ +OK\r\n ]
  │
  │ networking layer
  ▼
TCP socket
  │
  ▼
Terminal
```

So every client connection can have an output buffer. Think of it as: "Stuff Redis wants to send to this client." For example:

```
client_42 output buffer

┌────────────────────────┐
│ Redis wants to send:   │
│ "hello"                │
└────────────────────────┘
```

Eventually the networking layer sends those bytes over TCP.

#### 6. Now introduce Pub/Sub

Pub/Sub means Publish / Subscribe. There are two types of clients:

```
Publisher
    │
    │ publishes message
    ▼
 Redis
    │
    │ forwards message
    ▼
Subscribers
```

The important word is channel. Example: `chat:room1`. A channel is basically a name. It isn't a normal Redis key containing messages. That's one of the most important things to understand.

#### 7. Channel ≠ Redis key

You might have a normal Redis key: `SET user:alice:online 1`. That goes into Redis's normal keyspace:

```
Keyspace

"user:alice:online" → "1"
```

But when you do `SUBSCRIBE chat:room1`, Redis does not create `chat:room1 → "some messages"`. Instead, Pub/Sub maintains separate subscription information. Conceptually:

```
Normal keyspace

user:alice:online → 1
user:bob:online   → 1


Pub/Sub registry

chat:room1 → [client_42, client_77]
chat:room2 → [client_99]
```

That's a huge distinction.

#### 8. Terminal A subscribes

Suppose Terminal A runs `redis-cli` and then `SUBSCRIBE chat:room1`. Here's what happens.

**Step 1 — command travels to Redis**

```
Terminal A
    │
    │ SUBSCRIBE chat:room1
    ▼
  Redis
```

**Step 2 — Redis identifies the client**

Redis knows: "This command came from client_42."

**Step 3 — Redis updates Pub/Sub state**

Conceptually:

```
Before:

chat:room1 → [ ]

After:

chat:room1 → [client_42]
```

That's essentially the important state change. Redis is saying: "If somebody publishes to chat:room1, client_42 should receive it."

#### 9. Why does the terminal look "stuck"?

After subscribing, you might see:

```
1) "subscribe"
2) "chat:room1"
3) (integer) 1
```

and then nothing. It looks like Redis stopped responding. But it hasn't. The client is now waiting for Pub/Sub messages. Think:

```
Terminal A

"I'm listening to chat:room1.
 I'll wait here."
```

Meanwhile Redis has:

```
chat:room1
     │
     ▼
 client_42
     │
     ▼
 Terminal A
```

So Terminal A is essentially sitting there waiting for Redis to push something.

#### 10. Now Terminal B publishes

Open another terminal: `redis-cli`. This creates another connection. Conceptually:

```
client_42 ───────► Terminal A
client_99 ───────► Terminal B
```

Terminal B runs `PUBLISH chat:room1 "hello"`. Now the interesting part begins.

#### 11. Redis receives PUBLISH

The command travels:

```
Terminal B
     │
     │ PUBLISH chat:room1 "hello"
     ▼
   Redis
```

Redis knows: "This command came from client_99." It parses the command:

```
PUBLISH
channel = chat:room1
message = hello
```

#### 12. Redis looks up the channel

Redis checks its Pub/Sub registry. Conceptually:

```
Pub/Sub registry

chat:room1 → [client_42]
```

Redis finds `client_42`. So Redis now knows: "I need to deliver this message to client_42."

#### 13. Redis doesn't put the message into the channel

This is the critical part. You might imagine:

```
PUBLISH
   │
   ▼
chat:room1
   │
   ▼
message queue
   │
   ▼
subscriber
```

That's not what Redis Pub/Sub is doing. There isn't a persistent message queue behind `chat:room1`. Instead, conceptually:

```
PUBLISH
   │
   ▼
find subscribers
   │
   ▼
[client_42]
   │
   ▼
write message to client_42's output buffer
```

#### 14. What is actually written?

Redis takes the Pub/Sub message and prepares the appropriate protocol response. Conceptually:

```
client_42 output buffer

┌─────────────────────────────┐
│ "message"                   │
│ "chat:room1"                │
│ "hello"                     │
└─────────────────────────────┘
```

Then Redis's networking layer sends those bytes through:

```
Redis
  │
  ▼
client_42 socket
  │
  ▼
TCP
  │
  ▼
Terminal A
```

Terminal A displays:

```
1) "message"
2) "chat:room1"
3) "hello"
```

#### 15. What happened to the message after that?

This is where Pub/Sub differs from a queue. After Redis delivers `hello`, there isn't a persistent `chat:room1 → ["hello"]` waiting around. The message was essentially:

```
PUBLISH
   │
   ▼
find subscribers
   │
   ▼
send to them
   │
   ▼
done
```

Once the relevant delivery work is complete, Redis doesn't retain the message for future subscribers.

#### 16. What if there are 3 subscribers?

Suppose:

```
chat:room1
    │
    ├── client_42
    ├── client_77
    └── client_91
```

Now `PUBLISH chat:room1 "hello"`. Redis effectively does:

```
find chat:room1
       │
       ▼
[client_42, client_77, client_91]
       │
       ├────► client_42 output buffer
       ├────► client_77 output buffer
       └────► client_91 output buffer
```

So all three receive the message. Redis returns `(integer) 3` because three subscribers received the publication.

#### 17. What if nobody is subscribed?

Suppose `chat:room1 → []`. Now `PUBLISH chat:room1 "hello"`. Redis does:

```
find chat:room1
       │
       ▼
      [ ]
```

There is nobody to send the message to. So `message → nowhere`. Redis returns `(integer) 0`. And importantly, Redis doesn't keep "hello" around waiting for someone to subscribe later.

#### 18. Now Ctrl+C becomes very easy to understand

Suppose Terminal A is subscribed:

```
chat:room1
      │
      ▼
 client_42
      │
      ▼
 Terminal A
```

You press Ctrl+C. The TCP connection closes. Redis eventually detects: "client_42 connection is gone." So Redis removes that client from its subscription state. Conceptually:

```
Before:

chat:room1 → [client_42]


After:

chat:room1 → []
```

Now Terminal B does `PUBLISH chat:room1 "you missed this"`. Redis looks: `chat:room1 → []`. Nobody is listening. Therefore:

```
"you missed this"
       │
       ▼
     nowhere
```

Redis returns `(integer) 0`.

#### 19. The entire story from beginning to end

Here's the complete mental model.

**Start Redis**

```
redis-server
     │
     ▼
Redis process
```

Redis has things like:

```
┌──────────────────────────────┐
│ Redis                        │
│                              │
│ Keyspace                     │
│ Client connections           │
│ Pub/Sub subscriptions        │
│ Event loop                   │
└──────────────────────────────┘
```

**Terminal A connects**

```
Terminal A
    │
    │ TCP
    ▼
 client_42
```

**Terminal A subscribes**

```
SUBSCRIBE chat:room1

Redis records:

chat:room1
      │
      ▼
 [client_42]
```

Meaning: "Send publications for this channel to client_42."

**Terminal B connects**

```
Terminal B
    │
    │ TCP
    ▼
 client_99
```

**Terminal B publishes**

```
PUBLISH chat:room1 "hello"

Redis:

PUBLISH
   │
   ▼
lookup "chat:room1"
   │
   ▼
[client_42]
   │
   ▼
write message to client_42's output buffer
   │
   ▼
TCP socket
   │
   ▼
Terminal A
```

**Terminal A disconnects**

```
client_42 ✕

Redis removes it:

chat:room1 → []
```

**Terminal B publishes again**

```
PUBLISH chat:room1 "you missed this"

Redis:

chat:room1
     │
     ▼
    [ ]

So:

message → nowhere
```

#### 20. The most important distinction: Pub/Sub vs a queue

This is probably the biggest takeaway.

**Redis Pub/Sub**

```
Publisher
    │
    ▼
 Redis
    │
    ├──► Subscriber A
    ├──► Subscriber B
    └──► Subscriber C
```

No persistent message history. If A wasn't connected:

```
Publisher
    │
    ▼
 Redis
    │
    └──► A isn't there ❌
```

Message is gone.

**A persistent queue/stream**

Would instead look conceptually like:

```
Publisher
    │
    ▼
 ┌───────────────┐
 │ message queue │
 │               │
 │ hello         │
 │ message2      │
 └───────────────┘
        │
        ▼
    Consumer
```

The messages have somewhere to remain until consumed/expired. Redis Streams, Kafka, RabbitMQ, etc. are designed for different forms of this persistent messaging model.

#### 21. One correction on "output buffer" wording

It's a useful mental model, but don't interpret it as: Redis literally performs a blocking TCP write and waits for the network. More accurately:

```
Redis command execution
        │
        ▼
prepare/enqueue reply data for subscriber
        │
        ▼
network/event machinery
        │
        ▼
TCP socket
```

This distinction becomes important later when you learn about slow subscribers, output-buffer limits, backpressure, and Redis's networking architecture.

#### The mental model to remember

Forget almost everything else and remember this:

```
             Redis
              │
       ┌──────┴──────┐
       │             │
   Keyspace      Pub/Sub registry
                     │
               chat:room1
                     │
             ┌───────┴───────┐
             │               │
          client_A        client_B
             │               │
             ▼               ▼
          socket          socket
             │               │
             ▼               ▼
        Subscriber A    Subscriber B
```

`SUBSCRIBE` means: "Put my connection in this channel's subscriber list."

`PUBLISH` means: "Look at this channel's subscriber list right now and send this message to those connections."

And that's why: the channel is not a mailbox. It is essentially a routing label → subscriber connections. That one concept makes Redis Pub/Sub much easier to understand.

---

## 2. Presence Tracking via TTL (not a boolean)

### The core idea

Instead of storing `online = true/false` and needing something to flip it back to `false` on disconnect (which a crashed client can't do), track **"has proven it's alive recently"** using a key's existence + an expiry timer.

### The parking meter analogy

- Paying for time = `SET user:alice:online 1 EX 10` (create key, auto-delete in 10s).
- Adding more coins before it runs out = a **heartbeat**: re-run the same `SET` (or `EXPIRE`) every few seconds while still connected, resetting the countdown.
- Meter hits zero on its own = if heartbeats stop (crash, disconnect), nobody has to intervene — Redis deletes the key itself when the TTL expires.
- Checking status = `EXISTS user:alice:online` → `1` = online, `0` = offline.

### Key commands

- `SET key value EX seconds` / `SETEX key seconds value` — set with TTL.
- `EXPIRE key seconds` — refresh/extend TTL on an existing key.
- `TTL key` / `PTTL key` — check remaining time.
- `EXISTS key` — the presence check itself.

### The staleness trade-off

Because "offline" is only detected once a TTL runs out, there is an unavoidable lag window: if a client crashes right after a heartbeat, it still *appears* online for up to one full TTL duration before flipping to offline.

```
t=0s   heartbeat sent → SET user:alice:online 1 EX 10
t=1s   client crashes (no goodbye)
t=2..9s   key still exists → appears ONLINE (but isn't)
t=10s  TTL hits 0 → key deleted → appears OFFLINE
```

This is an accepted trade-off, the same one used by liveness/health-check systems generally (e.g. Kubernetes node health). You can't know about a crash instantly — silence is the only signal, and detecting silence requires waiting.

**Tuning dial:** heartbeat interval vs TTL duration.

| Heartbeat every | TTL | Worst-case staleness | Trade-off |
|---|---|---|---|
| 5s | 10s | ~10s | safe margin against network blips |
| 2s | 3s | ~3s | more accurate, but more Redis traffic and more flicker risk |

Rule of thumb: **TTL ≈ 2–3× the heartbeat interval**, so a single missed heartbeat (network hiccup) doesn't falsely mark someone offline.

### Real-world comparison (e.g. WhatsApp-style apps)

Apps with persistent connections (long-lived sockets) can detect disconnects almost instantly, because the open connection itself acts as a continuous heartbeat — no periodic ping needed, the transport layer notices the drop. They likely combine that with a "last seen" timestamp (stored permanently, not TTL'd) and some grace-period logic to avoid flickering on flaky mobile networks.

The TTL-only pattern this project teaches is the right tool when connections aren't persistent (stateless HTTP, serverless, unreliable mobile wake/sleep cycles) — it's simple and crash-safe without needing to wire up disconnect handlers everywhere.

---

## 3. Active vs. Passive Expiry (how TTL deletion actually happens)

Redis doesn't have a perfect background clock instantly deleting every key the moment its TTL hits zero — with millions of keys, that would be wasteful. Instead it uses two complementary mechanisms.

### Passive expiry — "check when someone asks"

Redis stores the expiry timestamp alongside the key but doesn't proactively remove it. Any time a client touches the key (`GET`, `EXISTS`, `SET`, etc.), Redis first checks "has this passed its expiry time?" — if yes, deletes it right then and treats it as never having existed.

Analogy: a library book with a due date — nobody grabs it off the shelf automatically, but the moment a librarian looks at the record for any reason, they notice it's overdue and process it.

### Active expiry — "Redis proactively sweeps"

To avoid expired keys sitting in memory forever untouched, Redis also runs a background job many times per second that:

1. Randomly samples a small batch of keys that have a TTL.
2. Deletes any that are expired.
3. If a lot were expired in that sample, loops again immediately; otherwise waits and repeats later.

This is a cheap, probabilistic, incremental sweep — not a full scan — to avoid latency spikes.

### Why this matters for correctness

- If your app calls `EXISTS` right after expiry, **passive expiry guarantees a correct answer immediately**, regardless of whether the background sweep has gotten to it yet.
- If nobody ever reads the key again, the **active sweep** eventually reclaims the memory anyway, just not necessarily at the exact expiry millisecond.
- Correctness on reads never depends on the active sweep — it's purely a memory-hygiene mechanism.

This also explains why keyspace notification timing (below) isn't perfectly precise: an "expired" event fires when a key is actually deleted, via *either* path — whichever happens first.

---

## 4. Keyspace Notifications (Pub/Sub + TTL combined)

### 1. First: what problem are we solving?

Imagine you store whether Alice is online using a Redis key:

```
user:alice:online = "1"
```

You give this key a TTL of 30 seconds:

```
SET user:alice:online 1 EX 30
```

Meaning: "Alice is online. Keep this information for 30 seconds."

If Alice sends another heartbeat:

```
SET user:alice:online 1 EX 30
```

the 30-second timer starts again.

So if Alice disappears and never sends another heartbeat, `user:alice:online` eventually expires and disappears.

### 2. But how does our application know she disappeared?

Without keyspace notifications, your application has to keep asking Redis:

```
Does user:alice:online exist?
        ↓
       YES

(wait)

Does user:alice:online exist?
        ↓
       YES

(wait)

Does user:alice:online exist?
        ↓
       NO
```

This is called **polling**. The problem is that you're repeatedly asking Redis "did anything happen?" even when nothing happened.

### 3. What if Redis could tell us?

Instead, imagine Redis saying: "Hey! `user:alice:online` just expired." Your application doesn't need to repeatedly ask — it simply listens. That's the basic idea behind Keyspace Notifications.

```
                Redis
                  |
        user:alice:online expires
                  |
                  ↓
        "Hey! This key expired"
                  |
                  ↓
             Your app
```

### 4. How does Redis "tell" your application?

This is where Pub/Sub comes in — the same mechanism as normal channels:

```
Publisher
    |
    | message
    ↓
 Channel
    |
    ↓
Subscribers
```

When a key event happens, Redis can publish a notification:

```
user:alice:online expires
              ↓
Redis publishes an event
              ↓
"expired"
```

Your application subscribes to that notification channel.

### 5. Redis provides special channels for these events

For example:

```
__keyevent@0__:expired
```

Think of it simply as: "Tell me whenever ANY key expires."

Your application does:

```
SUBSCRIBE __keyevent@0__:expired
```

Now it is listening.

### 6. Then Alice's key expires

```
TTL reaches 0
      ↓
Key expires
      ↓
Redis generates notification
      ↓
__keyevent@0__:expired
      ↓
Your application receives:
"user:alice:online"
```

So your application can now say: **Alice is offline** — no polling required.

### 7. Why is this called "Keyspace Notification"?

Because Redis is notifying you about things happening in its **keyspace** — essentially all the keys stored in Redis:

```
Redis keyspace

user:alice:online
user:bob:online
session:123
cart:456
leaderboard
...
```

Redis can notify you when things happen to these keys, e.g. `SET`, `DEL`, `EXPIRE`, `EXPIRED`, `LPUSH`, etc.

### 8. There are TWO ways Redis can describe the event

Suppose `user:alice:online` expires. Redis can publish it in two styles.

**Style 1 — Keyspace**

- Channel: `__keyspace@0__:user:alice:online`
- Message: `expired`
- Meaning: "Something happened to `user:alice:online` — it expired."

**Style 2 — Keyevent**

- Channel: `__keyevent@0__:expired`
- Message: `user:alice:online`
- Meaning: "Something expired — specifically `user:alice:online`."

For your presence system, the second style is convenient, because you can subscribe once to `__keyevent@0__:expired` and hear about every expiration: `user:alice:online`, `user:bob:online`, `user:charlie:online`, `session:123`, etc.

### 9. How do we turn this feature on?

Redis doesn't enable these notifications by default. You configure Redis:

```
CONFIG SET notify-keyspace-events Ex
```

- `E` means: enable key-event notifications.
- `x` means: notify about expired keys.

So `Ex` roughly means: "Publish notifications when keys expire."

### 10. Put the whole thing together

```
Alice sends heartbeat
        |
        ↓
SET user:alice:online 1 EX 30
        |
        ↓
Redis stores key + 30 sec TTL
        |
        ↓
Alice stops sending heartbeats
        |
        ↓
30 seconds pass
        |
        ↓
Redis expires the key
        |
        ↓
Keyspace notification
        |
        ↓
__keyevent@0__:expired
        |
        ↓
Your application receives
"user:alice:online"
        |
        ↓
Mark Alice OFFLINE
```

### 11. The important catch

Keyspace notifications use Pub/Sub, and Redis Pub/Sub is **not durable**.

```
Redis
  |
  | "Alice's key expired!"
  ↓
Pub/Sub
  |
  ↓
Your application ❌ crashed
```

The message is gone. Redis doesn't save it and give it to you when you come back.

```
App connected     → receives notification ✅
App disconnected  → misses notification ❌
```

This is exactly the same weakness as normal Redis Pub/Sub.

### 12. So why is this useful?

It's extremely useful when you want **real-time reactions** to Redis events and losing an occasional notification is acceptable:

```
Key expires
    ↓
Immediately notify application
    ↓
Update UI / trigger some action
```

But if every event must be reliably processed, Keyspace Notifications + Pub/Sub isn't enough — that's where **Redis Streams** become interesting:

```
Pub/Sub

Event → subscriber
         ↓
       missed ❌


Streams

Event → stored in stream
             ↓
       subscriber can
       read it later ✅
```

### The one mental model to keep

> "Redis watches its own keys and uses Pub/Sub to shout when something happens."

For your project:

```
TTL expires
    ↓
Redis notices
    ↓
Redis publishes
    ↓
Your app is listening
    ↓
Your app knows Alice went offline
```

And the key distinction to remember:

- **TTL/expiry** = the key disappears
- **Keyspace notification** = Redis tells someone that it disappeared
- **Pub/Sub** = the mechanism used to tell them

---

## Summary Table: Redis Usage per Build Step

| Step | Redis mechanism | Status |
|---|---|---|
| 1. Basic chat | `PUBLISH` / `SUBSCRIBE` on a channel like `chat:room1` | ✅ Built & verified |
| 2. Show fire-and-forget loss | Same commands, sequenced to expose the gap (subscribe → disconnect → publish → reconnect → message is gone) | ✅ Built & verified |
| 3. Presence via heartbeat + TTL | `SET presence:<user> 1 EX <n>` or `EXPIRE`, refreshed on a timer | ✅ Built & verified |
| 4. Auto-offline via expiry | `EXISTS` / `TTL` to check status, or keyspace notifications (`__keyevent@0__:expired`) to get pushed an event | ✅ Built & verified |

---

## 5. Hands-On Build Log

### Environment

Single Docker container, matching the project brief:

```bash
docker run -d --name redis-presence -p 6379:6379 redis:7-alpine
docker exec -it redis-presence redis-cli
```

Two rules that avoided confusion during manual testing:

- **zsh prompt** (`sidd4gd@...%`) → only shell commands (`docker exec ...`)
- **Redis prompt** (`127.0.0.1:6379>`) → only Redis commands (`PUBLISH`, `SET`, `TTL`, etc.)

`SUBSCRIBE` blocks the client it's run in — that terminal looks "stuck" but is correctly just waiting to have messages pushed into it. Needed two separate `redis-cli` sessions open side by side to see pub/sub happen live, since a single connection can't simultaneously block on `SUBSCRIBE` and issue other commands.

### Manual verification via `redis-cli` (before writing any app code)

**Pub/Sub, live delivery:**
```
# Terminal A
SUBSCRIBE chat:room1

# Terminal B
PUBLISH chat:room1 "hello from terminal B"   # → (integer) 1, appears instantly in A
```
The `(integer) 1` returned by `PUBLISH` is literally the count of client sockets Redis wrote the message into — not an ack of storage, since nothing was stored.

**Pub/Sub, fire-and-forget loss:**
```
# Terminal A: Ctrl+C (disconnect) — Redis detects the closed TCP socket and
# removes that client from chat:room1's subscriber list immediately
# Terminal B
PUBLISH chat:room1 "you missed this while offline"   # → (integer) 0
# Terminal A: reconnect + SUBSCRIBE again → message never appears, unrecoverable
```
The `0` here isn't "queued, 0 delivered so far" — the subscriber list was empty at lookup time, so the message had nowhere to be written and was discarded within that single command's execution.

**TTL / presence, all three `TTL` return codes observed directly:**
```
SET permanent_key hello
TTL permanent_key            # → -1  (exists, no expiry)

SET user:alice:online 1 EX 30
TTL user:alice:online        # → 16, then → 9 a few seconds later (live countdown)

SET user:alice:online 1 EX 30  # heartbeat: re-SET before expiry
TTL user:alice:online        # → 30  (fully reset, not resumed from where it left off)

EXISTS user:bob:online       # → 0   (never created / already gone)
TTL user:bob:online          # → -2  (does not exist)
```

This run directly confirmed **passive expiry** in action: one `TTL` check landed right at the boundary (returned `0`, i.e. "less than 1 second left, not yet swept"), and the very next command against that same key crossed the expiry timestamp — Redis checked it on that read, deleted it on the spot, and reported `-2`. No background sweep was required for the answer to be correct; the read itself triggered the cleanup.

### App code (Python, `redis-py`)

Location: `app/` folder alongside this file. Set up with a virtual environment (`venv`) and `pip install redis`. Each script opens its own `redis.Redis(host="localhost", port=6379)` connection — a plain TCP client connection, the same kind `redis-cli` itself opens, just driven by Python instead of by typing.

**`chat.py`** — CLI wrapper around `PUBLISH`/`SUBSCRIBE`:
```bash
python chat.py sub chat:room1
python chat.py pub chat:room1 "hello from python"
```
Internally, `sub` calls `redis-py`'s `pubsub()` object, then `.subscribe(channel)` (sends the `SUBSCRIBE` command over the socket) and `.listen()` (blocks, yielding a dict each time a message frame arrives on that same connection — this is `redis-py` parsing the raw RESP protocol replies Redis pushes into the socket). `pub` calls `.publish(channel, message)`, a thin wrapper issuing the `PUBLISH` command and returning the subscriber count. Reproduced the exact same live-delivery and fire-and-forget behavior seen manually, now driven by code instead of typed commands — confirming the mechanism is identical regardless of client.

**`heartbeat.py`** — automates the manual "re-run `SET ... EX`" loop:
```bash
python heartbeat.py alice --interval 3 --ttl 10
```
Runs `r.set(key, "1", ex=ttl)` on a `time.sleep(interval)` loop — each call is a fresh `SET key value EX seconds`, which fully replaces the key's TTL rather than extending the old one (this is why the manual test above showed `TTL` jump back to `30`, not add on top of the remaining `9`). Killing it (Ctrl+C) simulates a crash — no "goodbye" is ever sent, matching the real-world case a boolean flag can't handle, since a crashed process cannot run cleanup code.

**`check_presence.py`** — polls `EXISTS`/`TTL` for a given user, optional `--watch` mode re-checking every second:
```bash
python check_presence.py alice --watch
```
Each check is two independent round-trips to Redis (`EXISTS`, then `TTL`) — this script has no persistent knowledge between checks, it's stateless polling exactly as described conceptually above. Confirmed: while `heartbeat.py` ran, this printed `ONLINE` continuously; after killing the heartbeat, it flipped to `OFFLINE` within one TTL window, with zero explicit "set offline" code anywhere in the codebase.

**`presence_listener.py`** — the keyspace-notifications stretch goal, replacing polling with a push-based listener:
```bash
python presence_listener.py
```
On startup, runs `r.config_set("notify-keyspace-events", "Ex")` itself — the exact `CONFIG SET` from Section 4, just issued by code instead of typed manually — then opens a `pubsub()` object and `.subscribe("__keyevent@0__:expired")`. This is a completely ordinary `SUBSCRIBE` from Redis's point of view; the only special thing is that Redis itself is the publisher on that channel, triggered internally whenever any key's deletion (from either active or passive expiry) fires. The script parses the arriving key name (`user:<name>:online`) back into a username via a plain string strip, no Redis command involved in that step — that parsing happens entirely client-side once the message payload is in hand.

**Full loop verified end-to-end, three terminals:**
1. `presence_listener.py` running and idle, blocked on `.listen()`, waiting.
2. `heartbeat.py alice` sending heartbeats — listener stays silent (nothing has expired, so `__keyevent@0__:expired` has had no `PUBLISH` fired on it).
3. Kill the heartbeat (Ctrl+C, simulating a crash) → within the TTL window, the listener — with no polling, no manual `EXISTS` check anywhere in its code — prints:
   ```
   [EVENT] alice went OFFLINE (key 'user:alice:online' expired)
   ```

This closes the loop described in Section 4: heartbeat (`SET ... EX`) → TTL countdown → expiry (active sweep or passive touch, whichever fires first) → Redis's internal `PUBLISH` to `__keyevent@0__:expired` → the listener's blocked `SUBSCRIBE` connection receives the message → app reacts live. Every arrow in that chain was independently verified by hand in `redis-cli` before being wired together in code, so nothing in the automated version was "trust the library" — each piece had already been seen working manually first.

### Open thread for Project 5 (Streams)

`presence_listener.py` inherits pub/sub's fire-and-forget weakness at the code level, not just conceptually: if the process isn't running (or is mid-restart) at the exact moment a key's expiry event fires, that `PUBLISH` finds zero subscribers and the event is discarded — same as the fire-and-forget chat demo in Step 2, just happening automatically instead of by hand. The only recovery path with the current design is falling back to `check_presence.py`-style polling to reconcile state after a restart, since there is no log of "which offline events happened while I was down." This is precisely the gap Redis Streams (project 5) are built to close: an appended, persisted, replayable structure instead of a live-only broadcast, so a consumer that reconnects late can catch up rather than silently drifting out of sync.

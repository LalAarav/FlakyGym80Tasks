# fanout: webhook fan-out service — behavioural specification

`fanout` delivers events to webhook subscribers. A publisher calls
`Publisher.publish(topic, body, event_id)`. The event is stored durably as one job in a
file-backed queue (`src/queue`). A `Dispatcher` runs a worker pool over that queue and
delivers each event to each of its target subscribers over RPC (`src/rpc`). Each
completed delivery is recorded in a durable ledger (`src/fanout/ledger.py`). Subscriber
lists are served from a read-through TTL cache (`src/cache`) in front of the
`SubscriptionRegistry`.

Subscribers deduplicate deliveries by the RPC idempotency key: two requests with the
same key have one effect. A subscriber may also be slow, drop connections (before or
after applying a request) or be unreachable for a while.

The service-level guarantees come first. Every component has its own contract after
that, and the service relies on all of them.

## Service guarantees

**Exactly-once effect.** For each event and each target subscriber, the
subscriber applies the event's effect exactly once. This holds even when deliveries are
retried within an RPC call, when the whole job is retried later, and when a job is
redelivered after its lease expired.

**Eventual delivery.** An event is delivered to every one of its target
subscribers. A subscriber that fails transiently is retried, through job retries, until
it has received the event. Failures of one subscriber do not cause the event to be
dropped for it or redelivered to the others.

**Subscription changes are immediate.** The targets of an event are the topic's
subscribers at the moment `publish` is called. Once `subscribe()` or `unsubscribe()` has
returned, every subsequent `publish` reflects the change.

**Truthful ledger.** The ledger says a delivery is complete only after the
subscriber acknowledged it (or permanently rejected it with an application error). A
rejection is final: that delivery is settled and is not attempted again.

**Idempotent publish.** Publishing an `event_id` that was already published has no
effect and returns `None`.

**Arbitrary identifiers.** Event ids, topics and subscriber names are arbitrary
non-empty strings of up to 200 characters, and any Unicode character except NUL may
appear in them. Distinct
(event id, subscriber) pairs are distinct deliveries. One webhook endpoint may be
registered under several subscriber names, and it then receives one delivery per name.

**Crash safety.** The process may die at any instant: between any two `FsOps`
calls, or during one. A file being written by `write_bytes` may then be left holding
any prefix of its data, and a file being created by `create_exclusive` may be left
empty. After a restart on the same directories (new `JobStore`, `DeliveryLedger` and
`Dispatcher` objects over the same paths), “Exactly-once effect” and “Eventual delivery” hold for:

- every event whose `publish` returned its id, including events a worker of the dead
  process was handling; and
- every event whose `publish` was interrupted by the crash, once the producer
  publishes it again with the same `event_id`. That retry either returns the id, or
  returns `None` because the event is already queued.

**Failure isolation.** A subscriber that keeps failing delays only the deliveries
addressed to it. While it is down, other events still reach healthy subscribers, even
with a single worker.

**Durable subscriptions.** A `SubscriptionRegistry` given a `KVState` records every
change in it. A registry opened on the same state after a crash (see “Crash safety”) has exactly the
subscriptions the live registry had: every `subscribe` and `unsubscribe` that returned, in
the order they took effect, including racing changes to the same subscription, for any
topic and subscriber names allowed by “Arbitrary identifiers”.

**Dead letters.** A `Dispatcher` given `deadletters` and `max_job_attempts` parks
an event in the dead-letter store, under its event id, once its job has failed
`max_job_attempts` times, and settles the job. `Dispatcher.replay(event_id)` queues a
parked event again and returns `True` if this call re-queued it. A replayed event is
then delivered to the target subscribers that do not have it yet; the others are not
sent it again, and “Exactly-once effect” holds across any number of parkings and replays.

**Signed deliveries.** A `Dispatcher` given a `Signer` adds a `signature` member
to every delivery payload: `{"headers": ..., "body": ...}`, the headers and the body
(decoded as UTF-8) of `Signer.sign` applied to the event's `event_id`, `topic` and
`body`. A `Verifier` over the same keyring accepts it. Each time the dispatcher handles
an event's job it signs that delivery afresh, with the active key and the current time,
so a delivery that is retried or replayed long after it was published still verifies
within the verifier's tolerance. (Attempts within one RPC call carry identical bytes;
subscribers recognise those by the idempotency key.) A `Publisher` given the same
`Signer` refuses, by raising before anything is queued, an event body that the
canonical encoding cannot represent once the body has been stored as JSON.

**Delivery metrics.** A `Dispatcher` given a metrics `Registry` keeps the counter
`deliveries` with the label `outcome` set to `delivered` or `rejected`. It is incremented
exactly once for each (event, target subscriber) that is settled, however many times the
event's job is handled, including when two workers handle it at once after a lease
expired.

**SLO feed.** A `Dispatcher` given an `alert` (any object with
`record_outcome(ok)`, such as a `BurnRateAlert`) records one outcome per delivery attempt
its RPC client made: `True` when the subscriber took or rejected the delivery, `False`
for a transport failure. A delivery that an open circuit breaker refused sent no request
and is not recorded.

## Architecture constraints

- Filesystem access goes through `FsOps` (`src/fsops.py`); no other module imports
  `os` or calls `open`.
- Time goes through a clock object (`src/clock.py`); no other module imports `time`.
- Socket access goes through `src/rpc/conn.py`; no other module imports `socket`.
- Keep these seams. The behaviour in this document is verified through them.

## Component contract: durable queue (`src/queue`)

`relayq` is an in-process job queue backed by a directory on local disk. Producers call
`JobStore.enqueue`; a `WorkerPool` leases jobs, runs a handler on each, and acknowledges
them. The store must survive process restarts and be safe to use from many threads.

### Architecture constraints

- All filesystem access goes through the store's `FsOps` instance (`src/fsops.py`).
  No other module imports `os` or calls `open`.
- All time-dependent behaviour goes through a clock object (`src/clock.py`).
  No other module imports `time`.
- These seams exist so the behaviour below can be verified against slow filesystems,
  adversarial thread schedules and adjusted clocks. Keep them.

### Guarantees

**Atomic visibility.** A job becomes visible to `lease()` only once its file is
completely written. No reader may ever observe, claim or parse a partially written job
file, whether it is being created by `enqueue` or rewritten by `nack`.

**Exclusive leases.** At any instant a job is leased to at most one holder. Any
number of threads calling `lease()` concurrently receive distinct jobs (or `None`).

**Lease timing.** Lease deadlines are measured on a clock that never jumps.
Stepping the wall clock forwards or backwards (NTP corrections, manual changes) must
neither shorten nor extend a lease.

**Exact statistics.** `Stats` counters are exact when updated from many threads.

**FIFO.** A single store hands out ready jobs in the order they were enqueued,
including jobs enqueued within the same clock tick and across a restart of the store on
the same directory.

**Orderly shutdown.** `WorkerPool.stop(drain=True)` returns only after every job
that was enqueued before the call has been acknowledged, and after every pool thread has
exited. A handler that is still running counts as unsettled work.

**Fenced settlement.** Once a lease has expired and its job has been re-granted,
`ack()` or `nack()` with the old lease raises `StaleLease` and has no effect on the new
holder's job.

**Concurrent maintenance.** `reap()` may run at any time, concurrently with
`lease()`, `ack()` and `nack()` from other threads, without raising and without
invalidating leases granted while it runs.

**Idempotent enqueue.** For a given `job_id`, exactly one job is ever created, even
when several producers enqueue that id at the same time. Later attempts return `None`.

**Parallelism.** Handlers on different workers run concurrently; the pool must not
serialize handler execution.

### Delivery semantics

Delivery is at-least-once: a job whose lease expires is returned to the ready queue and
may be handled again. “Fenced settlement” is what keeps a slow, expired holder from settling a job that
now belongs to someone else.

## Component contract: RPC client (`src/rpc`)

`wirecall` is the client side of an internal RPC protocol. Requests and responses are
JSON objects sent as frames: a 4-byte big-endian length followed by the UTF-8 body.
Every request carries a unique `id`; the server echoes it in the response. A request may
also carry an idempotency `key`, which the server uses to recognise and discard
duplicates of the same logical call.

`RpcClient.call` runs one logical call over a `ConnectionPool`, retrying transient
failures (`ConnectionError`, `TimeoutError`, `ProtocolError`) when that is safe, with an
optional `CircuitBreaker` in front. `TokenBucket` is a standalone rate limiter used by
callers.

A response with `ok: false` means the server processed the request and rejected it.
`call` then raises `RemoteError` at once and never retries it, and the breaker counts the
exchange as a success, because the server answered.

### Architecture constraints

- All socket access goes through `src/rpc/conn.py` (`Dialer`, `Connection`). No other module
  imports `socket`. The pool only ever obtains connections from its dialer.
- All time-dependent behaviour goes through a clock object (`src/clock.py`). No other
  module imports `time`. Socket timeouts are set with `Connection.settimeout`.
- These seams exist so the behaviour below can be verified against fragmenting, slow and
  lossy networks and adjustable clocks. Keep them.

### Guarantees

**No unsafe retries.** A call that is neither `idempotent=True` nor `dedupe=True`
is attempted at most once. If its attempt fails, the error propagates to the caller,
because the server may already have applied it.

**Stable idempotency keys.** With `dedupe=True`, one key is generated per logical
call and the same key is sent on every attempt of that call, so the server applies it at
most once. Different calls get different keys.

**Overall deadline.** `deadline` bounds the entire call: every attempt, every
backoff pause and every wait for a pooled connection. A call that cannot finish in time
raises `DeadlineExceeded` no later than its deadline.

**Clean connection reuse.** A connection goes back to the pool for reuse only after
a complete, matching request/response exchange. After a timeout, a decode error or any
other interrupted exchange, it is closed, because unread bytes of an old response
may still arrive on it.

**Framing.** A frame may arrive split across any number of reads, down to one byte
per read, including inside the 4-byte header. The client always reassembles complete
frames.

**Pool waiters.** A caller blocked in `ConnectionPool.acquire` is woken as soon as
capacity frees up: when a connection is released for reuse, and when one is discarded.

**Pool capacity.** At most `max_size` connections exist at once. A dial that fails
does not consume capacity.

**Breaker probes.** After `reset_timeout`, an open breaker admits exactly one probe
call, even when many threads call `allow()` at the same moment. Until that probe
reports back, everyone else is refused.

**Breaker threshold.** The breaker opens after `failure_threshold` *consecutive*
failures. A success resets the count.

**Rate limiting.** `TokenBucket.try_acquire` never admits more calls than the
bucket holds, under any thread interleaving. Tokens refill continuously at `rate` per
second up to `burst`.

**Concurrency.** Calls from different threads proceed in parallel, up to the pool
size. The client must not serialize calls.

## Component contract: cache (`src/cache`)

`memo` is an in-process cache. `TTLCache` stores values with a time-to-live and a size
bound. `LoadingCache` wraps it as a read-through cache: `get(key)` returns the cached
value or calls the user's `loader(key)`, shares that call between concurrent callers,
honours invalidation, and can refresh hot entries in the background before they expire.
Loaders report an expected failure by raising `LoadError`.

### Architecture constraints

- All time-dependent behaviour goes through a clock object (`src/clock.py`); no other
  module imports `time`. The cache and loading cache take the clock they are given.
- `Stats` (`src/stats.py`) is the only place counters live.

### Guarantees

**Expiry instant.** An entry stored at monotonic time `t` with lifetime `ttl` is
returned by `get` exactly while `t <= now < t + ttl`. At `now == t + ttl` it is gone.

**Absolute TTL.** Reading an entry never extends its lifetime. Only `put` (including
a reload) sets a new expiry.

**LRU eviction.** When `max_entries` is exceeded, the least recently *used* entry is
evicted. Both reads and writes count as use.

**Single flight.** Concurrent `get` misses for the same key result in exactly one
loader call; every caller receives that call's result.

**Failure propagation.** If the shared load fails with `LoadError`, the caller that
ran it and every caller waiting on it receive a `LoadError` promptly. Nobody waits
for a timeout.

**Invalidation wins.** After `invalidate(key)` returns, no value whose load *started*
before that call may be stored in the cache, and no `get` that starts after it returns
receives such a value. The next `get` triggers a fresh load.

**Failures are not cached.** A failed load leaves no trace. The next `get` for that
key starts a new load.

**Shutdown.** `close()` returns only when no background refresh is running, and no
background load starts after it returns.

**One refresh at a time.** With refresh-ahead enabled, any number of hits on an entry
that is about to expire schedule at most one background reload of that key until that
reload completes.

**Exact statistics.** `Stats` counters are exact under concurrent updates.

**Parallel loads.** Loads of *different* keys run concurrently. The cache must not
serialize unrelated loads.

## Component contract: durable state journal (`src/journal`)

`ledgerlog` is an in-process key-value store whose contents survive crashes and
restarts. `KVState(directory)` keeps a `str -> bytes` map in memory and makes every
change durable in an append-only write-ahead log on local disk before the change
becomes visible. Several changes can be committed together as one *batch*
(`state.batch()` / `state.apply(ops)`). Because the log only grows, the store
periodically *compacts*: it writes a snapshot of the whole map and drops the log
the snapshot replaces (`compact()`, or `maybe_compact()` when the log size or age
limit is reached). Opening a `KVState` on an existing directory *recovers* the
state the previous process left there, however that process ended.

The store will hold the fanout service's subscription registry, so the registry
survives restarts.

### Architecture

- `src/journal/codec.py` encodes operations and batches; `src/journal/journal.py` frames batches
  into checksummed log records, appends them and replays them; `src/journal/snapshot.py`
  writes and loads snapshots, performs the compaction switch-over and recovery;
  `src/journal/kvstate.py` is the thread-safe public API.
- All filesystem access goes through the store's `FsOps` instance (`src/fsops.py`).
  No other module imports `os` or calls `open`. `FsOps.append_bytes`, `write_bytes`
  and `create_exclusive` are durable when they return; a process may die *during*
  any of them, leaving any prefix of the data on disk, and may die before or after
  any `rename`, `remove` or `truncate`.
- All time-dependent behaviour goes through a clock object (`src/clock.py`). No
  other module imports `time`.
- These seams exist so the behaviour below can be verified against crashing,
  slow and instrumented filesystems and adversarial thread schedules. Keep them.

### Data model

- Keys are non-empty `str` values of at most 255 characters; any Unicode code
  points are allowed except lone surrogates. Values are `bytes` of any length from
  0 up to 16 MiB. The empty value is a value: a key mapped to `b""` is present.
- `put`, `delete` and `apply` validate their arguments before anything is written
  and raise `TypeError` / `ValueError` for keys or values outside the data model.
- A *crash* is the process dying at an arbitrary instant: the `KVState` object is
  abandoned without `close()` and a new one is opened on the same directory.
  An operation is *acknowledged* once its call has returned.

### Guarantees

**Durability.** Once `put`, `delete`, `apply` or a batch commit returns, its
effect survives any later crash, whatever the store was doing at that moment —
writing other batches, compacting, or recovering. An operation that was in progress
when the process died takes effect entirely or not at all.

**Torn tails.** A crash part-way through writing a log record leaves an
incomplete record at the end of the log. Recovery discards it (the interrupted
operation does not take effect) and keeps every record before it. The store then
carries on so that everything written after the recovery is itself durable across
later restarts: a torn tail from one run never costs data from the next.

**Integrity.** A log record is applied only if every one of its bytes — header
fields and body alike — is exactly as it was written; the checksum protects all of
it. Replay applies records in order and stops at the first record that is
incomplete or fails verification: that record and everything after it are
discarded, and nothing before it is lost.

**Atomic batches.** A batch takes effect entirely or not at all, both for
concurrent readers of the live store and across a crash, whatever the number of
operations in it and whatever the size of their values.

**Crash-safe compaction.** A crash at any point of a compaction loses no
acknowledged write and resurrects nothing: the recovered state is exactly the
acknowledged state. This holds for every compaction over the life of a store, which
may compact any number of times.

**Compaction alongside writers.** `compact()` may run while other threads put,
delete and commit batches. Writers are not held up for the duration of the snapshot
I/O; writes that happen during a compaction are neither lost (now or after a restart)
nor able to make the compaction fail.

**Order.** Changes are applied in one total order, the order in which their
calls took effect in the live store; sequence numbers returned by writes increase
strictly in that order. Recovery reproduces exactly the state the live store had:
for every key, the same final value, even when many threads wrote the same keys
concurrently.

**Repeatable recovery.** Opening a store is itself crash-safe and repeatable:
opening it any number of times in a row — with crashes during opening, and with or
without writes in between — always yields the same state. Recovery never removes or
rewrites on-disk data that the recovered state still depends on.

**Keys and values round-trip.** Every key and value inside the data model —
including keys of the maximum length in any script, the empty value, and values of
any length up to the maximum — reads back identically from the live store, after a
restart, and after a compaction.

### Other documented behaviour

- `get(key, default=None)`, `contains(key)`, `items()` (a consistent copy),
  `keys(prefix="")` (sorted) and `len()` read the live map.
- `batch()` returns a context manager: the batch commits when the block exits
  normally and is discarded, with nothing written or applied, when the block raises.
  An empty batch commits nothing and returns `None`.
- After `close()`, writes raise `StoreClosed`. After a write fails with an `OSError`,
  further writes raise `StoreClosed` until the store is reopened.
- After a compaction completes, files of older generations are gone: disk usage is
  bounded by the size of the live state plus the log written since the last
  compaction.

## Component contract: webhook signing (`src/signing`)

`sigil` signs outgoing webhook deliveries and verifies incoming ones. The fanout
dispatcher signs every delivery with it; subscribers (in any language) verify with it
or with their own implementation of the formats below.

- `canonical.encode(value)` turns a JSON value into the canonical bytes that are sent
  as the HTTP body and signed.
- `Keyring(root, overlap=…, fs=…, clock=…)` holds signing keys by id, rotates them and
  persists them under directory `root`. Keys are `add`ed (staged), `activate`d (the
  previously active key becomes *superseded*), or `retire`d (removed, secret discarded);
  `rotate(id, secret)` is `add` + `activate` as one change. `signing_key()` returns the
  active key's id and secret; `verification_secret(id)` returns a key's secret if that
  key may verify right now.
- `Signer(keyring, clock=…, nonce_factory=…)`: `sign(payload)` returns a
  `SignedMessage` with `headers` and `body`.
- `Verifier(keyring, tolerance=…, clock=…, replay=…)`: `verify(headers, body)` returns
  a `Verified(key_id, timestamp, nonce, payload)` or raises a `VerificationError`
  subclass. `ReplayCache(max_entries=…, clock=…)` is the verifier's nonce memory.

### Architecture constraints

- All filesystem access goes through an `FsOps` instance (`src/fsops.py`); no other
  module imports `os` or calls `open`. All time goes through a clock object
  (`src/clock.py`); no other module imports `time`. Every component takes the `fs` /
  `clock` it is given. These seams exist so the guarantees below can be verified against
  crashing filesystems, adversarial thread schedules and stepped clocks. Keep them.
- Two clocks with different jobs. Signature timestamps and the freshness check are
  *wall-clock* time (`clock.wall()`), because sender and receiver must agree on them.
  Windows a process keeps for itself (a key's overlap window, how long a nonce is
  remembered) are measured on the *monotonic* clock (`clock.monotonic()`) and must not
  shorten or lengthen when the wall clock is stepped. The only exception is crossing a
  restart, where monotonic readings are meaningless (“Overlap window”).
- One `Keyring` per directory at a time. Readers (`signing_key`,
  `verification_secret`) may run concurrently with rotations from other threads.

### Formats (byte-level contracts)

**Canonical encoding.** UTF-8 bytes of a JSON text with no whitespace outside strings.
`None`, `True`, `False` are `null`, `true`, `false`. Lists and tuples are arrays,
elements in order, separated by `,`. A dict is an object whose keys must all be `str`
(any other key type raises `CanonicalError`); members are written `"key":value`,
separated by `,`, in ascending order of the key's UTF-16 code-unit sequence (the order
RFC 8785 and JavaScript's default sort use). Strings are written between `"` as raw
UTF-8 with these escapes only: `\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t`, and every other
character below U+0020 as `\u00XX` with lowercase hex; nothing else is escaped. Strings
that are not valid Unicode (lone surrogates) raise `CanonicalError`. Numbers are encoded
by value: an `int`, or a finite `float` with no fractional part, is written as the exact
decimal integer it equals (optional `-`, no leading zeros, no point, no exponent; minus
zero is `0`); any other finite float is written as Python's `repr()` of it. NaN and
infinities raise `CanonicalError`, as does any other type and nesting deeper than 64
containers.

**Signature headers.** `sign` produces exactly these four headers, and the body is the
canonical encoding of the payload:

- `Sigil-Key-Id`: the id of the key that signed (1-64 characters of `[A-Za-z0-9._-]`).
- `Sigil-Timestamp`: the signer's wall clock at signing time, rounded down to whole
  seconds since the Unix epoch, in ASCII decimal digits.
- `Sigil-Nonce`: 32 lowercase hexadecimal characters, unique per signed message.
- `Sigil-Signature`: `v1=` followed by the lowercase hex HMAC-SHA256, keyed with the
  key's secret, of the *signed message*.

The signed message is the bytes `sigil-v1`, LF, key id, LF, timestamp, LF, nonce, LF,
body — the three header values in ASCII exactly as they appear in the headers, LF being
the single byte 0x0A, and the body exactly as sent. Verifiers look headers up by name
case-insensitively; a missing, duplicated or malformed header is a `MalformedSignature`.

### Guarantees

**Canonical member order.** The canonical encoding of an object depends only on its
set of members, never on dict insertion order, and members appear in the order defined
above for any keys whatsoever, at every nesting level.

**Numbers by value.** Two numbers that are equal as numbers encode to the same
bytes, whatever their Python type or magnitude. Integral values are always written as
their exact decimal integer. `True` and `False` are not numbers and stay `true` and
`false`.

**Strings.** Strings are written as raw UTF-8 with exactly the escapes listed in the
format; non-ASCII characters are never `\u`-escaped, and invalid strings are rejected
rather than written.

**Signature format.** Every `SignedMessage` carries the four headers and a
signature that any independent implementation of the format above reproduces from the
key's secret, the header values and the body.

**Consistent signing key.** Every signature `sign` returns was computed with the
secret of the key named in its `Sigil-Key-Id`, even while other threads rotate the
keyring. In particular, as long as that key can still verify, the message verifies.

**Overlap window.** When a key stops being the active key it is superseded and
continues to verify for exactly `overlap` seconds, measured on the monotonic clock from
the instant it stopped being active: it verifies while less than `overlap` seconds have
elapsed and is refused from then on. Wall-clock steps do not change this. A restart does
not extend the window: after the keyring is reloaded, a superseded key verifies only for
what remains of its original window (the elapsed part measured on the wall clock across
the restart, since monotonic readings do not survive one), and never for more than
`overlap` seconds after loading. `retire` makes a key unusable immediately, whatever
remains of its window. Staged keys never verify.

**Durable rotation.** When `add`, `activate`, `rotate` or `retire` returns, the
change is on disk: a new `Keyring` opened on the same directory afterwards sees that
change, and no older state. This holds when several threads rotate concurrently: the
persisted state is always the latest one the keyring published.

**Crash-safe persistence.** If the process dies at any point during a keyring
change (including in the middle of writing a file), a `Keyring` opened on the directory
afterwards loads successfully and holds either the state before that change or the state
after it.

**Freshness.** A message with timestamp `ts` passes the freshness check exactly
when `ts - tolerance <= now <= ts + tolerance`, where `now` is the verifier's wall-clock
reading (with its fractional part, not rounded). Messages outside that interval are
rejected with `StaleMessage`.

**The bytes are what is signed.** Verification is computed over the received body
bytes exactly as received. A body that differs from the signed body in any byte is
rejected, even if it parses to an equal JSON value.

**Replay protection.** The nonce of every accepted message has a *retention
period* of `2 × tolerance` seconds of monotonic time, starting when the message was
accepted. While it lasts, any message carrying that nonce is rejected, however the wall
clock moves in the meantime (a wall-clock step can make an old timestamp fresh again).
Of several copies of one message verified concurrently, exactly one is accepted. The
replay cache is bounded (`max_entries`) but never forgets a nonce before its retention
period ends: if accepting a new message would require that, the new message is rejected
with `ReplayCacheFull` instead. Once a nonce's retention period is over it no longer
counts towards the bound.

**Rejection has no side effects.** A delivery that is rejected for any reason
(malformed, stale, unknown or refused key, bad signature, malformed body) does not
influence whether any other delivery, earlier, later or concurrent, is accepted. Only
accepted deliveries are remembered by the replay cache.

## Component contract: retry scheduling and dead letters (`src/retry`)

`backoffice` decides when a failed job may run again and keeps the jobs that ran out
of retries. It has three parts and a small piece of glue:

- `RetryPolicy` (`src/retry/policy.py`) computes backoff delays: exponential growth from
  `base` by `multiplier`, capped at `cap`, with optional jitter drawn from an
  injectable `random.Random`. It tracks, per key, how many times the key has failed,
  and enforces `max_attempts`. `call(fn)` is a synchronous retry helper that sleeps
  on the policy's clock.
- `DelayScheduler` (`src/retry/timers.py`) holds keyed items and hands each one to a
  `sink(key, payload)` callback when it comes due on the scheduler's monotonic
  clock. Items can be scheduled, rescheduled and cancelled; a background thread
  (`start()` / `stop()`) does the releasing, or `pump()` releases due items on the
  calling thread for a scheduler that was never started.
- `DeadLetterStore` (`src/retry/deadletter.py`) is a file-backed store of `DeadLetter`
  records (item id, JSON payload, attempt count, reason, time of parking) with
  `put`, `get`, `list`, `replay(item_id, target)`, `purge` and `purge_all`.
- `Retrier` (`src/retry/retrier.py`) routes a failure to a delayed retry or, once the
  policy gives up, to the dead letters, and replays dead letters with a fresh budget.

The fanout dispatcher will use these instead of retrying failed jobs immediately.

### Architecture constraints

- All time goes through a clock object (`src/clock.py`); no other module imports
  `time`. Delays and due times use `clock.monotonic()`; `DeadLetter.dead_at` uses
  `clock.wall()`; `RetryPolicy.call` waits with `clock.sleep()`. Tests drive
  components with fake clocks whose `monotonic()` advances only when told to and
  whose `sleep()` advances it.
- All filesystem access goes through an `FsOps` instance (`src/fsops.py`); no other
  module imports `os` or calls `open`.
- `DelayScheduler` waits for the earliest due time on a condition variable, re-reading
  its clock at least every `max_wait` real seconds when `max_wait` is set (`None`:
  sleep until the earliest due time). The sink runs on the scheduler's thread, one
  item at a time, and must not raise.
- Counters live in `Stats` (`src/stats.py`).

### Guarantees

### Backoff policy

**Exponential ceiling.** For failure number `n` (the first failure is `n = 1`),
the un-jittered delay `ceiling(n)` is `min(cap, base * multiplier ** (n - 1))`. This
holds for every `multiplier > 1`, every `0 < base <= cap`, and every positive integer
`n` with no upper limit: a key may fail millions of times, and `ceiling` and
`next_delay` must still return a finite number no greater than `cap` (and never raise)
however large `n` is. `ceiling` is non-decreasing in `n`.

**Jitter.** `next_delay(key)` returns, for the key's current failure number `n`:
with `jitter="none"`, exactly `ceiling(n)`; with `"full"`, a value uniformly
distributed over `[0, ceiling(n)]`; with `"decorrelated"`, a value uniformly
distributed over `[base, min(cap, 3 * p)]`, where `p` is the delay previously returned
for the same key (or `base` before its first delay). Every draw comes from the
policy's `random.Random`. These bounds and distributions hold at every attempt, the
ten-thousandth as much as the first; in particular the cap limits the range a delay is
drawn from, it does not replace the draw.

**Per-key state.** Each key's failure count and previous delay are its own;
failures of one key never change another key's delays or budget. With `max_attempts =
m`, the first `m - 1` failures of a key each get a delay and the `m`-th (and every
later one) gets `None`; `attempts(key)` is the number of failures recorded. After
`reset(key)` the key is indistinguishable from one the policy has never seen: its
next delay is drawn exactly as a brand-new key's first delay would be.

**Synchronous retries.** `call(fn, key=None, retry_on=(RetryableError,))` runs
`fn` until it returns and returns its result. After each failure in `retry_on` it
sleeps `next_delay(key)` seconds on the policy's clock; any other exception propagates
immediately. When the budget is used up it raises `RetriesExhausted` (with `attempts`
equal to the number of runs and the last failure as `__cause__`). The key's state is
reset when `call` returns or raises.

### Delay scheduler

**Release time.** An item scheduled with `delay` seconds at monotonic time `t` is
due at `t + delay` (a negative delay counts as zero). It is never delivered before it
is due. Once it is due, a running scheduler delivers it promptly: within `max_wait`
(when set) plus the time the sink takes for items released before it — and this holds
even when the item was scheduled, or rescheduled, while the thread was asleep waiting
for a later item.

**Order.** Items are delivered in order of due time. Items with equal due times
are delivered in the order in which they were scheduled, where `reschedule` counts as
scheduling the item anew (it moves to the back of the items that share its new due
time). A rescheduled item is delivered once, at the due time of its latest
`schedule`/`reschedule`, however many times it was moved and wherever it was moved.

**Cancel and reschedule are truthful.** `cancel(key)` returns `True` exactly when
the key had a pending item that will now never be delivered. Once the scheduler has
begun delivering an item (its sink call may still be running), that item is no longer
pending: `cancel` and `reschedule` for it return `False` and have no effect, and the
item is delivered exactly once. A new item may be scheduled under the same key at any
time after its previous item stopped being pending.

**Stop.** `stop()` returns only after the release thread has finished, including
every delivery it had begun; nothing is delivered after `stop()` returns, and
`schedule`/`reschedule` raise `RuntimeError` from the moment `stop()` is called.
Every item ever scheduled ends up in exactly one of three places: delivered to the
sink, cancelled (a `cancel` returned `True`), or in the list `stop()` returns
(`(key, payload)` pairs in due order). While `stop()` is waiting for the thread,
`state` is `"stopping"`; afterwards it is `"stopped"`.

### Dead-letter store

**Arbitrary item ids.** Item ids are arbitrary non-empty strings of up to 200
characters drawn from any Unicode characters except NUL and lone surrogates; they may
contain `/`, `.`, `%`, spaces and non-ASCII letters, may be `.` or `..`, and may look
like anything else the store itself produces. Distinct ids are distinct items: a `put`
for one id never replaces, hides or exposes another id's record, and `list`, `get`,
`replay` and `purge` see exactly the records put under the ids they are given.

**Concurrent replay.** Any number of `replay` calls for the same id may run at
once, from threads of one process or from several processes, each with its own
`DeadLetterStore` instance on the same directory. For one parked record, exactly one of
them hands it to its target and returns `True`; the others return `False` and call no
target. `purge` concurrent with `replay` likewise either removes the record or loses to
the replay, never both.

**Replay hand-off.** The target receives the parked record with `attempts` reset
to `0` and every other field unchanged. When `replay` returns `True` the record is no
longer parked. If the target raises, the record stays parked and the exception
propagates. An item may be dead-lettered again while its replay is still running (for
instance, the target re-queues it and it fails again at once): that new record is
parked afterwards, whatever the order in which the replay and the new `put` finish.
`Retrier.replay` also resets the policy state of the item before the target sees it.

**Crash safety.** The process may die at any instant: between any two `FsOps`
calls, during one, or while a replay target is running. A file being written by
`write_bytes` may be left holding any prefix of its data. After a restart (a new
`DeadLetterStore` on the same directory, opened before any new replay starts):
every record whose `put` returned is still parked unless it was purged, replaced by a
later `put`, or handed to a replay target by a `replay` that returned `True`; a record whose replay was interrupted is
parked again (its target may or may not have seen it — targets are idempotent per
item id); a `put` interrupted by the crash leaves either the previous record for that
id or the new one, complete; and `list` never returns a torn record.

## Component contract: metrics and SLO alerting (`src/metrics`)

`tally` is the in-process metrics layer of the fanout service. The dispatcher records
delivery latency into histograms, counts attempts and outcomes per subscriber in a
labelled `Registry`, and feeds every delivery outcome into a `BurnRateAlert` that pages
when the delivery SLO is being burned too fast. Dashboards and the alert read the
numbers back; nothing here formats or exports text.

Modules:

- `src/metrics/histogram.py` — `Histogram(bounds)`: fixed buckets, count, exact sum, min/max,
  `snapshot()`, `quantile(q)`, `merge(other)`; `HistogramSnapshot` holds the same figures
  immutably and computes `quantile(q)` and `mean()`.
- `src/metrics/window.py` — `RollingCounter(slots, slot_seconds, clock, width)`: a rolling
  window of integer counters (`add(*amounts)`, `totals()`, `total()`, `per_slot()`).
- `src/metrics/registry.py` — `Registry` with `counter(name, labels)`, `gauge(name, labels)`,
  `histogram(name, bounds, labels)` and `snapshot()`; a `Snapshot` answers
  `value(name, labels)` and `series(name)`.
- `src/metrics/slo.py` — `BurnRateAlert(objective, fire_at=, clear_at=, long_slots=,
  short_slots=, slot_seconds=, min_events=, clock=, on_transition=)` with
  `record(good, bad)`, `record_outcome(ok)`, `evaluate()`, `burn_rates()`, `firing`,
  `transitions()`.

### Architecture constraints

- All time-dependent behaviour goes through a clock object (`src/clock.py`); no other
  module imports `time`. Windows and alerts use the clock's `monotonic()` reading and
  take the clock they are given.
- Every public operation may be called from any number of threads at once.
- `Registry` creates counter series by calling the module-level class
  `src.metrics.registry.Counter(name, labels)`, and `snapshot()` reads a counter through its
  `sample()` method. This is a seam like the clock: the behaviour below is verified by
  substituting an instrumented `Counter`. Keep it.
- Observed values are arbitrary finite floats (negative values included; they come
  from gauges, deltas and clock offsets as well as latencies). Non-finite values and
  non-finite or non-increasing bucket bounds are rejected with `ValueError`.

### Guarantees

**Bucket layout.** A histogram with bounds `b0 < b1 < ... < bn-1` has `n + 1`
buckets. Bucket `i < n` holds the values `v` with `b(i-1) < v <= bi` (bucket 0 has no
lower limit): a value exactly equal to a bound is counted in the bucket that bound
closes. The last bucket holds every value greater than `bn-1`.

**Exact sum.** `sum()` (and a snapshot's `total`) is the exact mathematical sum of
every value observed, rounded once to the nearest float (the value `math.fsum` gives for
the same values). It does not depend on the order or the number of observations and
does not drift, however many values are recorded and whatever their magnitudes or
signs.

**Quantile estimate.** `quantile(q)` is defined for `0 <= q <= 1` (otherwise
`ValueError`) and is `None` for an empty histogram. `quantile(0)` is the exact smallest
and `quantile(1)` the exact largest observed value. For other `q`, let `rank = q *
count`. The estimate comes from the first bucket (lowest index) whose cumulative count —
its own count plus the counts of all buckets below it — is at least `rank`. That bucket's
interpolation interval runs from its lower edge to its upper edge, where the edges are the
neighbouring bounds narrowed to the observed range: `lo = max(lower bound, min)` and
`hi = min(upper bound, max)` (the first bucket's lower bound and the last bucket's upper
bound are unbounded). The estimate is `lo + (hi - lo) * (rank - before) / in_bucket`,
where `before` is the cumulative count of the buckets below and `in_bucket` the bucket's
own count. In particular a histogram holding a single value returns that value for every
`q`.

**Merge.** `a.merge(b)` (with `b` a histogram or a snapshot with identical bounds)
leaves `a` exactly as if every value observed by `b` had also been observed by `a`:
bucket counts, count, minimum, maximum and the exact sum of “Exact sum” over the union of the
values. `b` is unchanged. Merging histograms with different bounds raises `ValueError`
and changes nothing. A histogram may be merged into itself, and two histograms may be
merged into each other concurrently, without deadlock.

**Consistent histogram reads.** A snapshot of a histogram describes one instant:
its `count` equals the sum of its bucket counts, and its count, sum, minimum and
maximum all cover the same set of observations, even while other threads observe.

**Slot boundaries.** A `RollingCounter` with `slots = N` and `slot_seconds = w`
created when the monotonic clock read `t0` divides time into slots: slot `k` covers
`t0 + k*w <= t < t0 + (k+1)*w`. These boundaries are fixed for the life of the counter
and never shift, however the counter is used. An amount added at time `t` belongs to the
slot containing `t`. The window at time `t` consists of the slot containing `t` and the
`N - 1` slots before it; an amount added in slot `k` is therefore included in the totals
for every `t` with `t0 + k*w <= t < t0 + (k+N)*w` and not at or after `t0 + (k+N)*w`.

**Gaps and jumps.** The counter's totals depend only on the amounts added in the
slots currently inside the window, whatever happened before. This holds after the
counter has been idle for any length of time and when the clock moves forward by any
amount between two calls (several windows at once, or an exact multiple of the window):
amounts from slots that have left the window never reappear, and every slot entered
after a gap starts empty.

**Series identity.** A series is identified by its metric name and the *set* of
its label pairs. Label names are identifiers (`[A-Za-z_][A-Za-z0-9_]*`); label values
are arbitrary strings. Requests for the same name and the same set of pairs return the
same series regardless of the order in which the labels are given; requests that differ
in any label name or value return different series, and a snapshot reports them
separately. A name used for one kind of metric (or, for histograms, one set of bounds)
cannot be reused for another (`ValueError`).

**Concurrent first use.** Any number of threads may request the same series for
the first time at once: exactly one series is created, every caller gets that series,
and no update made through any of them is lost. Counters and gauges are exact under
concurrent updates.

**Consistent snapshot.** `Registry.snapshot()` returns the values of all series
as of a single instant, even while other threads update them. If one update to a series
happened before another update (to the same or a different series), a snapshot that
reflects the later update also reflects the earlier one. For example, when every
delivery increments `attempts` before it increments `delivered`, no snapshot shows more
deliveries than attempts.

**Exact thresholds.** The error budget is `1 - objective/100`. A window's burn
rate is `(failed events / total events) / budget`, computed exactly. Objectives and
thresholds are integers, decimal strings or `Fraction`s (floats are rejected with
`TypeError`), and comparisons against thresholds are exact: a window burning at
exactly `fire_at` is at the threshold, not below it, and likewise for `clear_at`.

**Hysteresis.** A window with fewer than `min_events` events in it gives no
verdict. While not firing, `evaluate()` fires the alert when both windows give a verdict
and both burn rates are `>= fire_at`. While firing, it clears the alert when the short
window gives a verdict and its burn rate is `< clear_at`; anything else (including a
short window burning exactly at `clear_at`, or a window without a verdict because traffic
stopped) leaves the alert firing. Nothing but these rules changes the state, so an alert
whose burn rate stays within `[clear_at, fire_at)` never changes state.

**Transitions.** Each state change is recorded exactly once in `transitions()`
(fire and clear strictly alternate, starting with fire) and passed exactly once to
`on_transition`, in the order the changes happened, never concurrently, even when
several threads evaluate the alert at once.

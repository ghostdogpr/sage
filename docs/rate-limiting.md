# Rate limiting

A **rate limiter** caps how often something may happen: so many requests per second for an API key, a budget of login attempts per account, a fair-use quota per tenant. Sage ships one built in, so every client has it with no extra dependency.

The limiter is **distributed**. Its state lives on the server, not in process memory, so the limit holds across every process pointed at the same server (they share the keyspace, not a connection). Each check runs its whole decide-and-consume cycle atomically on the server; in steady state that is a single round trip. The script is cached on the server, so only a cold server (or one after `SCRIPT FLUSH`) costs a second round trip to resend the script body. In a cluster that resend goes to the node owning the subject's key, like the check itself.

`rateLimiter` binds a policy to the client. Each `tryAcquire` consumes tokens for a subject and returns a `Decision`.

::: code-group

```scala [Ox]
val limiter  = client.rateLimiter[String](RateLimit.perSecond(100))
val decision = limiter.tryAcquire("user:42")
val allowed  = decision.isAllowed
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
val limiter = client.rateLimiter[String](RateLimit.perSecond(100))
for {
  decision <- limiter.tryAcquire("user:42")
} yield decision.isAllowed
```

:::

## The algorithm: token bucket

Each subject has a **bucket** that holds up to `capacity` tokens and refills continuously over time. A `tryAcquire` takes `cost` tokens (one by default) when the bucket holds them, and is denied otherwise. `capacity` is the burst ceiling: a subject that has been idle can spend up to a full bucket at once, then is paced by the refill rate.

Refill is smooth rather than stepped, so a caller regains its allowance gradually instead of all at once at a window boundary.

Build a policy with the constructors:

```scala
RateLimit.perSecond(100)                              // 100 per second, bursting to 100
RateLimit.perMinute(5000)                             // 5000 per minute, bursting to 5000
RateLimit(permits = 100, per = 1.second, burst = 200) // 100/s sustained, bursting to 200
```

## Reading the decision

`Decision` is an ordinary returned value, never a thrown error: an admitted or rejected request is a normal outcome, not a failure.

- `isAllowed`: whether the request was admitted.
- `remainingTokens`: tokens left in the bucket. A denial consumes nothing, so it reports the untouched balance. Convenient for an `X-RateLimit-Remaining` header.
- `Allowed(remaining, resetAfter)`: `resetAfter` is the time until the bucket refills to full.
- `Denied(remaining, retryAfter)`: `retryAfter` is the time until enough tokens are available. Convenient for a `Retry-After` header.

There is no blocking `acquire`: `tryAcquire` never waits. To retry, sleep for `retryAfter` at the call site and try again.

Waits are reported at microsecond resolution, saturating at `RateLimiter.maximumReportedWait` if a server-clock rollback ever pushes one past what a `FiniteDuration` holds.

## Peek and reset

- `peek(subject)` reports the current standing without consuming: `Allowed` while at least one token is available, otherwise `Denied` with the wait until one is. The bucket is still refilled by elapsed time, but no tokens are taken.
- `reset(subject)` clears a subject's bucket, so its next request starts from full capacity.

## Cost and subjects

Pass `cost` to charge a heavier request more than one token:

```scala
limiter.tryAcquire("user:42", cost = 10)
```

A subject can be any type with a `KeyCodec`. It is encoded and prefixed with a namespace (`ratelimit` by default) to form the bucket's key. Give a custom namespace when more than one limiter runs against the same server:

```scala
client.rateLimiter[String](RateLimit.perSecond(100), namespace = "login")
```

Give each policy that is active at the same time its own namespace. A namespace that does see its policy change, during a rolling deployment say, carries its buckets over safely: a bucket records the policy that created it, and on a change each subject keeps the lesser of its whole tokens and the new capacity, drops the fraction, and refills under the new settings. That stops overlapping old and new instances from handing out full buckets repeatedly. Idle full buckets expire quickly, so a subject first seen after the switch starts at the new capacity.

## Valid policies

A policy and a `cost` must satisfy:

- `capacity` greater than 0, `refillTokens` greater than 0, `refillPeriod` at least one microsecond.
- `capacity`, `refillTokens`, and `refillPeriod` in microseconds each within `2^53` (the range a server-side Lua number represents exactly).
- `capacity` multiplied by `refillPeriod` in microseconds within `2^53`, so the refill arithmetic stays exact on the server.
- `cost` in the range `1` to `capacity`.

Violations are a programming error, not a runtime outcome. On the `rateLimiter` factory path (`tryAcquire`, `peek`) a bad policy or cost fails with a typed `SageException.InvalidArgument` through the effect before any server call. On the composable `command` path the server rejects the call with an error and never modifies the bucket.

## When the store is unreachable

A check reaches the server, so it can fail when the server is unreachable. The failure surfaces through the effect `F` for the caller to handle, exactly like any other command. Sage applies no fallback of its own: decide at the call site whether an outage should admit the request (availability first) or reject it (protection first).

## Capacity planning

Each check is one cached-script request and one constant-time atomic operation. It rewrites a small hash and refreshes its expiry, so it consumes write throughput even on a denial, and the server runs each script serially however many the client multiplexes.

One key exists per subject whose bucket is not full, expiring when that bucket would refill completely. Active keys therefore track the distinct subjects seen within one refill window: long windows over high-cardinality subjects retain the most state.

For a limiter on every application call: count its operations, memory, replication, and persistence traffic in the store's capacity test; keep denied callers from retrying in a tight loop; and give it a dedicated deployment or more cluster shards if it would take a material share of an existing store.

## The composable command

`tryAcquire` runs on the client directly. To pipeline the check or run it yourself instead, `command` returns the underlying `Command` to pass to `client.run`:

```scala
client.run(limiter.command("user:42"))
```

## Topology

The limiter works on every topology. A subject's whole state is one key, so it hashes to a single cluster slot with no hash-tag gymnastics and routes correctly on a standalone, master-replica, or cluster client with no change to your code.

# Rate limiting

A **rate limiter** caps how often something may happen: so many requests per second for an API key, a budget of login attempts per account, a fair-use quota per tenant. Sage ships one built in, so every client has it with no extra dependency.

The limiter is **distributed**. Its state lives on the server, not in process memory, so the limit holds across every process pointed at the same server (they share the keyspace, not a connection). Each check runs its whole decide-and-consume cycle atomically on the server; in steady state that is a single round trip. The script is cached on the server, so a check pays a one-time load-and-retry only when the server has not cached it yet (a cold server, or after `SCRIPT FLUSH`), not per connection.

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

Waits are reported at microsecond resolution. After an exceptionally large server-clock rollback, a wait that cannot fit in a `FiniteDuration` saturates at `RateLimiter.maximumReportedWait`; the bucket's server-side expiry still covers the complete wait.

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

While bucket state exists, it records the policy that created it. If a limiter keeps the same namespace but changes capacity or refill settings, each subject retains no more than its existing whole tokens or the new capacity, drops fractional credit, and begins refilling under the new policy. This conservative transition prevents overlapping old and new instances during a rolling deployment from repeatedly recreating full buckets. Full idle buckets expire quickly because retaining their state cannot affect decisions under the same policy; after expiry, a later policy sees a new subject and starts at its own capacity. Use distinct namespaces for independently active policies.

## Valid policies

A policy and a `cost` must satisfy:

- `capacity` greater than 0, `refillTokens` greater than 0, `refillPeriod` at least one microsecond.
- `capacity`, `refillTokens`, and `refillPeriod` in microseconds each within `2^53` (the range a server-side Lua number represents exactly).
- `capacity` multiplied by `refillPeriod` in microseconds within `2^53`, so the refill arithmetic stays exact on the server.
- `cost` in the range `1` to `capacity`.

Violations are a programming error, not a runtime outcome. On the `rateLimiter` factory path (`tryAcquire`, `peek`) a bad policy or cost fails with a typed `SageException.InvalidArgument` through the effect before any server call. On the composable `command` path the server rejects the call with an error and never modifies the bucket.

## When the store is unreachable

A check reaches the server, so it can fail when the server is unreachable. The failure surfaces through the effect `F` for the caller to handle, exactly like any other command. The library hides nothing: decide there whether an outage should admit the request (availability first) or reject it (protection first).

## Capacity planning

Every check is one cached-script request and one constant-time atomic operation on the server. It reads and rewrites one small hash and refreshes its expiry, so it consumes write throughput even when the decision is denied. Concurrent checks are automatically multiplexed by the client, but Redis or Valkey still executes each script serially and atomically.

One key exists for each subject whose bucket is not full. Its expiry is the remaining time until that bucket can refill completely; a full bucket expires almost immediately. Active-key cardinality therefore depends mainly on the number of distinct subjects seen within a full-refill window. Long refill windows and high-cardinality subjects retain more state than short windows over a stable subject set.

For a limiter on every application call, include its operations, memory, replication, and persistence traffic in the store's capacity test. Avoid retrying a denied decision in a tight loop, and use a dedicated deployment or enough cluster shards when limiter traffic would consume a material share of an application's existing store.

## The composable command

`tryAcquire` runs on the client directly. To pipeline the check or run it yourself instead, `command` returns the underlying `Command` to pass to `client.run`:

```scala
client.run(limiter.command("user:42"))
```

## Topology

The limiter works on every topology. A subject's whole state is one key, so it hashes to a single cluster slot with no hash-tag gymnastics and routes correctly on a standalone, master-replica, or cluster client with no change to your code.

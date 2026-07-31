# Rate limiting

A **rate limiter** caps how often something may happen: so many requests per second for an API key, a budget of login attempts per account, a fair-use quota per tenant. Sage ships one built in, so every client has it with no extra dependency.

The limiter is **distributed**. Its state lives on the server, not in process memory, so the limit holds across every process pointed at the same server. Each check decides and consumes atomically on the server, in a single round trip.

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

Give each policy that is active at the same time its own namespace. Changing the policy on an existing namespace is safe during a rolling deployment: a bucket remembers the policy that created it, and on a change each subject carries over the lesser of its current tokens and the new capacity, so overlapping old and new instances cannot hand out full buckets repeatedly.

## Invalid policies

A policy needs a positive `capacity` and `refillTokens` and a `refillPeriod` of at least a microsecond, and `cost` must be between `1` and `capacity`. Very large values are also rejected, since the server-side arithmetic has to stay exact.

These are programming errors, not runtime outcomes: `tryAcquire` and `peek` fail with `SageException.InvalidArgument` before any server call.

## When the store is unreachable

A check reaches the server, so it fails like any other command when the server is unreachable, through the effect `F`. Sage applies no fallback of its own: decide at the call site whether an outage should admit the request (availability first) or reject it (protection first).

## Capacity planning

Each check is one script call and one constant-time atomic operation. It writes even on a denial, so a limiter on every application call adds real write throughput. One key exists per subject whose bucket is not full, expiring once that bucket would be full again, so live keys track the distinct subjects seen within one refill window: long windows over many subjects hold the most state.

If the limiter would take a material share of an existing store, give it its own deployment or more cluster shards, and keep denied callers from retrying in a tight loop.

## The composable command

`tryAcquire` runs on the client directly. To pipeline the check or run it yourself instead, `command` returns the underlying `Command` to pass to `client.run`:

```scala
client.run(limiter.command("user:42"))
```

## Topology

The limiter works on every topology. A subject's whole state is one key, so it hashes to a single cluster slot with no hash-tag gymnastics and routes correctly on a standalone, master-replica, or cluster client with no change to your code.

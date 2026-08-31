# Rate limiting

A rate limiter controls how often an action may occur. You can limit API requests by key, login attempts by account, or usage by tenant. Every Sage client includes rate limiting.

The limiter stores its state on the server rather than in process memory. The limit therefore applies across every process that uses the same server. Each check decides whether to allow the request and consumes tokens atomically in one round trip.

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

## Token bucket algorithm

Each subject has a bucket that holds up to `capacity` tokens and refills continuously. `tryAcquire` takes `cost` tokens when the bucket has enough. The default cost is one. Otherwise, the request is denied. An idle subject can spend a full bucket at once. After that, the refill rate controls its request rate.

Refill is smooth rather than stepped, so a caller regains its allowance gradually instead of all at once at a window boundary.

Build a policy with the constructors:

```scala
RateLimit.perSecond(100)                              // 100 per second, bursting to 100
RateLimit.perMinute(5000)                             // 5000 per minute, bursting to 5000
RateLimit(permits = 100, per = 1.second, burst = 200) // 100/s sustained, bursting to 200
```

## Reading the decision

`Decision` is a return value, not an exception. Both allowed and denied requests are normal results.

- `isAllowed`: whether the request was admitted.
- `remainingTokens` is the number of tokens left in the bucket. A denial consumes nothing and reports the existing balance. You can use it for an `X-RateLimit-Remaining` header.
- `Allowed(remaining, resetAfter)` reports how long the bucket takes to refill.
- `Denied(remaining, retryAfter)` reports how long the caller must wait for enough tokens. You can use it for a `Retry-After` header.

`tryAcquire` returns immediately instead of waiting for capacity. To retry, sleep for `retryAfter` at the call site and try again.

## Peek and reset

- `peek(subject)` checks the current state without consuming a token. Elapsed time still refills the bucket. It returns `Allowed` while at least one token is available. Otherwise, it returns `Denied` with the time until a token becomes available.
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

Give each active policy its own namespace. You can change a policy during a rolling deployment without resetting its buckets. Each bucket records the policy that created it. When the policy changes, the bucket keeps the smaller of its current token count and the new capacity. Old and new application instances therefore cannot each issue a full bucket.

## Invalid policies

A policy requires positive `capacity` and `refillTokens` values. `refillPeriod` must be at least one microsecond, and `cost` must be between `1` and `capacity`. Sage also rejects values that exceed the server's exact arithmetic range.

These are programming errors, not runtime outcomes: `tryAcquire` and `peek` fail with `SageException.InvalidArgument` before any server call.

## When the store is unreachable

Each check contacts the server and fails through the effect `F` if the server is unreachable. Sage does not choose a fallback behavior. Your application must decide whether to allow or reject requests during an outage.

## Capacity planning

Each check makes one script call and performs one constant-time atomic operation. A denied check still writes to the server. Applying a limiter to every application call therefore adds write load. The limiter stores one key for each subject whose bucket is not full. That key expires when the bucket would become full again. Long refill windows with many subjects retain the most keys.

If the limiter would take a material share of an existing store, give it its own deployment or more cluster shards, and keep denied callers from retrying in a tight loop.

## The composable command

`tryAcquire` runs on the client directly. To pipeline the check or run it yourself instead, `command` returns the underlying `Command` to pass to `client.run`:

```scala
client.run(limiter.command("user:42"))
```

## Topology

The limiter works with every topology. Each subject's state uses one key and therefore one cluster slot. The same code works with standalone, master-replica, and cluster clients, without requiring hash tags.

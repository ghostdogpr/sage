# Consumer tailing stream: handler with auto-ack-after-success

The high-level consumer-group reader, `xConsume(group, consumer)(handle: (K, StreamEntry) => F[Unit]): F[Unit]`, loops `XREADGROUP … > BLOCK`, runs `handle` per entry, and issues `XACK` **only after `handle` completes successfully**. A failing handler leaves the entry in the Pending Entries List for later recovery; on startup the consumer first drains its own pending history (reading from `0-0` until empty) and only then tails `>`. It is a handler-driven consume that runs until its scope closes, not a bare `Stream` of entries, and it reuses the existing lease-per-poll blocking path (a bounded `BLOCK`, so scope closure cancels promptly) rather than any new connection machinery.

Only two clients ship a comparable abstraction, and they default oppositely. Spring Data Redis's default "auto-ack" is implemented as `XREADGROUP NOACK`: the entry never enters the PEL, so a handler that throws silently loses it — a documented footgun widely misread as "ack after processing." Go `redisqueue` acks only after the handler returns successfully (at-least-once) and is the model sage follows. Everyone else (Redisson, redis-py, StackExchange.Redis, redis4cats) offers only raw `XREADGROUP` plus manual `XACK`.

We chose auto-ack-after-success because it is both ergonomic — the caller never writes ack bookkeeping, a stated UX goal — and safe: it preserves the at-least-once guarantee that is the entire reason consumer groups exist. A bare `Stream[(entry, ackEffect)]` or manual-`xAck` API would be more flexible but pushes the bookkeeping back onto the user; NOACK-on-emit is rejected outright as lossy. Own-PEL recovery on startup is included because without it a consumer that crashes mid-processing strands its own in-flight entries, leaving a silent hole in the very guarantee the ack model provides, and it costs only an initial read from `0-0`. This is hard to reverse: the ack timing and recovery behavior are the consumer's delivery contract, and changing them later is a behavioral break for anyone relying on at-least-once.

Cross-consumer automatic reclaim — a background `XAUTOCLAIM` loop with a visibility timeout, picking up entries stranded by *other* dead consumers (redisqueue's standout feature) — is deliberately **out of scope** here. It carries opinionated policy (visibility timeout, reclaim cadence, fairness) that deserves its own design; users retain `xAutoClaim`/`xClaim`/`xPending` for manual cross-consumer recovery in the meantime.

## Considered Options

- **Handler + auto-ack-after-success + own-PEL recovery (chosen)** — ergonomic and at-least-once-safe; matches redisqueue and the community-idiomatic pattern.
- **Bare stream of `(entry, ackEffect)` or manual `xAck`** — maximally flexible, but acking is a manual step the UX goal aims to remove.
- **Auto-ack on emit via `NOACK` (Spring's default)** — simplest, but lossy: a failed handler drops the entry. Rejected.
- **Add cross-consumer auto-reclaim now** — maximal UX, but the heaviest, most opinionated piece; deferred to a deliberate later design.

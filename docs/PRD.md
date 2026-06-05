# PRD: Sage v1 — a modern Scala 3 client for Redis and Valkey

> Status: **ready-for-agent** (no issue tracker exists yet; this file stands in for the tracker issue)
> Governing docs: `CONTEXT.md` (glossary), `docs/adr/0001`–`0008`

## Problem Statement

Scala developers who need Redis or Valkey today must either wrap a Java client (lettuce, Jedis) — paying for impedance mismatch with their effect system, `CompletionStage` bridging, and Java-flavored APIs — or use an ecosystem-locked client (redis4cats for cats-effect, zio-redis for ZIO) that doesn't follow them across projects using different effect stacks. None of the existing options is Scala 3-first, none treats Valkey as a first-class target, and none brings the current client generation's features (RESP3, automatic pipelining, client-side caching) to Scala natively. Teams using Kyo or Ox have no native client at all.

## Solution

Sage: a Scala 3-only, dependency-light, native (no Java client wrapper) Redis/Valkey client, written once and published for four Backends — ZIO, cats-effect, Kyo, and Ox — with each artifact exposing that ecosystem's native types. It speaks RESP3 exclusively, auto-pipelines all commands over a Multiplexed Connection, models every command as a typed pure value (`Command[Out]`) with ergonomic method sugar, supports standalone and cluster topologies behind one client type, and ships TLS, transactions, pub/sub (including sharded), and invalidation-driven client-side caching in v1.

## User Stories

1. As a ZIO application developer, I want sage's API to return `ZIO` and `ZStream` values directly, so that I use it without any interop layer or wrapper types.
2. As a cats-effect application developer, I want the same API surface returning `IO` and fs2-compatible streams, so that sage feels native to the Typelevel stack.
3. As a Kyo application developer, I want the same API in Kyo's effect types, so that I have a first-class Redis client at all.
4. As an Ox/direct-style developer, I want blocking-style calls that compose with structured concurrency scopes, so that I can use Redis without adopting a monadic effect system.
5. As an application developer, I want to connect to a standalone server or a cluster by changing only configuration, so that my application code is topology-portable.
6. As an application developer, I want to connect to TLS-enabled managed services (ElastiCache, MemoryDB, Azure Cache, Upstash, Redis Cloud) with username/password ACL auth, so that sage works in my production environment from day one.
7. As an application developer, I want every command to have a precisely typed result (e.g. `GET` returning an optional value, `INCR` returning a number), so that I never pattern-match on raw protocol values.
8. As an application developer, I want command methods directly on the client for the whole command set, so that I can discover the API through IDE completion.
9. As an application developer, I want commands available as pure values I can compose and pass around, so that I can build abstractions on top of sage.
10. As an application developer, I want to compose several Commands into a Pipeline executed in one round-trip with a typed result per command, so that I get maximal throughput without manual batching.
11. As an application developer, I want to execute a Pipeline as an atomic Transaction, optionally guarded by `WATCH`, so that I can do optimistic-concurrency updates safely.
12. As an application developer, I want to encode and decode my own key and value types via Codec typeclasses resolved at the call site, so that heterogeneous data lives on one client without stringly-typed call sites.
13. As an application developer, I want built-in codecs for primitives (strings, numerics, booleans, raw bytes), so that common cases need zero setup.
14. As an application developer, I want decode failures surfaced as a specific error telling me what was expected and what arrived, so that codec bugs are diagnosable.
15. As an application developer, I want to subscribe to channels and patterns and receive messages as a stream in my ecosystem's native stream type, so that pub/sub composes with the rest of my application.
16. As an application developer, I want sharded pub/sub (`SSUBSCRIBE`) supported in cluster mode, so that pub/sub scales with my cluster.
17. As an application developer, I want my subscription to end (and `UNSUBSCRIBE` to be sent) when my stream's scope closes, so that resources never leak.
18. As an application developer, I want a slow pub/sub consumer in my code to never delay command responses elsewhere in the application, so that one misbehaving consumer can't degrade everything.
19. As an application developer, I want to opt a read into client-side caching with a TTL on a per-call basis, so that hot reads are served from local memory and invalidated automatically when the server data changes.
20. As an application developer, I want blocking commands (`BLPOP`, `XREAD BLOCK`, …) to work without stalling other concurrent commands, so that I can mix workloads on one client.
21. As an application developer, I want concurrent commands from many fibers automatically pipelined onto one connection, so that I get near-optimal throughput without tuning a pool.
22. As an application developer, I want the client to auto-reconnect with configurable backoff and restore subscriptions and tracking state, so that transient network failures self-heal.
23. As an application developer, I want commands in flight during a connection loss to fail with an error that tells me they may have executed, so that I can decide myself whether retrying is safe.
24. As an application developer, I want commands submitted while disconnected to fail fast rather than queue invisibly, so that latency is predictable and I own my retry policy with my ecosystem's combinators.
25. As an application developer, I want all failures to belong to one sealed exception hierarchy, so that I can handle classes of failure exhaustively.
26. As an application developer using a cluster, I want key-based commands routed to the right node automatically, including `MOVED`/`ASK` redirect handling and topology refresh, so that cluster mechanics never appear in my code.
27. As an application developer using a cluster, I want a Pipeline spanning multiple slots transparently split per node, run in parallel, and merged in order, so that batching works the same as on standalone.
28. As an application developer using a cluster, I want a cross-slot Transaction rejected with a clear error, so that I never get silent non-atomic behavior.
29. As an operator/SRE, I want a listener interface reporting command completions (duration, node, outcome), connection events, cache hits/misses, and topology changes, so that I can wire sage into my metrics and tracing without sage imposing a telemetry dependency.
30. As an operator/SRE, I want a connection watchdog that health-checks idle connections and kills/reconnects dead ones, so that stuck connections never hang the application indefinitely.
31. As an operator/SRE, I want connection, TLS, auth, backoff, and watchdog settings in one configuration entry point, so that production tuning is discoverable.
32. As a security-conscious operator, I want TLS certificate validation on by default with explicit opt-outs, so that the secure path is the default path.
33. As a Valkey user, I want sage to treat Valkey as a first-class server — tested against it and tracking its command set — so that I'm not betting on a Redis-only client.
34. As a library evaluator, I want to know exactly which commands are implemented and which are pending, so that I can judge coverage honestly before adopting.
35. As a contributor, I want commands organized by family with a repeating, reviewable pattern, so that adding a missing command is a small, mechanical PR.
36. As a contributor, I want the protocol, command, codec, and cluster-routing logic to be pure and testable without any server or socket, so that most development needs no infrastructure.
37. As a maintainer, I want a coverage test diffing implemented commands against the server specs with an allowlist, so that new server releases surface as failing-test TODOs instead of silent gaps.
38. As a user on an exotic platform, I want the pure Core published for Scala.js and Scala Native, so that I can build my own runtime on top of it where the JVM runtime doesn't reach.

## Implementation Decisions

All decisions below were settled during design (see ADRs); the PRD restates them as build constraints.

### sbt module structure

- **`sage-core`** — pure, zero external dependencies, cross-published JVM/JS/Native, Scala 3.3 LTS. Contains four components:
  - **RESP3 codec**: incremental parser (bytes in → protocol frames out, tolerating partial input across feeds) and frame writer. RESP3 only (ADR-0002); push frames are first-class.
  - **Command model**: `Command[Out]` pure values pairing argument encoding with a typed reply decoder; hand-written command families, no code generation (ADR-0005).
  - **Codecs & Bytes**: an opaque immutable `Bytes` type over `IArray[Byte]` with explicit content equality (ADR-0004); separate key and value codec typeclasses (keys must hash to cluster slots); primitive instances only — sage is not a serialization framework.
  - **Cluster slot engine**: pure cluster logic — CRC16 slot hashing with hash-tag extraction, redirect-reply parsing, routing decisions `(topology, command) → node`, and Pipeline split/merge planning. No I/O.
- **`sage-client`** — the Runtime, written once against kyo-compat and cross-published per Backend (`sage-zio`, `sage-ce`, `sage-kyo`, `sage-ox`) via the kyo-compat sbt plugin (ADR-0001). JVM 21+ (ADR-0008). Contains seven components:
  - **Socket layer** (internal component, not a separate sbt module): blocking `Socket`/`SSLSocket` on virtual threads, two per connection (reader feeding the parser, writer draining a send queue), bridged once into the compat effect type; TLS from the JDK; idle-`PING` watchdog (ADR-0008).
  - **Multiplexer**: FIFO reply matching and auto-pipelining over the Multiplexed Connection; reconnect lifecycle with configurable backoff; in-flight commands fail as possibly-executed, commands while disconnected fail fast; no offline queue, no automatic retry (ADR-0003, ADR-0006). Designed as a state machine over the socket layer's interface so logic is testable without sockets.
  - **Dedicated pool**: on-demand acquisition of Dedicated Connections for `WATCH`/`MULTI`/`EXEC` and blocking commands, transparent to the caller.
  - **Pub/sub dispatcher**: all subscriptions share one lazily-created Subscription Connection per client, isolating consumer backpressure from command traffic; auto-resubscribe on reconnect; subscriptions are streams whose closure unsubscribes.
  - **Client-side cache**: explicit per-call opt-in (`cached(command, ttl)`); local cache evicted by RESP3 invalidation pushes (which ride the Multiplexed Connection) and TTL.
  - **Cluster runtime**: topology discovery and refresh, redirect execution, per-node Multiplexer management; executes the slot engine's pure plans (ADR-0007).
  - **Public API**: a single client type for both topologies (configuration decides, ADR-0007); `run`/`pipeline`/`transact`/`subscribe`/`cached` primitives plus per-command method sugar; Pipelines compose applicatively into typed tuples; sealed exception hierarchy (server error, connection lost with may-have-executed flag, not connected, decode error, cross-slot error, timed out); listener SPI for observability with zero telemetry dependencies; no default per-command timeout — the watchdog owns liveness, users own deadlines.
- **`integration-tests`** — testcontainers matrix and the command-coverage spec.

### Cross-cutting decisions

- Protocol handshake via `HELLO 3` with optional `AUTH user pass`; servers below RESP3 are rejected (ADR-0002).
- Redis and Valkey are co-equal targets; command metadata tracks per-server availability as the specs diverge.
- The server command specs (`commands.json`) are a test fixture, never a codegen source: a coverage spec diffs implemented commands against them with an allowlist for deliberate skips (ADR-0005).
- Group coordinates `com.github.ghostdogpr`, Apache-2.0, sbt build, Scala 3.3 LTS, JDK 21 floor.

## Testing Decisions

A good test exercises external behavior through a component's public interface — frames in/values out, commands in/bytes out — never internal state or call sequences. The pure Core makes this cheap: most of sage's correctness surface needs no server, no sockets, no effect system.

Selected test scope (per design review):

- **All pure Core components** — RESP3 codec (golden wire frames, property tests over arbitrary frame trees and arbitrary chunk-boundary splits of the byte stream), command model (argument encoding goldens, reply decoders fed synthetic frames), codec round-trips, slot engine (known CRC16 vectors, hash-tag cases, routing and split-plan decisions against synthetic topologies).
- **Multiplexer and client-side cache state machines** — driven through a fake connection: FIFO matching under concurrency, fail-in-flight on connection loss, fail-fast while disconnected, reconnect/resubscribe sequencing; cache hit/miss/TTL/invalidation-eviction behavior fed synthetic invalidation events.
- **Integration suite** — testcontainers matrix {Redis 6/7/8, Valkey} × {standalone, cluster}, run against one Backend only (the Runtime is shared; per-Backend repetition adds little), covering commands-against-real-server behavior, transactions, pub/sub, sharded pub/sub, cluster redirects mid-test, TLS, and auth.
- **Per-Backend smoke tests** — thin suites per published artifact proving the lowered API compiles, connects, and runs a representative slice.

Prior art: none in-repo (greenfield); the golden-frame and fake-connection patterns follow the conventions of comparable protocol libraries.

## Out of Scope

- **Sentinel** topology (address-discovery strategy; can be added later without architectural impact).
- **Module command families** (JSON, Search, Bloom — diverging between Redis and Valkey; future add-on modules).
- **RESP2** and servers older than Redis 6.0, including RESP2-only proxies (ADR-0002).
- **Safe automatic retry** of provably-unsent/idempotent commands (possible v2 refinement over ADR-0006).
- **Connection striping** across N multiplexed connections per node (later tuning knob, ADR-0003).
- **Scala.js / Scala Native runtime** (Core cross-publishes from day one; the Runtime is JVM-first).
- **Serialization integrations** (circe, zio-schema, …) and **telemetry integrations** (OpenTelemetry) — future modules atop the codec typeclasses and listener SPI.
- **Upstreaming the socket layer to kyo-compat** — optional, post-production-proof.
- An offline command queue or sage-owned retry policies (rejected, ADR-0006).

## Further Notes

- kyo-compat fixes the error channel to `Throwable` and provides no per-Backend environment typing; ZIO/Kyo users can refine the sealed hierarchy into typed errors on their side. If kyo-compat proves insufficient, the sans-IO Core means only `sage-client` is at risk (ADR-0001).
- Platform reach of the published Backends is bounded by kyo-compat: Ox is JVM-only by nature.
- The name collision with SageMath/Sage accounting was considered and accepted.
- v1 ships when core command families are complete; the coverage spec tracks the long tail honestly rather than gating the release.

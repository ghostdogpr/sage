# Sage

A modern, Scala 3-only, native (no Java library wrapper) client for Redis and Valkey, usable from multiple effect ecosystems.

## Language

**Backend**:
An effect ecosystem that sage is published for: ZIO, cats-effect, Kyo, or Ox. Each backend is a separate artifact of the same runtime, with effect types lowered to the ecosystem's native types.
_Avoid_: runtime (reserved for sage's connection-management layer), integration, flavor

**Core**:
The pure, effect-free protocol layer: RESP encoding/decoding, the command model, and codecs. Contains no I/O.

**Runtime**:
The effectful layer written once against kyo-compat: sockets, connection lifecycle, pipelining, pub/sub dispatch, cluster handling.
_Avoid_: backend (see above), engine

**Bytes**:
The Core's opaque, immutable byte container, used at every protocol and codec boundary. Content equality is explicit (`sameBytes`) — universal `==` on Bytes is unreliable by design.
_Avoid_: byte string, blob, buffer

**Frame**:
A single RESP3 protocol value as read from the wire — the unit the Core's parser produces and reply decoders consume. The Frame model enumerates all RESP3 types, even those no command uses yet.
_Avoid_: message, packet, token

**Client**:
The user-facing handle (`SageClient`) owning all connections to one server or cluster. Constructed with `connect`, released with `close`; each Backend also offers a scope-tied construction form (`scoped`, plus `layer` for ZIO and `resource` for cats-effect) whose release swallows a failing `close`. Per-command methods are concrete sugar that must delegate to `run`, so a fake implementing `run` gets the whole command surface.
_Avoid_: connection (a Client holds several)

**Multiplexed Connection**:
The single auto-pipelined connection per node that carries ordinary commands from all fibers, replies matched in FIFO order.
_Avoid_: shared connection, main connection

**Auto-pipelining**:
The Multiplexed Connection's transparent behavior: commands from concurrent fibers are written without waiting for prior replies, coalesced into fewer socket writes. Invisible to users — a Pipeline, by contrast, is a value the user builds.
_Avoid_: batching, implicit pipelining

**Dedicated Connection**:
A connection exclusively held for commands with per-connection state or blocking behavior (`WATCH`/`MULTI`/`EXEC`, `BLPOP`, …), acquired transparently from an on-demand pool.
_Avoid_: exclusive connection, pooled connection

**Subscription Connection**:
The lazily-created connection carrying classic (channel and pattern) pub/sub subscriptions and their push frames, isolating slow consumers from the Multiplexed Connection. One per client against a standalone server; in a cluster, one per client pinned to an arbitrary Node — classic `PUBLISH` broadcasts across the whole cluster bus, so any Node sees every channel — and moved to another Node on failover. Created on the first subscription and closed when the last one ends; it re-issues every active subscription on reconnect. A slow consumer backpressures this connection at the TCP level (lossless), which can stall its peer subscriptions but never the Multiplexed Connection's commands.
_Avoid_: pub/sub connection

**Sharded Subscription Connection**:
A connection carrying the Shard Channel subscriptions for the channels a single Node owns. A client holds one per owning Node, since a Shard Channel's `SSUBSCRIBE` must reach the Slot's owner; each is created on demand and released when its last subscription ends. On Slot migration or failover the affected subscriptions are re-homed to the new owner. Distinct from the Subscription Connection, which carries classic subscriptions; the two never share a connection.
_Avoid_: per-node pub/sub connection, shard connection

**Shard Channel**:
A pub/sub channel whose name hashes to a Slot (its Hash Tag, if present), so `SSUBSCRIBE`/`SPUBLISH` target that Slot's owning Node and messages stay within the Shard — unlike a classic channel, whose `PUBLISH` broadcasts across the whole cluster bus. There is no pattern form; a Shard Channel delivery surfaces to the subscriber as an ordinary Message (channel and payload).
_Avoid_: sharded topic, shard topic

**Message**:
One pub/sub delivery: a channel and a payload, surfaced on a subscription's effect stream. A pattern subscription yields a **Pattern Message** instead, which also names the glob pattern that matched. The delivery unit of classic pub/sub — distinct from a Frame (the wire value carrying it) and a Stream Entry (a record in a Redis Stream).
_Avoid_: event, notification; calling a Frame or a Stream Entry a "message"

**Watchdog**:
The per-connection health check that probes an otherwise-idle Multiplexed Connection and declares it dead — to be reconnected — when replies stop arriving, catching half-open connections a blocking socket read never surfaces.
_Avoid_: heartbeat, health checker, keepalive

**Poisoning**:
Marking a connection (or the parser) unusable so it is discarded and reconnected rather than reused, because continuing would be unsafe: after a protocol error (RESP3 has no resync point) or a `READONLY` reply from a demoted master.
_Avoid_: invalidate

**Generation**:
The monotonic epoch of the Multiplexed Connection's current socket, bumped each time a fresh socket becomes live. A Dedicated Connection is stamped with the Generation that was live when it joined the pool and discarded once that Generation is no longer current — so a connection that outlived a reconnect (e.g. a DNS failover to a new master) is never reused. The Multiplexed Connection is the sole authority on its Generation: it answers `liveGeneration` (the epoch to stamp, only while live) and `isCurrent` (whether a stamp is still the live epoch); no caller compares raw epoch numbers.
_Avoid_: version, era, incarnation

**Cached Read**:
A read command executed with explicit per-call opt-in to client-side caching: served from the local cache until a server invalidation push or TTL evicts it.
_Avoid_: tracked read, local read

**Command**:
A pure value in the Core describing one server command: its wire encoding and its typed reply decoder (`Command[Out]`). The single source of truth; the client's per-command methods are sugar over it.
_Avoid_: request, operation

**Commands**:
The public builder facade (`Commands.get`, `Commands.incr`, …) yielding `Command` values, named one-for-one with the client's methods. The path for `run`, pipelines, transactions, and reuse; the per-family objects behind it are internal.
_Avoid_: Strings/Keys/Sets/… (internal families), Cmd

**Pipeline**:
An applicative composition of Commands sent in one round-trip, yielding a typed tuple of results. Not a transaction — no atomicity.

**Transaction**:
A Pipeline executed atomically via `MULTI`/`EXEC` on a Dedicated Connection, optionally guarded by `WATCH`. A watched key changing before `EXEC` aborts it — a normal optimistic-concurrency outcome the caller retries, not a failure. A queueing-phase rejection discards the whole transaction (nothing runs); an execution-phase error leaves the other commands committed (Redis does not roll back), surfaced per-position like a Pipeline.
_Avoid_: batch (that's a Pipeline)

**Transaction Scope**:
The handle opened by `transaction { tx => … }`, holding one leased Dedicated Connection for the block. Within it the caller may `watch`, run reads via the shared command surface (`tx.get`, `tx.run`, …), then `exec` a Pipeline (or `discard`) — enough to read, decide, and commit on the one connection that `WATCH` requires. Reads must be ordinary commands; a blocking command is rejected rather than parking the lease. The lease is always released; a scope abandoned with watches still armed discards its connection rather than recycling it.
_Avoid_: transaction context, session

**Family**:
A group of Commands mirroring one of the server's documented command groups (strings, keys, hashes, …): one Core object of command builders plus matching Client sugar, built and reviewed as a unit.
_Avoid_: command group, category, module

**Coverage Spec**:
The test diffing sage's implemented commands against the command list each supported server reports live, partitioned exactly into implemented, skipped (deliberate, with reasons), and todo (acknowledged gaps). Gaps are reported, never gating; unacknowledged drift fails.
_Avoid_: coverage report, gap test

**Codec**:
A typeclass converting one user type to/from its wire bytes at a command boundary. A boundary converter, not a serialization framework; keys and values have separate codec typeclasses (keys must also be hashable to cluster slots). Built-in codecs decode strictly: bytes that are not the type's canonical wire form fail with a decode error rather than being coerced. The RESP3 parser/writer is not a Codec — that layer converts wire bytes to/from Frames, not user types.
_Avoid_: serializer, schema

**Slot**:
One of the 16384 hash slots a cluster partitions the keyspace into. A key's slot is fixed by CRC16 of the key bytes (or of its hash tag, if present); routing, redirects, topology ownership, and Pipeline splitting are all expressed in terms of slots.
_Avoid_: bucket, partition, shard (a Shard owns slots — they are not the same)

**Hash Tag**:
The `{…}` substring of a key whose bytes alone are hashed, so that related keys (`{user:1}:name`, `{user:1}:age`) deliberately land in the same slot. Only the bytes between the first `{` and the first `}` after it are used, and only when that span is non-empty; otherwise the whole key hashes.
_Avoid_: key tag, slot tag

**Node**:
One server process in a cluster, addressed by host and port. A Node is a master or a replica within a Shard. The user-facing handle is still the Client; a Node is an internal routing target the engine names, not a connection.
_Avoid_: server, instance, connection

**Shard**:
A cluster's unit of replication and slot ownership: one master Node, its replica Nodes, and the slot ranges it owns. The Cluster Topology is a set of Shards.
_Avoid_: node group, partition

**Cluster Topology**:
The Core's pure snapshot of which Shards own which slots, the input every routing decision reads. Produced by the runtime from `CLUSTER SHARDS`/`SLOTS` and refreshed on redirects; the engine only consumes it.
_Avoid_: cluster map, slot map, layout

**Seed**:
A user-supplied address the cluster runtime contacts at startup to discover the Cluster Topology. Seeds bootstrap discovery only — once the topology is known, routing targets are Nodes the cluster reports, and a seed that is not itself a master is dropped. Any one seed answering is enough.
_Avoid_: bootstrap node, contact point, seed node

**Redirect**:
A server reply telling the client a slot lives elsewhere. `MOVED` is permanent — the slot's owner changed, refresh the topology — while `ASK` is a one-shot hand-off during a live migration: send the single command (prefixed with `ASKING`) to the named node without touching the topology. The Core parses both into one value; acting on the difference is the runtime's job.
_Avoid_: move, redirection, MOVED (the umbrella term covers both kinds)

**Stream**:
A Redis/Valkey stream key: an append-only log of entries, each carrying a Stream Entry ID and an ordered list of field/value pairs. Read by range or by tailing, and consumed cooperatively through Consumer Groups. Always capitalized to distinguish it from an effect stream (`ZStream`/`CStream`), which is the unrelated pagination type the client's `…All` helpers return.
_Avoid_: log, queue, topic; "stream" lowercase (that's the effect type)

**Stream Entry**:
One record appended to a Stream (`StreamEntry`): a Stream Entry ID plus an ordered, duplicate-permitting list of field/value pairs. The body is a `Vector`, not a `Map` — a Stream preserves field order and allows repeated field names, which a `Map` would silently drop. Fields are identifiers (a `KeyCodec`, like a Hash field); values take a `ValueCodec`.
_Avoid_: message, record, event, item

**Stream Entry ID**:
The `ms-seq` identifier ordering entries within a Stream: a millisecond timestamp and a per-millisecond sequence. A concrete `StreamId(ms, seq)` for replies and explicit writes; each command position that also admits a special token — `*` auto-id, `-`/`+` range extremes, `(` exclusive bound, `$` last-id, `>` new-for-group — carries its own type listing only the tokens legal there, so an illegal form at a position cannot be written.
_Avoid_: offset, sequence number, timestamp, cursor

**Consumer Group**:
A named, server-tracked cooperative reader over a Stream, created by `XGROUP CREATE`: it holds a last-delivered ID and a Pending Entries List, so several Consumers can split a Stream's entries between them without overlap. Reading `>` (new-for-group) advances the group and records delivery; reading an explicit ID re-reads a Consumer's own pending history.
_Avoid_: group, subscriber group, consumer pool

**Consumer**:
A named member of a Consumer Group. Every delivered-but-unacknowledged entry is owned by exactly one Consumer until acknowledged (`XACK`) or transferred to another (`XCLAIM`/`XAUTOCLAIM`). A Consumer is created implicitly on first read or explicitly via `XGROUP CREATECONSUMER`.
_Avoid_: worker, reader, client (a Client owns connections, not group membership)

**Pending Entries List (PEL)**:
The Consumer Group's set of entries delivered to some Consumer but not yet acknowledged, each tracking its owning Consumer, idle time, and delivery count. `XACK` removes an entry from it; `XCLAIM`/`XAUTOCLAIM` reassign ownership within it; `XPENDING` inspects it as either a group-level summary or a per-entry extended form.
_Avoid_: pending list, ack list, inflight set

## Example dialogue

> **Dev**: When a fiber calls `client.get`, does it borrow a connection from a pool?
>
> **Expert**: No — ordinary Commands are pipelined onto the **Multiplexed Connection**; replies come back FIFO. Only `WATCH`/`MULTI`/`EXEC` and blocking commands acquire a **Dedicated Connection**, transparently.
>
> **Dev**: And a `SUBSCRIBE` — also a Dedicated Connection?
>
> **Expert**: No, subscriptions share the one **Subscription Connection**, created lazily, so a slow consumer can never stall command replies.
>
> **Dev**: If I send three GETs as a batch, is that a Transaction?
>
> **Expert**: That's a **Pipeline** — one round-trip, typed results, no atomicity. It only becomes a **Transaction** if executed via `MULTI`/`EXEC`, and then all keys must hash to one slot in cluster mode. A Pipeline, by contrast, may be split across nodes.

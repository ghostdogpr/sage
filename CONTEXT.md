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

**Dedicated Connection**:
A connection exclusively held for commands with per-connection state or blocking behavior (`WATCH`/`MULTI`/`EXEC`, `BLPOP`, …), acquired transparently from an on-demand pool.
_Avoid_: exclusive connection, pooled connection

**Subscription Connection**:
The lazily-created connection (one per client) that carries all pub/sub subscription state and push frames, isolating slow consumers from the Multiplexed Connection.
_Avoid_: pub/sub connection

**Cached Read**:
A read command executed with explicit per-call opt-in to client-side caching: served from the local cache until a server invalidation push or TTL evicts it.
_Avoid_: tracked read, local read

**Command**:
A pure value in the Core describing one server command: its wire encoding and its typed reply decoder (`Command[Out]`). The single source of truth; the client's per-command methods are sugar over it.
_Avoid_: request, operation

**Pipeline**:
An applicative composition of Commands sent in one round-trip, yielding a typed tuple of results. Not a transaction — no atomicity.

**Transaction**:
A Pipeline executed atomically via `MULTI`/`EXEC` on a Dedicated Connection, optionally guarded by `WATCH`.
_Avoid_: batch (that's a Pipeline)

**Codec**:
A typeclass converting one user type to/from its wire bytes at a command boundary. A boundary converter, not a serialization framework; keys and values have separate codec typeclasses (keys must also be hashable to cluster slots).
_Avoid_: serializer, schema

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

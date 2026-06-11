# Shared ReadFrom policy and replica read routing

Read scaling routes read-only commands to replicas, governed by one `ReadFrom` policy that serves both the cluster `READONLY` mode and the master-replica topology. The policy lives at the **top level** of `SageConfig` (`readFrom: ReadFrom = Master`), not inside `ClusterConfig`, so a single setting covers both deployment shapes and the public surface stays consistent across the #40 freeze. The values are `Master | MasterPreferred | Replica | ReplicaPreferred`, defaulting to `Master` — today's behavior, every command to the master. The vocabulary is **master**, never *primary*, matching `Shard.master` and the Node/Shard glossary; an `Any` value was dropped as redundant with the `*Preferred` pair.

## The eligibility gate

The policy redirects only the read-only subset. A command is replica-eligible iff `command.isReadOnly && command.execution == Ordinary`; everything else goes to the master regardless of the policy:

- **Writes and non-read-only reads** — always master.
- **Blocking read-only commands** (`XREAD BLOCK`) — always master. Honoring the policy here would force a Dedicated Pool onto every replica for a rare case; instead a replica is a single auto-pipelined Multiplexed Connection and blocking reads stay on the writable node.
- **`cached` reads** — always master. Client-side caching (ADR-0028) is anchored to the connection that registers tracking and receives invalidation pushes; serving a `cached` read from a replica would cache a possibly-stale value that the master's tracking stream could never invalidate. The two features compose by not overlapping. (In both cluster and master-replica mode `cached` currently runs *uncached* on the master — the local cache there is a follow-up, as in cluster today — but it stays master-pinned so the call is portable and never silently reads a stale replica.)
- **Transactions / `WATCH` / `MULTI` / `EXEC`** — always master (Dedicated Pool, and they need the writable node anyway).

The gate is `isReadOnly`, the looser property, not `cacheable` (the stricter, deterministic one). Non-deterministic read-only commands (`SRANDMEMBER`, `TTL`) therefore run on replicas under a replica policy and may observe slightly stale or time-varying state — the accepted contract of replica reads, not a fault.

## Selection lives in the runtime

Replica selection is inseparable from connection liveness (is this replica's socket up? established?) and from round-robin and fallback state — none of which the pure `ClusterTopology` snapshot can or should hold (ADR-0021). So `ClusterTopology.route` keeps returning the master and slot; the core gains only a pure replica-lookup helper, and `ReadFrom` never enters `sage-core`. The runtime consults the policy, picks a **live** replica by **round-robin per shard**, and applies fallback. Nearest/latency-based routing is deliberately out of scope (it needs per-node latency tracking).

Fallback by policy: `MasterPreferred` → master, else a live replica; `ReplicaPreferred` → a live replica, else master; `Replica` is strict and **fails** when no replica is reachable, reusing `NotConnected` with a descriptive message rather than growing the sealed hierarchy. A `Standalone` topology has no replicas from sage's view, so config validation rejects `readFrom == Replica` there (a guaranteed-broken config) while letting the `*Preferred` policies degrade silently to the one node, so the same `readFrom` can be shared across environments.

## Pipelines route all-or-nothing

A Pipeline is split per node by slot, results reassembled in submission order. Letting individual commands within one pipeline pick master-vs-replica would split a single slot across two connections and create a read-your-own-write trap (a `SET k` then `GET k` in the same batch, the `GET` on a lagging replica). Instead the decision is per pipeline: if **every** command is replica-eligible, each shard's batch routes to a replica; if **any** command is not, the whole pipeline goes to the masters. This keeps the split model and reassembly untouched and never splits a slot across master and replica.

## Considered Options

- **`ReadFrom` inside `ClusterConfig`** — rejected: master-replica mode needs the same setting, and a cluster-local home would either duplicate it or be binary-incompatible to move after the #40 freeze.
- **Policy in the pure core** (`route` returns the chosen replica) — rejected: the core would have to model connection liveness and load-balancing state it deliberately lacks.
- **Per-command replica routing inside a pipeline** — rejected for the slot-splitting and read-your-write hazards above; all-or-nothing keeps the benefit for pure-read batches without them.

## Consequences

In cluster mode each replica is a single Multiplexed Connection (no Dedicated Pool, no Subscription Connection, no tracking), established lazily on the first read routed to it, with `READONLY` issued at connection setup (ADR-0039). In master-replica mode the same selection helper runs over the single shard. `Standalone` is unaffected at runtime (it has no replicas). The shared per-node-pool and replica-selection helpers are extracted so the cluster and master-replica runtimes do not duplicate them.

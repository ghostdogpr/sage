# All-masters broadcast routing for script and function management

A cluster does not replicate scripts or functions across shards — each master keeps an independent cache. With ordinary keyless routing (`Route.Keyless` → one arbitrary master), `SCRIPT LOAD`/`FUNCTION LOAD` would load on a single node, so an `EVALSHA`/`FCALL` routed by key to a *different* master fails `NOSCRIPT`. To make cluster scripting correct without the caller managing nodes (PRD story 26), the mutating management commands fan out to every slot-owning master.

Because per-command client methods must delegate to `run` (the `Client` contract), fan-out cannot live in bespoke sugar the way `scanAll` does — otherwise `client.run(Commands.scriptLoad(...))` would diverge from `client.scriptLoad(...)`. It is therefore a routing property the pure `Command` carries: a new field `allMasters: Boolean = false`. `ClusterLive.dispatch` checks it *before* `route` and sends to all masters in parallel. It is inert on a standalone server, where the single-connection `run` path never consults routing, so `run` behaves identically whether called directly or through sugar.

The broadcast set is the per-node mutations: `SCRIPT LOAD`, `FUNCTION LOAD`, `SCRIPT FLUSH`, `FUNCTION FLUSH`, `FUNCTION DELETE`, `FUNCTION RESTORE`, plus `FLUSHALL` and `FLUSHDB` (each master holds its own keyspace shard, so flushing the logical database means reaching every master). The reduction: the command is sent to every master and fails if any node fails; otherwise it returns one node's reply. Because these replies are deterministic across nodes (identical SHA1, identical library name, or `OK`), returning the first success is equivalent to comparing them, so no cross-node comparison is performed. There is no rollback on partial failure, but every one of these operations is idempotent, so a re-run converges.

A Pipeline batches commands per node and so cannot fan one out to every master; the cluster pipeline path rejects an all-masters command up front rather than loading a single node.

Reads (`SCRIPT EXISTS`, `FUNCTION DUMP`/`LIST`/`STATS`) and the `KILL` commands (`SCRIPT KILL`, `FUNCTION KILL`) do **not** broadcast: reads are consistent across masters once the mutations broadcast (`LIST`/`DUMP`) or are inherently per-node (`STATS`), and broadcasting a `KILL` would make idle nodes answer `NOTBUSY`, breaking the uniform any-error-fails rule. They stay keyless (one arbitrary master) with a documented cluster limitation.

## Considered Options

- **Implicit fan-out on the base command (chosen)** — transparent per PRD-26. The explicit node-selection alternative (Lettuce's `.masters()`) was rejected because sage has no node-targeting surface and the unified-client design (ADR-0007) deliberately hides nodes.
- **API-level orchestration like `scanAll`** — violates "sugar delegates to `run`"; the value path and the method path would diverge.
- **Leave node-local and document it** — silently breaks `EVALSHA` in cluster; rejected.

## Consequences

`Command` gains a routing field and `ClusterLive` gains a broadcast branch plus the any-error-fails combiner. A partial failure can leave a script on a subset of masters; an idempotent re-run converges, and the failure is surfaced as the failing node's error. There is no automatic `EVALSHA`→`EVAL` fallback (ADR-0033) — the caller owns retry.

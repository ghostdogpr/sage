# Cluster pub/sub: classic pinned, sharded per owning node, resubscription driven by disconnect

Cluster mode abandons the standalone single-Subscription-Connection model. Classic (channel/pattern) subscriptions keep one Subscription Connection per client, pinned to an arbitrary master — classic `PUBLISH` broadcasts across the whole cluster bus, so any node sees every channel — and moved to another master on failover. Sharded subscriptions instead take one Sharded Subscription Connection per owning Node, because a Shard Channel's `SSUBSCRIBE` only reaches the node owning the channel's Slot and an `SPUBLISH` only reaches subscribers on that same shard. A `ClusterSubscriptions` manager owns both: the per-node connections are created on demand and released when their last subscription ends. `SPUBLISH` carries its channel as a routing key (`FirstKey`), so the existing slot routing and MOVED/ASK handling place it on the owner with no new code; classic `PUBLISH` stays keyless and rides any node.

A single node can own several slot ranges, so the manager emits one `SSUBSCRIBE` per slot rather than batching a node's channels — batching across slots returns `CROSSSLOT` (the bug node-redis shipped in #2902). One logical `sSubscribe` call may span several owner nodes; it is backed by one manager-owned `Sink` registered into each node's connection, so the single-consumer/backpressure model is reused without a merge selector.

## Resubscription on migration and failover

Resubscription is **disconnect-driven**. On slot migration the owning server pushes `sunsubscribe` for the affected channels and then closes the client connection (redis/redis#8621); a subscribe-only workload never sees a `MOVED` on the command path, so the dropped socket is the reliable signal. Cluster connections therefore do **not** run their own reconnect loop to a fixed address (as the standalone Subscription Connection does); on disconnect they notify `ClusterSubscriptions`, which forces a topology refresh and re-establishes each affected subscription on the current owner — the new slot owner for sharded, any live master for classic. Subscriptions are also re-homed on every topology `adopt()`, so a `MOVED` observed elsewhere (e.g. from an `SPUBLISH`) proactively corrects the subscription side.

## Considered alternatives

- **Pinned, no re-route** (go-redis, Lettuce): bind a sharded subscription to the node chosen at first subscribe and never follow the slot. Rejected: it silently drops shard messages after migration or failover — the open, unresolved bugs in go-redis (#3099, WONTFIX) and Lettuce (#2940, #3213).
- **Topology-observer only** (no disconnect handling): re-home solely when a refresh installs a new topology. Rejected: nothing triggers a prompt refresh for a subscribe-only workload, and the connection's own reconnect would loop on the old owner in the meantime.
- **One connection per slot** instead of per node: lets the existing batch-`SUBSCRIBE` path stand unchanged, but channels hashing to many slots on one node open many sockets to it. Rejected for connection explosion.

## Consequences

- A migration/failover window is at-most-once: messages published between the server's `sunsubscribe` and the client's resubscribe on the new owner are lost. This matches every production client and the Redis pub/sub contract.
- Sharded subscriptions get better consumer isolation than classic ones: a slow consumer backpressures only the connections feeding its own subscription, and peers on other nodes are untouched.
- Sharded pub/sub is exposed on standalone too, for topology portability (ADR-0007): there it reuses the single Subscription Connection in shard mode (no slots, so no grouping), and `SPUBLISH`'s routing key is inert.

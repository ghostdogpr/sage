# Master-replica topology and event-driven failover

`Topology.MasterReplica(seeds, config)` connects to a non-cluster deployment of one master and one or more replicas, for read scaling outside a cluster. It is discovery-only: at connect, sage asks a reachable seed its role (`ROLE`, or `INFO replication` for a single seed) and classifies the master and replicas from the supplied endpoints. The client type is unchanged (ADR-0007) — only the topology selects the runtime, as Standalone and Cluster do. Sentinel-managed discovery stays out of scope.

There is no asserted-static `master + replicas` form. Its only advantage would be working when `ROLE`/`INFO` are blocked, but sage already mandates `HELLO` and `CLIENT SETINFO` (ADR-0037), so a node that answers those answers `ROLE` too; and an asserted master is a footgun across failover (writes to a demoted node until something refreshes). Listing your nodes as seeds covers the same use case while staying correct.

A master-replica deployment is effectively a single Shard owning the whole keyspace, but it is served by a focused `MasterReplicaLive` runtime rather than a degenerate `ClusterLive`. The cluster runtime couples discovery (`CLUSTER SLOTS`) to a redirect state machine and carries slots, `CROSSSLOT`, all-masters fan-out, and sharded pub/sub — none of which apply here, and none of which inject cleanly. The genuinely shared parts — the per-node connection pool and the replica-selection helper (ADR-0038) — are extracted and used by both runtimes; the rest stays mode-specific.

## Failover is event-driven, with no periodic poll

Master-replica mode has none of cluster mode's failover machinery: no cluster bus, no `MOVED`/`ASK` redirects, no `CLUSTER SLOTS`. When a replica is promoted and the master demoted, the client learns of it from two events, both throttled by `minRefreshInterval`:

1. **Reconnect** — a dropped master connection re-discovers roles, since a dropped master often *is* a failover. Combined with ADR-0014's reconnect-behind-a-stable-endpoint, this covers managed deployments where a primary DNS name or VIP repoints on failover: the connection reconnects to the same name, now the promoted node, and re-discovery confirms the roster.
2. **`-READONLY` from the presumed master** — it was demoted in place without dropping the socket. Re-discover roles, and **fail the offending write** (refresh-then-terminal), so the caller's retry lands on the freshly-discovered master. This is the same disposition cluster mode already applies to a `READONLY` from a demoted master.

There is deliberately **no background periodic `ROLE` poll**. A silent promotion with no traffic to the demoted node leaves the roster stale but harmlessly — the first write to hit `-READONLY` triggers the refresh — and cluster mode likewise refreshes on events, not a timer. `MasterReplicaConfig` leaves room to add an opt-in poll later if demand appears.

This is more than the reference clients do: Lettuce's plain (non-sentinel, non-cluster) master-replica performs a one-time `ROLE` lookup and stays static, surfacing `-READONLY` to the caller and requiring the connection to be rebuilt outside the library on failover; redis4cats inherits that. sage's reconnect and `READONLY`-driven re-discovery turn a silent write outage into a self-healing retry, and the acceptance criteria require roles to refresh on reconnect regardless.

## Consequences

Replicas serve reads **without** `READONLY` — that command is cluster-only; the connection-setup `READONLY` of ADR-0038/ADR-0039 applies only to cluster replica connections. A new pure `Connection.readonly` builder is added to the core for the cluster path; no `READWRITE` is needed, since master connections never issue `READONLY`. A write that meets a demoted master fails fast (ADR-0006) rather than following a redirect the protocol does not provide. Read routing and the staleness contract are governed by the shared `ReadFrom` policy (ADR-0038).

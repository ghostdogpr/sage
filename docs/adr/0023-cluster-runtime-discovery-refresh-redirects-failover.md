# Cluster runtime: discovery, refresh, redirects, and failover

ADR-0021 keeps cluster routing pure; this is its runtime counterpart — the effectful layer that discovers the Cluster Topology, executes the engine's `Route` classifications across one connection bundle per Node, and follows redirects. A cluster `Client` is the same type as standalone (ADR-0007), selected by a `Topology.Cluster(seeds)` config; each Node reuses the standalone bundle (Multiplexed Connection + Dedicated Pool), so ordinary, blocking, and (later) per-node work all run through the connection machinery already hardened for standalone.

Topology is discovered with `CLUSTER SLOTS`, not `CLUSTER SHARDS`: `SLOTS` exists on Redis 6 (the matrix's floor) where `SHARDS` does not, and it carries everything routing needs (slot ranges, master, replicas). Refresh is reactive and single-flight — triggered by `MOVED`, by an `Unowned` route, by `-READONLY`, and (the non-obvious one) by a routed command failing because its target Node is unreachable. That last trigger is required, not optional: after a master dies we keep routing its slots to the dead Node off the stale topology, so those commands fail `NotConnected`/`ConnectionLost` and never see a `MOVED` — a purely redirect-driven refresh would stall on the dead master forever. A refresh is also the pruning authority: bundles for masters absent from the new topology are closed, so a vanished Node's reconnect loop does not leak.

Following a redirect or retrying after an unreachable Node is deliberately *not* a violation of ADR-0006's no-auto-retry rule. `MOVED`/`ASK` are pre-execution refusals — the command did not run on the wrong Node — so re-sending is safe. After a failover refresh we retry only the provably-not-executed cases (`NotConnected`, `ConnectionLost(mayHaveExecuted = false)`); a `ConnectionLost(mayHaveExecuted = true)` still fails fast per ADR-0006, kicking a background refresh only so later commands route right. Redirects are bounded (default 5, as lettuce) and exhaustion surfaces as a plain `ServerError`, not a bespoke type — matching lettuce, the closest architectural analog, rather than Jedis's pool-per-call model.

`ASK` needs `ASKING` to immediately precede the command on the *same* connection. On the auto-pipelined Multiplexed Connection that is shared by every fiber, this is guaranteed by submitting the two as one atomic batch (the existing `submitAll`, one socket write, FIFO-matched consecutively); on a Dedicated Connection it is automatic, since the lease is exclusive.

Pipelines and Transactions are rejected outright in cluster mode for this slice — including the single-slot cases that would route cleanly — and arrive whole in the split/merge work (#24). Partial support would blur where that issue begins and invite bug reports about the boundary.

## Considered Options

- **`CLUSTER SLOTS` (chosen)** — one command across Redis 6/7/8 and Valkey; deprecated since 7.0 but still present in 8.x.
- **`CLUSTER SHARDS`** — richer and non-deprecated, but absent on Redis 6, which the integration matrix requires.
- **MOVED-only refresh** — simpler, but stalls on a dead master that never answers with a redirect.
- **A dedicated redirects-exhausted exception** — clearer to match on, but heavier than the condition warrants; lettuce surfaces it through its generic command-execution error.

## Consequences

- A `-READONLY` reply keeps its standalone poison-and-reconnect-and-fail-fast behavior (ADR-0014) and *additionally* triggers a topology refresh in cluster mode.
- Each Node carries its own Dedicated Pool even if no blocking command ever routes there; pools are lazy, so an idle Node's pool costs nothing.
- A seed that is not itself a master in the discovered topology is dropped after bootstrap; real master bundles are created lazily on first route.

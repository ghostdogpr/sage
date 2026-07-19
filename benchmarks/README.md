# sage runtime benchmarks

End-to-end JMH benchmarks of the sage client against a real Redis, alongside the realistic Scala-native competitors plus raw Lettuce as the
JVM ceiling. It is **dev-only and never published**, and **commits no result
numbers** — competitor numbers drift with their library versions, so you run the harness on demand and read the numbers fresh.

## What is measured

Workloads (the command path only — no blocking or pub/sub):

| Workload | Class / method | Notes |
| --- | --- | --- |
| Throughput | `ThroughputBench.get` / `.set` | swept over a **concurrency** param (`1, 8, 64, 256`) and **valueSize** (`16, 1024`); reported as a curve. Auto-pipelining only pays off as concurrency rises, so a single point would mislead. |
| Topology | `TopologyBench.get` / `.set` | sage-only, in the `benchmarksZio` cell: the throughput workload swept over a **topology** param (`standalone, cluster, master-replica`). The cluster is a single self-provisioned node owning every slot and master-replica discovers a replica-less master, so all three serve identical commands. The comparison is **end-to-end topology overhead**: the cluster trial runs a `--cluster-enabled` server in its own container, so server-mode differences and instance variance are included, not client dispatch alone. |
| Big collection | `CollectionBench.mget` / `.hgetall` | a single MGET / HGETALL of 1000 keys/fields — the large-reply parse path (concurrency does not apply). |

Allocation profile: add `-prof gc` to any run (see below).

## Clients

One JVM per backend (the sage backends are cross-compiled from the same `sage.*` sources and collide on a classpath; kyo is on a
different Scala version), so the harness is one `projectMatrix` cell per backend. Competitors live in the cell whose ecosystem they target:

| `client` param | Cell | Notes |
| --- | --- | --- |
| `sage-zio` / `zio-redis` | `benchmarksZio` | zio-redis is native ZIO. |
| `sage-ce` / `redis4cats` | `benchmarksCe` | redis4cats wraps Lettuce. |
| `sage-ox` / `lettuce` / `rediscala` / `jedis` | `benchmarksOx` | `lettuce` is the async/auto-pipelined Lettuce client — the JVM ceiling. `jedis` is the sync/blocking Java client (RESP3), driven with one pooled connection per concurrency lane. |
| `sage-pekko` | `benchmarksPekko` | no native Pekko (Future) competitor exists. |
| `sage-kyo` | `benchmarksKyo<scala-next>` | suffix tracks the Next Scala version; no native Kyo competitor exists. |

## Running

Each cell self-provisions Redis `8.8.0` via testcontainers inside JMH `@Setup(Level.Trial)`, so you only need Docker running (and `jq` for the merge).

`benchmarks/run.sh` runs the cells, merges the per-cell JMH JSONs into `benchmarks/results/all.json` (one directly-comparable run — identical
benchmark names, unique `client` param), and prints a summary. **Upload `all.json` to [jmh.morethan.io](https://jmh.morethan.io) for charts.**

Arguments are forwarded to each cell's JMH run, so scope a quick run instead of the full (hours-long) cross-product:

```bash
benchmarks/run.sh                                                        # everything — hours
benchmarks/run.sh -p concurrency=64 -p valueSize=256 -f 1 -wi 3 -i 3     # ALL benchmarks at medium concurrency/size — minutes
benchmarks/run.sh -p concurrency=1,64 -p valueSize=256 -f 1 -wi 3 -i 3   # sweep several concurrency points in one run (comma-separated)
benchmarks/run.sh ThroughputBench.get -p concurrency=64 -f 1 -wi 3 -i 3  # one workload — minutes
benchmarks/run.sh -prof gc ThroughputBench.get -p concurrency=64         # allocation profile of the round-trip
```

A single run with `-p concurrency=… -p valueSize=…` covers all benchmarks in one pass: throughput uses both params; collection uses
`valueSize` (it has no `concurrency` — a big-reply command is one command) and JMH harmlessly ignores the extra `-p concurrency` for it.

A filter that only matches one cell (e.g. `-p client=zio-redis`) simply leaves the others out of the merge. `benchmarks` is not part of the
root aggregate, so ordinary `compile`/`test` never pull in JMH or the competitor clients.

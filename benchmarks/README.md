# Sage runtime benchmarks

These JMH benchmarks compare Sage with Scala Redis clients and Lettuce against a real Redis server. The benchmark module is for development and is not published. The repository does not commit results because client versions and hardware change them. Run the benchmarks to get results for the current checkout and machine.

## What is measured

The benchmarks measure the command path. They exclude blocking commands and pub/sub.

| Workload | Class / method | Notes |
| --- | --- | --- |
| Throughput | `ThroughputBench.get` and `.set` | Sweeps `concurrency` over `1, 8, 64, 256` and `valueSize` over `16, 1024`. Use the resulting curve because auto-pipelining changes with concurrency. |
| Topology | `TopologyBench.get` and `.set` | Runs only in `benchmarksZio` and sweeps `topology` over `standalone, cluster, master-replica`. The cluster has one node that owns every slot. The master-replica case discovers a master without replicas. Each case serves the same commands. The cluster uses a separate `--cluster-enabled` server container, so the result includes server-mode and instance differences as well as client dispatch. |
| Big collection | `CollectionBench.mget` and `.hgetall` | Sends one `MGET` or `HGETALL` for 1,000 keys or fields. Concurrency does not apply to this large-reply parsing benchmark. |

Add `-prof gc` to measure allocations.

## Clients

Each backend runs in a separate JVM and `projectMatrix` cell. The Sage backends compile from the same `sage.*` sources and cannot share a classpath. Kyo also uses a different Scala version. Each competing client runs in the cell for its ecosystem.

| `client` param | Cell | Notes |
| --- | --- | --- |
| `sage-zio` / `zio-redis` | `benchmarksZio` | zio-redis is native ZIO. |
| `sage-ce` / `redis4cats` | `benchmarksCe` | redis4cats wraps Lettuce. |
| `sage-ox` / `lettuce` / `rediscala` / `jedis` | `benchmarksOx` | `lettuce` is the asynchronous, auto-pipelined Java baseline. `jedis` is the synchronous RESP3 Java client and uses one pooled connection per concurrency lane. |
| `sage-pekko` | `benchmarksPekko` | no native Pekko (Future) competitor exists. |
| `sage-kyo` | `benchmarksKyo<scala-next>` | suffix tracks the Next Scala version; no native Kyo competitor exists. |

## Running

Each cell self-provisions Redis `8.8.0` via testcontainers inside JMH `@Setup(Level.Trial)`, so you only need Docker running (and `jq` for the merge).

`benchmarks/run.sh` runs each cell and merges the JMH JSON files into `benchmarks/results/all.json`. Benchmark names remain identical, and the `client` parameter identifies each implementation. The script then prints a summary. Upload `all.json` to [jmh.morethan.io](https://jmh.morethan.io) to draw charts.

The script passes its arguments to each JMH cell. Use a filter or parameter to avoid the full run, which takes hours.

```bash
benchmarks/run.sh                                                        # every benchmark; takes hours
benchmarks/run.sh -p concurrency=64 -p valueSize=256 -f 1 -wi 3 -i 3     # every benchmark at one concurrency and size; takes minutes
benchmarks/run.sh -p concurrency=1,64 -p valueSize=256 -f 1 -wi 3 -i 3   # sweep several concurrency points in one run (comma-separated)
benchmarks/run.sh ThroughputBench.get -p concurrency=64 -f 1 -wi 3 -i 3  # one workload; takes minutes
benchmarks/run.sh -prof gc ThroughputBench.get -p concurrency=64         # allocation profile of the round-trip
```

A run with both `-p concurrency=...` and `-p valueSize=...` covers all benchmarks. The throughput benchmarks use both parameters. The collection benchmark uses only `valueSize` because it sends one command. JMH ignores its extra `concurrency` parameter.

A filter such as `-p client=zio-redis` can match only one cell. The merge then omits the other cells. The root project does not aggregate `benchmarks`, so ordinary `compile` and `test` tasks do not load JMH or competing clients.

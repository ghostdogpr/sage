# Examples

Runnable, idiomatic sage usage from each backend. Every example uses its ecosystem's native types — ZIO `Task`, cats-effect `IO`, Ox direct
style, Kyo computations — with no wrapper visible. The module is never published and is compiled in CI as part of the normal build.

Two imports cover everything: `import sage.*` for the command vocabulary and connection config, and `import sage.<backend>.*` for the client.

## Layout

```
shared/   Domain.scala            a User type with a hand-written ValueCodec, reused by every backend
zio/      …Example.scala + Tour   the ZIO tour, plus the cluster + sharded pub/sub spotlight
ce/       …Example.scala + Tour   the cats-effect tour, plus the TLS/ACL spotlight
ox/       …Example.scala + Tour   the Ox tour, plus the master-replica + ReadFrom spotlight
kyo/      …Example.scala + Tour   the Kyo tour
```

Each backend's `Tour` is a runnable entry point that wires the client with that ecosystem's idiomatic construction form (ZIO `layer`,
cats-effect `resource`, Ox/Kyo `scoped`) and runs the common feature set: commands across several families, a Pipeline, a WATCH-guarded
transaction, classic pub/sub, and a cached read. The `…Example` objects are the individual copy-pasteable snippets the tour stitches together.

## Running the tours

The tours connect to a Redis or Valkey server on `localhost:6379`. Start one first, e.g.:

```sh
docker run --rm -p 6379:6379 redis:8
```

Then run a tour:

```sh
sbt examplesZio/run
sbt examplesCe/run
sbt examplesOx/run
sbt examplesKyo3_8_3/run
```

## Spotlights

The cross-cutting connection features are config-only — the command code is identical to the tours — so each is shown once, on a
representative backend, rather than repeated four times:

- **Cluster + sharded pub/sub** — `zio/ClusterExample.scala`
- **TLS + ACL** — `ce/TlsExample.scala`
- **Master-replica + `ReadFrom`** — `ox/MasterReplicaExample.scala`

These need a cluster, a TLS-enabled server, or a master-replica deployment respectively, so they are not part of the localhost tours; they
compile in CI to keep the wiring honest and exist to be read.

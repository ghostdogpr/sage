# Examples

These runnable examples cover every Sage backend. They use plain blocking calls with the direct client, Ox direct style, ZIO `Task`, Cats Effect `IO`, Kyo computations, or Pekko `Future` and Pekko Streams. The build does not publish this module, but CI compiles it.

Import `sage.*` for commands and connection configuration. Import `sage.<backend>.*` for the client.

## Layout

```
shared/   Domain.scala            a User type with a hand-written ValueCodec, reused by every backend
direct/   …Example.scala + Tour   the direct tour (plain blocking calls, iterators, and callbacks)
ox/       …Example.scala + Tour   the Ox tour, plus the master-replica + ReadFrom spotlight
zio/      …Example.scala + Tour   the ZIO tour, plus the cluster + sharded pub/sub spotlight
ce/       …Example.scala + Tour   the Cats Effect tour, plus the TLS/ACL spotlight
kyo/      …Example.scala + Tour   the Kyo tour
pekko/    …Example.scala + Tour   the Pekko tour (scala.concurrent.Future + Pekko Streams)
```

Each backend's `Tour` is a runnable entry point. It constructs the client with `use` for the direct client, which closes the client when its block returns, a `scoped` value for Ox or Kyo, a ZIO `layer`, a Cats Effect `resource`, or Pekko `connect` with an explicit `close` and a typed `ActorSystem`. Each tour runs commands from several families, a pipeline, a `WATCH` transaction, classic pub/sub, and a cached read. The `...Example` objects contain the individual snippets used by the tour.

## Running the tours

The tours connect to a Redis or Valkey server on `localhost:6379`. Start one first, e.g.:

```sh
docker run --rm -p 6379:6379 redis:8
```

Then run a tour:

```sh
sbt examplesDirect/run
sbt examplesOx/run
sbt examplesZio/run
sbt examplesCe/run
sbt exampleKyo
sbt examplesPekko/run
```

## Spotlights

Connection behavior depends on configuration, while the command code remains the same. Each example below uses one backend to avoid repeating the command code.

- Cluster and sharded pub/sub use `zio/ClusterExample.scala`.
- TLS and ACL use `ce/TlsExample.scala`.
- Master-replica routing and `ReadFrom` use `ox/MasterReplicaExample.scala`.

These examples require a cluster, a TLS-enabled server, or a master-replica deployment. They do not run as part of the localhost tours, but CI compiles them.

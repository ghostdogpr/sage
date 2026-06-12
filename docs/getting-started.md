# Getting started

**Sage** is a native [Redis](https://redis.io) and [Valkey](https://valkey.io) client for [Scala 3](https://www.scala-lang.org/). There is no Java client wrapped underneath: the RESP3 protocol, the commands, and the codecs are implemented directly in Scala, on a zero-dependency, effect-free core.

That core is paired with a runtime written once and cross-published for [Ox](https://ox.softwaremill.com), [ZIO](https://zio.dev), [cats-effect](https://typelevel.org/cats-effect/), and [Kyo](https://getkyo.io), so you use sage with your ecosystem's native types and no wrapper in sight. It targets RESP3 and modern Redis 8+ / Valkey 8+, runs on Scala 3.3.x LTS and later, and requires JDK 21+.

## Installation

Add the artifact for your effect system. The core is pulled in transitively, so you depend on one module only.

::: code-group

```scala [Ox]
"com.github.ghostdogpr" %% "sage-client-ox" % "@VERSION@"
```

```scala [ZIO]
"com.github.ghostdogpr" %% "sage-client-zio" % "@VERSION@"
```

```scala [Cats Effect]
"com.github.ghostdogpr" %% "sage-client-ce" % "@VERSION@"
```

```scala [Kyo]
"com.github.ghostdogpr" %% "sage-client-kyo" % "@VERSION@"
```

:::

Two imports cover everything: `import sage.*` for the command vocabulary and connection config, and `import sage.<backend>.*` for the client.

## Your first connection

A `SageClient` owns all connections to one server or cluster. You build it from a `SageConfig` using your ecosystem's idiomatic construction form: a scoped resource for Ox and Kyo, a `ZLayer` for ZIO, and a `Resource` for cats-effect. The command surface is identical across all four; only this wiring differs.

::: code-group

```scala [Ox]
import ox.supervised

import sage.*
import sage.ox.*

@main def main(): Unit =
  supervised {
    val config = SageConfig(
      topology = Topology.Standalone(Endpoint("localhost", 6379))
    )
    val client   = SageClient.scoped(config)
    client.set("greeting", "hello")
    val greeting = client.get[String, String]("greeting")
    println(s"greeting=$greeting") // Some("hello")
  }
```

```scala [ZIO]
import zio.*

import sage.*
import sage.zio.*

object Main extends ZIOAppDefault {
  val config = SageConfig(
    topology = Topology.Standalone(Endpoint("localhost", 6379))
  )

  def run =
    ZIO.serviceWithZIO[SageClient] { client =>
      for {
        _        <- client.set("greeting", "hello")
        greeting <- client.get[String, String]("greeting")
      } yield greeting
    }.provide(SageClient.layer(config))
}
```

```scala [Cats Effect]
import cats.effect.{IO, IOApp}

import sage.*
import sage.ce.*

object Main extends IOApp.Simple {
  val config = SageConfig(
    topology = Topology.Standalone(Endpoint("localhost", 6379))
  )

  def run: IO[Unit] =
    SageClient.resource(config).use { client =>
      for {
        _        <- client.set("greeting", "hello")
        greeting <- client.get[String, String]("greeting")
      } yield ()
    }
}
```

```scala [Kyo]
import kyo.*

import sage.*
import sage.kyo.*

object Main extends KyoApp {
  val config = SageConfig(
    topology = Topology.Standalone(Endpoint("localhost", 6379))
  )

  run {
    Scope.run {
      for {
        client   <- SageClient.scoped(config)
        _        <- client.set("greeting", "hello")
        greeting <- client.get[String, String]("greeting")
      } yield greeting
    }
  }
}
```

:::

::: tip How it works
A `SageClient` is not a single connection, and ordinary commands do not borrow from a pool. They are written to one **multiplexed connection** per node, shared by every fiber: sage **auto-pipelines** them, coalescing concurrent commands into fewer socket writes and matching replies back in FIFO order. This is transparent (you never assemble a pipeline to get it), and it is what keeps throughput high under concurrency.

Two cases step off that connection automatically. Commands that hold per-connection state or block (`WATCH`/`MULTI`/`EXEC`, `BLPOP`, and the like) lease a **dedicated connection** from an on-demand pool for the duration. Pub/sub subscriptions share a separate **subscription connection**, created the first time you subscribe, so a slow consumer can never stall command replies.
:::

## A short tour

The snippets below show the same operations on each backend: pick your tab. In Ox they return values directly; in ZIO, cats-effect, and Kyo they are steps in a for-comprehension over that ecosystem's effect type. Each assumes a `client` in scope and the usual imports for your effect type (plus `import scala.concurrent.duration.*` where a duration appears).

### Commands

Methods are named one-for-one with Redis commands, grouped by family (strings, hashes, lists, sets, sorted sets, and so on). Keys and values are typed, with the codec selected by the type parameters. Any type with a `ValueCodec` (here, a `User`) rides over the wire the same way as a `String`.

::: code-group

```scala [Ox]
client.set("greeting", "hello")
val greeting = client.get[String, String]("greeting") // Some("hello")
client.incrBy("counter", 10)

client.hSet("user:1", ("name", "Ada"), ("age", "36"))
val profile = client.hGetAll[String, String, String]("user:1")
// Map("name" -> "Ada", "age" -> "36")

client.set("user:ada", User("Ada", 36))
val ada = client.get[String, User]("user:ada") // Some(User("Ada", 36))
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _        <- client.set("greeting", "hello")
  greeting <- client.get[String, String]("greeting")
  _        <- client.incrBy("counter", 10)
  _        <- client.hSet("user:1", ("name", "Ada"), ("age", "36"))
  profile  <- client.hGetAll[String, String, String]("user:1")
  _        <- client.set("user:ada", User("Ada", 36))
  ada      <- client.get[String, User]("user:ada")
} yield (greeting, profile, ada)
```

:::

See [Commands & codecs](/commands) for the full vocabulary and how to write a codec for your own types.

### Pipelines and transactions

Compose commands into a **pipeline** to send them in one round-trip and get back a typed tuple of results:

::: code-group

```scala [Ox]
client.set("pipe:a", "x")
client.set("pipe:n", 10)
val tuple = client.pipeline(
  (
    Commands.get[String, String]("pipe:a"),
    Commands.incrBy("pipe:n", 5)
  ).pipeline
)
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _     <- client.set("pipe:a", "x")
  _     <- client.set("pipe:n", 10)
  tuple <- client.pipeline(
             (
               Commands.get[String, String]("pipe:a"),
               Commands.incrBy("pipe:n", 5)
             ).pipeline
           )
} yield tuple
```

:::

A **transaction** runs a pipeline atomically via `MULTI`/`EXEC`, optionally guarded by `WATCH` for optimistic concurrency. A `None` result means a watched key changed before `EXEC`, the normal signal to retry:

::: code-group

```scala [Ox]
client.set("tx:n", 1)
val result = client.transaction { tx =>
  tx.watch("tx:n")
  tx.get[String, Int]("tx:n")
  tx.exec(
    (Commands.incr("tx:n"), Commands.incrBy("tx:n", 4)).pipeline
  )
}
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _      <- client.set("tx:n", 1)
  result <- client.transaction { tx =>
              for {
                _   <- tx.watch("tx:n")
                _   <- tx.get[String, Int]("tx:n")
                res <- tx.exec(
                         (
                           Commands.incr("tx:n"),
                           Commands.incrBy("tx:n", 4)
                         ).pipeline
                       )
              } yield res
            }
} yield result
```

:::

The distinction is covered in [Pipelines & transactions](/pipelines-transactions).

### Pub/Sub

Subscribing yields a stream of messages in your ecosystem's native stream type: an Ox `Flow`, a ZIO `ZStream`, an fs2 `Stream`, or a Kyo `Stream`. Ending the stream unsubscribes.

::: code-group

```scala [Ox]
val collector =
  fork(client.subscribe[String]("news").take(3).runToList())
(1 to 3).foreach(i => client.publish("news", s"item-$i"))
val messages = collector.join() // the three published payloads
```

```scala [ZIO]
for {
  sub      <- client.subscribe[String]("news").take(3).runCollect.fork
  _        <- ZIO.sleep(300.millis)
  _        <- ZIO.foreachDiscard(1 to 3) { i =>
                client.publish("news", s"item-$i")
              }
  messages <- sub.join
} yield messages.map(_.payload).toList
```

```scala [Cats Effect]
for {
  sub      <- client.subscribe[String]("news").take(3).compile.toVector.start
  _        <- IO.sleep(300.millis)
  _        <- (1 to 3).toList.traverse_ { i =>
                client.publish("news", s"item-$i")
              }
  messages <- sub.joinWithNever
} yield messages.map(_.payload).toList
```

```scala [Kyo]
for {
  publisher <- Fiber.init {
                 for {
                   _ <- Async.sleep(300.millis)
                   _ <- Kyo.foreachDiscard(1 to 3) { i =>
                          client.publish("news", s"item-$i")
                        }
                 } yield ()
               }
  chunk     <- client.subscribe[String]("news").take(3).run
  _         <- publisher.get
} yield chunk.toList.map(_.payload)
```

:::

Classic and sharded pub/sub are both covered in [Pub/Sub](/pubsub).

### Cached reads

Opt a read into client-side caching per call. The first read fetches and caches; the second is served locally until a server invalidation or the TTL evicts it.

::: code-group

```scala [Ox]
client.set("cached:key", "v1")
// first fetches and caches; second is a local hit
val v1 = client.cached(Commands.get[String, String]("cached:key"), 1.minute)
val v2 = client.cached(Commands.get[String, String]("cached:key"), 1.minute)
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _  <- client.set("cached:key", "v1")
  v1 <- client.cached(Commands.get[String, String]("cached:key"), 1.minute)
  v2 <- client.cached(Commands.get[String, String]("cached:key"), 1.minute)
} yield (v1, v2)
```

:::

More in [Client-side caching](/client-side-caching).

## Next steps

- [Commands & codecs](/commands) for the command surface and typing your own values
- [Pipelines & transactions](/pipelines-transactions) for batching and atomicity
- [Pub/Sub](/pubsub) for classic and sharded messaging
- [Streams](/streams) for append-only logs and consumer groups
- [Client-side caching](/client-side-caching) for cached reads and invalidation
- [Configuration](/configuration) for cluster, master-replica, read routing, and TLS
- [Error handling](/error-handling) and [Observability](/observability)

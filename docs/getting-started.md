# Getting started

**Sage** is a native [Redis](https://redis.io) and [Valkey](https://valkey.io) client for [Scala 3](https://www.scala-lang.org/). There is no Java client wrapped underneath: the RESP3 protocol, the commands, and the codecs are implemented directly in Scala, on a zero-dependency, effect-free core.

That core is paired with a runtime written once and cross-published for [Ox](https://ox.softwaremill.com), [ZIO](https://zio.dev), [Cats Effect](https://typelevel.org/cats-effect/), [Kyo](https://getkyo.io), and [Apache Pekko](https://pekko.apache.org), so you use sage with your ecosystem's native types and no wrapper in sight. It targets RESP3 and modern Redis 8+ / Valkey 8+, runs on Scala 3.3.x LTS and later, and requires JDK 21+.

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

```scala [Pekko]
"com.github.ghostdogpr" %% "sage-client-pekko" % "@VERSION@"
// Pekko also needs Pekko Streams and the typed actor module; you provide the ActorSystem
"org.apache.pekko" %% "pekko-stream" % "1.6.0"
"org.apache.pekko" %% "pekko-actor-typed" % "1.6.0"
```

:::

Two imports cover everything: `import sage.*` for the command vocabulary and connection config, and `import sage.backend.*` for the client. That second import is the same regardless of effect system; only the dependency you choose differs.

## Your first connection

A `SageClient` owns all connections to one server or cluster. You build it from a `SageConfig` using your ecosystem's idiomatic construction form: a scoped resource for Ox and Kyo, a `ZLayer` for ZIO, a `Resource` for Cats Effect, and a `Future` on Pekko that registers a `CoordinatedShutdown` task to close the client when your `ActorSystem` terminates. The command surface is identical across all five; only this wiring differs.

::: code-group

```scala [Ox]
import ox.supervised

import sage.*
import sage.backend.*

@main def main(): Unit =
  supervised {
    val config = SageConfig(
      topology = Topology.Standalone(Endpoint("localhost", 6379))
    )
    val client   = SageClient.scoped(config)
    client.set("greeting", "hello")
    val greeting = client.get[String]("greeting")
    println(s"greeting=$greeting") // Some("hello")
  }
```

```scala [ZIO]
import zio.*

import sage.*
import sage.backend.*

object Main extends ZIOAppDefault {
  val config = SageConfig(
    topology = Topology.Standalone(Endpoint("localhost", 6379))
  )

  def run =
    ZIO.serviceWithZIO[SageClient] { client =>
      for {
        _        <- client.set("greeting", "hello")
        greeting <- client.get[String]("greeting")
      } yield greeting
    }.provide(SageClient.layer(config))
}
```

```scala [Cats Effect]
import cats.effect.{IO, IOApp}

import sage.*
import sage.backend.*

object Main extends IOApp.Simple {
  val config = SageConfig(
    topology = Topology.Standalone(Endpoint("localhost", 6379))
  )

  def run: IO[Unit] =
    SageClient.resource(config).use { client =>
      for {
        _        <- client.set("greeting", "hello")
        greeting <- client.get[String]("greeting")
      } yield ()
    }
}
```

```scala [Kyo]
import kyo.*

import sage.*
import sage.backend.*

object Main extends KyoApp {
  val config = SageConfig(
    topology = Topology.Standalone(Endpoint("localhost", 6379))
  )

  run {
    Scope.run {
      for {
        client   <- SageClient.scoped(config)
        _        <- client.set("greeting", "hello")
        greeting <- client.get[String]("greeting")
      } yield greeting
    }
  }
}
```

```scala [Pekko]
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.concurrent.{ExecutionContext, Future}

import sage.*
import sage.backend.*

@main def main(): Unit = {
  // you own the ActorSystem; the client closes when it terminates
  given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "sage")
  given ExecutionContext             = system.executionContext

  val config = SageConfig(
    topology = Topology.Standalone(Endpoint("localhost", 6379))
  )

  val done =
    for {
      client   <- SageClient.connect(config)
      _        <- client.set("greeting", "hello")
      greeting <- client.get[String]("greeting")
    } yield println(s"greeting=$greeting") // Some("hello")

  done.onComplete(_ => system.terminate())
}
```

:::

::: tip How it works
A `SageClient` is not a single connection, and ordinary commands do not borrow from a pool. They are written to one **multiplexed connection** per node, shared by every fiber: sage **auto-pipelines** them, coalescing concurrent commands into fewer socket writes and matching replies back in FIFO order. This is transparent (you never assemble a pipeline to get it), and it is what keeps throughput high under concurrency.

Two cases step off that connection automatically. Commands that hold per-connection state or block (`WATCH`/`MULTI`/`EXEC`, `BLPOP`, and the like) lease a **dedicated connection** from an on-demand pool for the duration. Pub/sub subscriptions share a separate **subscription connection**, created the first time you subscribe, so a slow consumer can never stall command replies.
:::

## A short tour

The snippets below show the same operations on each backend: pick your tab. In Ox they return values directly; in ZIO, Cats Effect, Kyo, and Pekko they are steps in a for-comprehension over that ecosystem's effect type, where on Pekko that type is `scala.concurrent.Future` (each `for` needs an `ExecutionContext`, for example `system.executionContext`). Each assumes a `client` in scope and the usual imports for your effect type (plus `import scala.concurrent.duration.*` where a duration appears).

### Commands

Methods are named one-for-one with Redis commands, grouped by family (strings, hashes, lists, sets, sorted sets, and so on). Keys and values are typed: the key type is fixed on the client (String by default), so a read only names its value type. Any type with a `ValueCodec` (here, a `User`) rides over the wire the same way as a `String`.

::: code-group

```scala [Ox]
client.set("greeting", "hello")
val greeting = client.get[String]("greeting") // Some("hello")
client.incrBy("counter", 10)

client.hSet("user:1", ("name", "Ada"), ("age", "36"))
val profile = client.hGetAll[String, String]("user:1")
// Map("name" -> "Ada", "age" -> "36")

client.set("user:ada", User("Ada", 36))
val ada = client.get[User]("user:ada") // Some(User("Ada", 36))
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _        <- client.set("greeting", "hello")
  greeting <- client.get[String]("greeting")
  _        <- client.incrBy("counter", 10)
  _        <- client.hSet("user:1", ("name", "Ada"), ("age", "36"))
  profile  <- client.hGetAll[String, String]("user:1")
  _        <- client.set("user:ada", User("Ada", 36))
  ada      <- client.get[User]("user:ada")
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
  )
)
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _     <- client.set("pipe:a", "x")
  _     <- client.set("pipe:n", 10)
  tuple <- client.pipeline(
             (
               Commands.get[String, String]("pipe:a"),
               Commands.incrBy("pipe:n", 5)
             )
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
  tx.get[Int]("tx:n")
  tx.exec(
    (Commands.incr("tx:n"), Commands.incrBy("tx:n", 4))
  )
}
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _      <- client.set("tx:n", 1)
  result <- client.transaction { tx =>
              for {
                _   <- tx.watch("tx:n")
                _   <- tx.get[Int]("tx:n")
                res <- tx.exec(
                         (
                           Commands.incr("tx:n"),
                           Commands.incrBy("tx:n", 4)
                         )
                       )
              } yield res
            }
} yield result
```

:::

The distinction is covered in [Pipelines & transactions](/pipelines-transactions).

### Pub/Sub

Subscribing yields a stream of messages in your ecosystem's native stream type: an Ox `Flow`, a ZIO `ZStream`, an fs2 `Stream`, a Kyo `Stream`, or a Pekko Streams `Source`. Ending the stream, or closing its scope, unsubscribes.

Because publishing here happens right after subscribing, these examples use the variant that returns only once the server has confirmed the subscription, so the publish can't outrun the registration: `subscribeScoped` on ZIO and Kyo, `subscribeResource` on Cats Effect, and plain `subscribe` on Ox (where the call is already synchronous). On Pekko, `subscribe` returns a `Source` whose materialized value is a `Future[Done]` that completes once the subscription is registered, so awaiting it before publishing closes the same gap on a standalone or master-replica server (in cluster mode that confirmation is best-effort, as on every backend). The plain stream-returning `subscribe` registers lazily on first pull and is the right choice for a long-lived consumer that isn't racing its own publisher.

::: code-group

```scala [Ox]
val news = client.subscribe[String]("news")
(1 to 3).foreach(i => client.publish("news", s"item-$i"))
val messages = news.take(3).runToList()
```

```scala [ZIO]
ZIO.scoped {
  for {
    stream   <- client.subscribeScoped[String]("news")
    _        <- ZIO.foreachDiscard(1 to 3) { i =>
                  client.publish("news", s"item-$i")
                }
    messages <- stream.take(3).runCollect
  } yield messages.map(_.payload).toList
}
```

```scala [Cats Effect]
client.subscribeResource[String]("news").use { stream =>
  for {
    _        <- (1 to 3).toList.traverse_ { i =>
                  client.publish("news", s"item-$i")
                }
    messages <- stream.take(3).compile.toVector
  } yield messages.map(_.payload).toList
}
```

```scala [Kyo]
for {
  stream <- client.subscribeScoped[String]("news")
  _      <- Kyo.foreachDiscard(1 to 3) { i =>
              client.publish("news", s"item-$i")
            }
  chunk  <- stream.take(3).run
} yield chunk.toList.map(_.payload)
```

```scala [Pekko]
// Keep.both keeps the confirmation Future[Done] and the collected messages;
// await it before publishing so the publish can't outrun the registration (best-effort in cluster)
val (confirmed, collected) =
  client.subscribe[String]("news").take(3).toMat(Sink.seq)(Keep.both).run()
val messages =
  for {
    _        <- confirmed
    _        <- Future.traverse(1 to 3)(i => client.publish("news", s"item-$i"))
    received <- collected
  } yield received.map(_.payload).toList
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

```scala [ZIO · Cats Effect · Kyo · Pekko]
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

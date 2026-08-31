# Getting started

Sage is a [Redis](https://redis.io) and [Valkey](https://valkey.io) client for [Scala 3](https://www.scala-lang.org/). It implements RESP3, commands, and codecs directly in Scala. The core has no dependencies on an effect system.

Sage provides integrations for [Ox](https://ox.softwaremill.com), [ZIO](https://zio.dev), [Cats Effect](https://typelevel.org/cats-effect/), [Kyo](https://getkyo.io), and [Apache Pekko](https://pekko.apache.org). Each integration uses its ecosystem's native types. Sage targets Redis 8+ and Valkey 8+. It supports Scala 3.3.x LTS and later and requires JDK 21 or later.

## Installation

Add the artifact for your Scala stack. The core is pulled in transitively, so you depend on one module only.

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
```

:::

Import `sage.*` for commands and connection configuration. Import `sage.backend.*` for the client. Every backend uses these imports, but each one has a different dependency.

## Your first connection

A `SageClient` owns all connections to one server or cluster. Build it from a `SageConfig` with the resource type for your backend. Ox and Kyo use a scope, ZIO uses a `ZLayer`, Cats Effect uses a `Resource`, and Pekko uses a `use` block that closes the client when the program finishes. All five integrations provide the same commands.

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
  given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "sage")
  given ExecutionContext             = system.executionContext

  val config = SageConfig(
    topology = Topology.Standalone(Endpoint("localhost", 6379))
  )

  val done =
    SageClient.use(config) { client =>
      for {
        _        <- client.set("greeting", "hello")
        greeting <- client.get[String]("greeting")
      } yield println(s"greeting=$greeting") // Some("hello")
    }

  done.onComplete(_ => system.terminate())
}
```

:::

::: tip How it works
Ordinary commands share one **auto-pipelined connection** per node. Sage can group concurrent commands into fewer network writes and returns each reply to the correct caller. You do not need to build a pipeline yourself.

Transactions and blocking commands such as `WATCH`, `MULTI`, `EXEC`, and `BLPOP` borrow a dedicated connection from a pool. Pub/sub subscriptions use a separate subscription connection, which Sage creates on the first subscription. A slow subscriber therefore does not delay command replies.
:::

## A short tour

Choose your backend tab in the examples below. Ox returns values directly. The other backends use a for-comprehension over their effect type. Pekko uses `Future`, so each `for` needs an `ExecutionContext`. Every example assumes that `client` and the imports for the effect type are in scope.

### Commands

Method names match Redis commands and are grouped by family (strings, hashes, lists, sets, sorted sets, and so on). Keys and values are typed. The client uses `String` keys by default. For a read, specify the type of value you expect, as in `get[String]`. You can read and write your own types, such as the `User` below, by defining a `ValueCodec` for them.

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

Put commands in a pipeline to send them in one round trip and receive a typed tuple of results.

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

A transaction runs a pipeline atomically with `MULTI` and `EXEC`. You can use `WATCH` for optimistic concurrency. If a watched key changes before `EXEC`, the transaction returns `None`. You can then retry the transaction.

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

Subscribing returns the backend's native stream type. Ox returns `Flow`, ZIO returns `ZStream`, Cats Effect returns an fs2 `Stream`, Kyo returns `Stream`, and Pekko returns `Source`. Ending the stream or closing its scope unsubscribes.

These examples publish immediately after subscribing, so they use the variant that waits for the server to confirm the subscription first. [Pub/Sub](/pubsub) explains when you need it.

::: code-group

```scala [Ox]
val news = client.subscribeScoped[String]("news")
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
// Keep.both gives both the confirmation Future[Done] and the collected messages
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

See [Client-side caching](/client-side-caching) for invalidation and cache limits.

## Next steps

- [Commands & codecs](/commands) for available commands and custom value types
- [Pipelines & transactions](/pipelines-transactions) for batching and atomicity
- [Pub/Sub](/pubsub) for classic and sharded messaging
- [Streams](/streams) for append-only logs and consumer groups
- [Client-side caching](/client-side-caching) for cached reads and invalidation
- [Configuration](/configuration) for cluster, master-replica, read routing, and TLS
- [Error handling](/error-handling) and [Observability](/observability)

# Testing

Sage includes an in-memory client for tests that should not need a Redis server. It has the same `SageClient` type as the networked client, so code that takes a `SageClient` can be tested with it unchanged. Commands run against an in-memory keyspace instead of a socket: each command is executed from its name and arguments, and its reply goes through the same decoder as a server's reply, so results and errors have the same types.

## Installation

Add the testkit for your backend to the test scope.

::: code-group

```scala [Direct]
"com.github.ghostdogpr" %% "sage-testkit-direct" % "@VERSION@" % Test
```

```scala [Ox]
"com.github.ghostdogpr" %% "sage-testkit-ox" % "@VERSION@" % Test
```

```scala [ZIO]
"com.github.ghostdogpr" %% "sage-testkit-zio" % "@VERSION@" % Test
```

```scala [Cats Effect]
"com.github.ghostdogpr" %% "sage-testkit-ce" % "@VERSION@" % Test
```

```scala [Kyo]
"com.github.ghostdogpr" %% "sage-testkit-kyo" % "@VERSION@" % Test
```

```scala [Pekko]
"com.github.ghostdogpr" %% "sage-testkit-pekko" % "@VERSION@" % Test
```

:::

## Creating a client

`InMemorySageClient()` returns a client with its own empty keyspace. Creating it needs no connection, so it returns the client directly on every backend. `close` makes later commands fail with `NotConnected`.

::: code-group

```scala [Direct]
import sage.*
import sage.testkit.InMemorySageClient

val client = InMemorySageClient()
client.set("greeting", "hello")
client.get[String]("greeting") // Some("hello")
```

```scala [ZIO]
import sage.*
import sage.testkit.InMemorySageClient

val client = InMemorySageClient()
for {
  _        <- client.set("greeting", "hello")
  greeting <- client.get[String]("greeting") // Some("hello")
} yield greeting
```

```scala [Cats Effect]
import sage.*
import sage.testkit.InMemorySageClient

val client = InMemorySageClient()
for {
  _        <- client.set("greeting", "hello")
  greeting <- client.get[String]("greeting") // Some("hello")
} yield greeting
```

:::

The other backends work the same way: Ox code calls the client inside a `supervised` scope, Kyo code composes its effects, and Pekko code composes its `Future`s.

## Sharing a keyspace between clients

An `InMemoryServer` is one keyspace that several clients can use, like several connections to the same Redis server. Clients of the same server see each other's writes, receive each other's pub/sub messages, and wake each other's blocking commands. A lock held through one client excludes the others.

```scala
import sage.testkit.{InMemoryServer, InMemorySageClient}

val server     = InMemoryServer()
val publisher  = InMemorySageClient(server)
val subscriber = InMemorySageClient(server)
```

## Controlling time

Key expiry and `TIME` read the clock of the backend's effect system. A test clock of that effect system therefore controls them:

- ZIO: `TestClock.adjust` from zio-test.
- Cats Effect: `TestControl` from cats-effect-testkit, which advances its virtual time when every fiber is sleeping.
- Kyo: `Clock.withTimeControl`.

```scala
// ZIO, in a ZIOSpecDefault. SetExpiry takes a Scala duration, and TestClock.adjust takes a ZIO duration.
test("a session expires") {
  val client = InMemorySageClient()
  for {
    _     <- client.set("session", "token", expiry = SetExpiry.In(scala.concurrent.duration.Duration(10, "seconds")))
    _     <- TestClock.adjust(zio.Duration.fromSeconds(10))
    after <- client.get[String]("session")
  } yield assertTrue(after.isEmpty)
}
```

The direct client, Ox and Pekko read the system clock. For them, create the server with a `TestClock`, which moves only when the test advances it. Every client of that server reads time from the clock. A `TestClock` also works with the other backends.

```scala
import sage.testkit.{InMemoryServer, InMemorySageClient, TestClock}

val clock  = TestClock()
val client = InMemorySageClient(InMemoryServer(clock))
client.set("session", "token", expiry = SetExpiry.In(10.seconds))
clock.advance(10.seconds)
client.get[String]("session") // None
```

The timeout of a blocking command such as `BLPOP` is measured by the backend's own timer, so it follows the effect system's test clock in the same way. Advancing a `sage.testkit.TestClock` does not shorten it.

## What is supported

The in-memory client supports the common commands of strings, keys with expiry, hashes, lists, sets and sorted sets, including blocking pops such as `BLPOP`. It also supports pipelines, transactions with `WATCH`, and pub/sub with `PUBLISH` and subscriptions to channels and patterns.

On top of these commands, it supports Sage's own features: `cached` reads, locks, rate limiters created with `client.rateLimiter`, and the paging helpers `scanAll`, `hScanAll`, `sScanAll` and `zScanAll`.

Streams, bitmaps, HyperLogLog, geo, JSON, Lua scripts and shard channels are not supported, so code that uses them needs a real server. Any other command or option that the client does not support fails with an `ERR` error that names it.

## Differences from a server

- Errors have the same code as a server's errors, such as `ERR` or `WRONGTYPE`, but their messages can be worded differently.
- The client behaves like one standalone server that never disconnects. Use a real server to test clusters, reconnection or keyspace notifications.
- A lock has no lease. It stays held until the code it protects ends, so a holder that stalls never loses it.
- The client takes no `SageConfig`, so listeners, tracers, timeouts and pool settings do not apply.

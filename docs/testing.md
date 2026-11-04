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

Key expiry, hash field expiry and `TIME` read the clock of the backend's effect system. A test clock of that effect system therefore controls them:

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

The direct client, Ox and Pekko read the system clock. For them, pass a `TestClock.Manual`, which moves only when the test advances it. A manual clock also works with the other backends, and an `InMemoryServer` accepts one for all its clients.

```scala
import sage.testkit.{InMemorySageClient, TestClock}

val clock  = TestClock.manual()
val client = InMemorySageClient(clock)
client.set("session", "token", expiry = SetExpiry.In(10.seconds))
clock.advance(10.seconds)
client.get[String]("session") // None
```

The timeout of a blocking command such as `BLPOP` is measured by the backend's own timer, so it follows the effect system's test clock in the same way. A manual clock does not shorten it.

## What is supported

The in-memory client implements:

- String, key, hash (including field expiry), list, set and sorted-set commands, with `WRONGTYPE` errors and expiry.
- Bitmaps, HyperLogLog and geo commands.
- Streams, including consumer groups, pending entries, claiming and `XINFO`.
- Blocking commands such as `BLPOP`, `BLMOVE`, `BZPOPMIN`, `XREAD BLOCK` and `XREADGROUP BLOCK`.
- Pipelines and transactions, including `WATCH`.
- Pub/sub with channels, patterns and shard channels, and the `PUBSUB` introspection commands.
- Distributed locks and rate limiters.
- `SORT`, `LCS`, `SCAN` and its variants, `KEYS`, `DBSIZE`, `FLUSHALL`, `PING`, `ECHO` and `TIME`.

## What is not supported

A command that the client does not implement fails with a `ServerError` whose code is `ERR` and whose message names the command. A transaction that contains one is discarded before anything runs, as a server discards a transaction with an unknown command. The following commands are not implemented:

- JSON commands (`JSON.SET`, `JSON.GET` and the others).
- Redis array commands (`ARSET`, `ARGET` and the others).
- Lua scripts other than those of Sage's locks and rate limiters, and functions (`FUNCTION`, `FCALL`, `FCALL_RO`).
- `DIGEST`, and the `IFDEQ` and `IFDNE` conditions of `DELEX`, which need the server's XXH3 digest. `DELEX` with `IFEQ` or `IFNE` works.
- `MOVE`, because the client has a single database. The `database` setting of `SageConfig` does not apply to it.
- `DUMP`, `RESTORE` and `MIGRATE`, which use the server's serialization format.
- `OBJECT`, `WAIT` and `WAITAOF`.
- Server administration and introspection: `ACL`, `CLIENT`, `CLUSTER`, `COMMAND`, `COMMANDLOG`, `CONFIG`, `INFO`, `LATENCY`, `MEMORY`, `ROLE` and `SLOWLOG`.

## Differences from a server

The in-memory client runs the integration tests that Sage runs against Redis for the features above. These behaviors differ:

- `PFCOUNT` counts the elements exactly. A server estimates the cardinality with a small error, so both give the same answer for small sets, but for large sets a server's answer can be off by about 1%.
- Lua scripts run only when they are the scripts of Sage's locks and rate limiters, which the client recognizes by their SHA-1 digest and runs as Scala code.
- `cached` reads always read the keyspace. The client keeps no local cache, so the results are the same as with a networked client whose cache is always up to date.
- The client takes no `SageConfig`, so listeners and tracers receive no events, and settings such as timeouts and pool sizes do not apply.
- The client behaves like one standalone server. Commands that would fail in a cluster because their keys belong to different slots (`CROSSSLOT`) succeed, so tests of cluster key placement need a real cluster.
- Keyspace notifications are not published.
- Expiry is checked when a command reads a key, so an expired key disappears at the first read after its expiry time. A server also removes expired keys in the background, which is only visible through memory usage and keyspace notifications.
- Approximate stream trimming (`~`) removes entries only in whole blocks of 100, the default node size of a server's streams. A server's blocks depend on how the entries were added, so the number of entries it keeps can differ, although a short stream is not trimmed by either.
- `XCFGSET` accepts its settings and has no effect, because Sage's `XADD` has no idempotency options to use them.
- `INCREX` and `XNACK` are recent Redis commands. Their main cases are tested against Redis, and their edge cases follow the Redis documentation.
- The client never loses its connection, so tests of reconnection and failover need a real server.

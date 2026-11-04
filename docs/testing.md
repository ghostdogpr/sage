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

The in-memory client implements:

- String, key, hash, list, set and sorted-set commands, with key expiry and `WRONGTYPE` errors, including blocking commands such as `BLPOP`, `BLMOVE` and `BZPOPMIN`.
- Streams: `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XDEL`, `XTRIM`, `XSETID` and `XREAD`, consumer groups with `XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM` and `XAUTOCLAIM`, and `XINFO`.
- Pub/sub with channels, patterns and shard channels, and the `PUBSUB` introspection commands.
- Pipelines and transactions, including `WATCH`.
- Distributed locks and rate limiters. `EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS` and `SCRIPT FLUSH` work for their scripts.
- `SCAN` and its variants, `KEYS`, `DBSIZE`, `FLUSHALL`, `FLUSHDB`, `PING`, `ECHO` and `TIME`.

Sage's own features therefore work with it: `cached` reads, subscriptions, transactions, locks, rate limiters, and the paging helpers such as `scanAll`, `xRangeAll`, `xTail`, `xConsume` and `xAutoClaimAll`.

## What is not supported

A command that the client does not implement fails with a `ServerError` whose code is `ERR` and whose message names the command. A transaction that contains one is discarded before anything runs, as a server discards a transaction with an unknown command. An option that the client does not implement fails with an `ERR` error that names the option and the command, but only when its command runs. A Lua script that the client cannot run also fails with an `ERR` error when its `EVAL` or `EVALSHA` runs, so the rest of its transaction still runs.

These command families are not implemented:

- Bitmaps (`SETBIT`, `GETBIT`, `BITCOUNT`, `BITOP`, `BITFIELD` and the others), HyperLogLog and geo commands.
- Hash field expiry (`HEXPIRE`, `HTTL`, `HPERSIST`, `HGETEX`, `HSETEX` and the others).
- Lua scripts other than those of Sage's locks and rate limiters, `EVAL_RO`, `EVALSHA_RO`, and functions (`FUNCTION`, `FCALL`, `FCALL_RO`).
- JSON commands and Redis array commands.
- Server administration and introspection, such as `ACL`, `CLIENT`, `CLUSTER`, `COMMAND`, `CONFIG`, `INFO`, `OBJECT`, `SLOWLOG` and `WAIT`, and the commands that copy data between servers or databases: `DUMP`, `RESTORE`, `MIGRATE` and `MOVE`.

Within the implemented families, these commands and options are not implemented:

- `LCS`, `SORT`, `SORT_RO`, `INCREX`, `DELEX`, `DELIFEQ` and `DIGEST`, and the `IFEQ`, `IFNE`, `IFDEQ` and `IFDNE` conditions of `SET`.
- `XDELEX`, `XACKDEL`, `XNACK`, `XCFGSET` and `XINFO STREAM FULL`, and these stream options: `KEEPREF`, `DELREF` and `ACKED` of `XADD` and `XTRIM`, `ENTRIESREAD` of `XGROUP`, `ENTRIESADDED` and `MAXDELETEDID` of `XSETID`, `FORCE` and `LASTID` of `XCLAIM`, and `CLAIM` of `XREADGROUP`.
- `SCRIPT KILL`, `SCRIPT SHOW`, `SCRIPT DEBUG` and the `HELP` subcommands, such as `XINFO HELP`.

## Differences from a server

The in-memory client runs the integration tests that Sage runs against Redis for the features above. These behaviors differ:

- An error has the same code as a server's error, such as `ERR`, `WRONGTYPE`, `NOGROUP` or `BUSYGROUP`, but its message can be worded differently. When a command has several invalid arguments, the client can report a different one than a server.
- Lua scripts run only when they are the scripts of Sage's locks and rate limiters, which the client recognizes by their SHA-1 digest and runs as Scala code. The rate limiter's arguments must be decimal integers, as Sage sends them, where a server's Lua interpreter also accepts other number formats such as `0x10` or `1e3`.
- `cached` reads always read the keyspace. The client keeps no local cache, so the results are the same as with a networked client whose cache is always up to date.
- The client takes no `SageConfig`, so listeners and tracers receive no events, and settings such as timeouts and pool sizes do not apply.
- The client behaves like one standalone server. Commands that would fail in a cluster because their keys belong to different slots (`CROSSSLOT`) succeed, so tests of cluster key placement need a real cluster.
- Keyspace notifications are not published.
- Expiry is checked when a command reads a key, so an expired key disappears at the first read after its expiry time. A server also removes expired keys in the background, which is only visible through memory usage and keyspace notifications.
- Approximate stream trimming (`~`) trims exactly, and removes at most `LIMIT` entries when `LIMIT` is given. A server trims whole blocks of entries, so it can keep more entries than the threshold.
- `XINFO GROUPS` reports `entries-read` as null, as a server does when it cannot tell, and reports as `lag` the number of entries after the group's last delivered id.
- `XINFO STREAM` omits `radix-tree-keys` and `radix-tree-nodes`, which describe a server's memory layout, so Sage reports them as 0.
- The client never loses its connection, so tests of reconnection and failover need a real server.

# Streams

A stream is an append-only log. Each entry has an ID (a millisecond timestamp plus a sequence number) and an ordered list of field/value pairs, where a field may repeat. You read a stream by range or by tailing it, and several workers can share one cooperatively through a consumer group.

## Appending and reading

`xAdd` appends an entry and returns its ID; by default the server assigns the ID. `xRange` reads entries back in ID order, each as a `StreamEntry` whose `fields` is a `Vector` (order preserved, repeats allowed).

::: code-group

```scala [Ox]
client.del("stream:orders")
client.xAdd("stream:orders")(("item", "book"), ("qty", "2"))
client.xAdd("stream:orders")(("item", "pen"), ("qty", "5"))
val len     = client.xLen("stream:orders") // 2
val entries = client.xRange[String, String]("stream:orders")
// Vector(StreamEntry(id, Vector(("item","book"), ("qty","2"))), ...)
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _       <- client.del("stream:orders")
  _       <- client.xAdd("stream:orders")(("item", "book"), ("qty", "2"))
  _       <- client.xAdd("stream:orders")(("item", "pen"), ("qty", "5"))
  len     <- client.xLen("stream:orders")
  entries <- client.xRange[String, String]("stream:orders")
} yield (len, entries)
```

:::

Field and value types are codec-driven, exactly like [other commands](/commands): the two type parameters above name the field type and the value type.

## Consumer groups

A consumer group lets several consumers split a stream's entries between them without overlap. The group tracks a last-delivered ID and a pending entries list (PEL) of entries delivered but not yet acknowledged. `xReadGroup` with `GroupReadId.New` (the `>` token) delivers never-seen entries and records them as pending; `xAck` removes them from the PEL once handled.

::: code-group

```scala [Ox]
// create the group reading from the start of the stream
client.xGroupCreate(
  "stream:orders",
  "workers",
  id = GroupStartId.At(StreamId.Zero)
)
val batches = client.xReadGroup[String, String]("workers", "w1")(
  ("stream:orders", GroupReadId.New)
)()
val ids = batches.flatMap(_._2).map(_.id)
client.xAck("stream:orders", "workers")(ids.head, ids.tail*)
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _       <- client.xGroupCreate(
               "stream:orders",
               "workers",
               id = GroupStartId.At(StreamId.Zero)
             )
  batches <- client.xReadGroup[String, String]("workers", "w1")(
               ("stream:orders", GroupReadId.New)
             )()
  ids      = batches.flatMap(_._2).map(_.id)
  _       <- client.xAck("stream:orders", "workers")(ids.head, ids.tail*)
} yield ids
```

:::

Each command uses a separate type for the ID values it accepts. `XADD` takes an `XAddId` (`Auto` by default), `XREADGROUP` takes a `GroupReadId` (`New` or `After(id)`), `XGROUP CREATE` takes a `GroupStartId` (`Last` or `At(id)`), and range commands take a `StreamRangeId`. This prevents you from passing an unsupported ID value.

## Tailing a group

For a long-running worker, `xConsume` tails a group and runs your handler on each entry. It first replays this consumer's own pending entries (recovering whatever a previous run left unacknowledged), then blocks waiting for new ones. An entry is acknowledged only once the handler succeeds, so a failure leaves it in the PEL for another attempt.

On Pekko, where a `Future` cannot be cancelled, the loop runs in the background and `xConsume` returns a `RunningConsumer`: call `stop()` to halt it between entries and await its `completion`.

::: tip At-least-once delivery
The same entry can be delivered again after a crash or a failed handler, so make your handler idempotent. `xConsume` blocks while waiting for entries and is intended for a long-running worker rather than a one-time read.
:::

::: code-group

```scala [Ox]
// runs inside a `supervised` scope; tails new entries forever
client.xConsume[String, String]("workers", "w1", "stream:orders") {
  entry => println(s"got ${entry.id}: ${entry.fields}")
}
```

```scala [ZIO]
client.xConsume[String, String]("workers", "w1", "stream:orders") {
  entry => Console.printLine(s"got ${entry.id}: ${entry.fields}")
}
```

```scala [Cats Effect]
client.xConsume[String, String]("workers", "w1", "stream:orders") {
  entry => IO.println(s"got ${entry.id}: ${entry.fields}")
}
```

```scala [Kyo]
client.xConsume[String, String]("workers", "w1", "stream:orders") {
  entry => Console.printLine(s"got ${entry.id}: ${entry.fields}")
}
```

```scala [Pekko]
// a Future has no interruption, so the loop is returned as a RunningConsumer:
// the handler returns Future[Unit], and you call stop() to halt it and await completion
val consumer = client.xConsume[String, String]("workers", "w1", "stream:orders") {
  entry => Future(println(s"got ${entry.id}: ${entry.fields}"))
}
// later: consumer.stop() // Future[Done], resolves once the loop has drained
```

:::

The remaining `X*` commands are also available: trimming (`xTrim`), reverse range (`xRevRange`), blocking reads (`xRead`), claim and auto-claim (`xClaim`, `xAutoClaim`), pending inspection (`xPending`), and group management. See the [API docs](https://javadoc.io/doc/com.github.ghostdogpr/sage-core_3/) for the complete list.

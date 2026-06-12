# Streams

A Stream is an append-only log. Each entry has a Stream Entry ID (a millisecond timestamp and a per-millisecond sequence) and an ordered, duplicate-permitting list of field/value pairs. You read a Stream by range or by tailing, and you consume it cooperatively through Consumer Groups.

## Appending and reading

`xAdd` appends an entry and returns its ID; by default the server assigns the ID. `xRange` reads entries back in ID order, each as a `StreamEntry` whose `fields` is a `Vector` (order preserved, repeats allowed).

::: code-group

```scala [Ox]
client.del("stream:orders")
client.xAdd("stream:orders")(("item", "book"), ("qty", "2"))
client.xAdd("stream:orders")(("item", "pen"), ("qty", "5"))
val len     = client.xLen("stream:orders") // 2
val entries = client.xRange[String, String, String]("stream:orders")
// Vector(StreamEntry(id, Vector(("item","book"), ("qty","2"))), ...)
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _       <- client.del("stream:orders")
  _       <- client.xAdd("stream:orders")(("item", "book"), ("qty", "2"))
  _       <- client.xAdd("stream:orders")(("item", "pen"), ("qty", "5"))
  len     <- client.xLen("stream:orders")
  entries <- client.xRange[String, String, String]("stream:orders")
} yield (len, entries)
```

:::

Field and value types are codec-driven, exactly like [other commands](/commands): `xRange[K, F, V]` here decodes string fields and string values.

## Consumer groups

A Consumer Group lets several consumers split a Stream's entries between them without overlap. The group tracks a last-delivered ID and a Pending Entries List (PEL) of entries delivered but not yet acknowledged. `xReadGroup` with `GroupReadId.New` (the `>` token) delivers never-seen entries and records them as pending; `xAck` removes them from the PEL once handled.

::: code-group

```scala [Ox]
// create the group reading from the start of the stream
client.xGroupCreate(
  "stream:orders",
  "workers",
  id = GroupStartId.At(StreamId.Zero)
)
val batches = client.xReadGroup[String, String, String]("workers", "w1")(
  ("stream:orders", GroupReadId.New)
)()
val ids = batches.flatMap(_._2).map(_.id)
client.xAck("stream:orders", "workers")(ids.head, ids.tail*)
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _       <- client.xGroupCreate(
               "stream:orders",
               "workers",
               id = GroupStartId.At(StreamId.Zero)
             )
  batches <- client.xReadGroup[String, String, String]("workers", "w1")(
               ("stream:orders", GroupReadId.New)
             )()
  ids      = batches.flatMap(_._2).map(_.id)
  _       <- client.xAck("stream:orders", "workers")(ids.head, ids.tail*)
} yield ids
```

:::

Each command position that admits a special ID token carries its own type, so an illegal form cannot be written: `XADD` takes an `XAddId` (`Auto` by default), `XREADGROUP` a `GroupReadId` (`New` or `After(id)`), `XGROUP CREATE` a `GroupStartId` (`Last` or `At(id)`), and the range commands a `StreamRangeId`.

## Tailing a group

For a long-running worker, `xConsume` tails a group as a stream in your ecosystem's native type. It first drains this consumer's own pending history (at-least-once recovery after a restart), then blocks for new entries. Your handler runs per entry, and the entry is acknowledged only after the handler succeeds, so a failure leaves it in the PEL for another attempt.

::: tip At-least-once delivery
Because an entry is acknowledged only after the handler succeeds, the same entry can be delivered again after a crash or a failed handler. Make your handler idempotent. `xConsume` also blocks while tailing, so it is the body of a long-running worker, not a one-shot read.
:::

::: code-group

```scala [Ox]
// runs inside a `supervised` scope; tails new entries forever
client.xConsume[String, String, String]("workers", "w1", "stream:orders") {
  entry => println(s"got ${entry.id}: ${entry.fields}")
}
```

```scala [ZIO]
client.xConsume[String, String, String]("workers", "w1", "stream:orders") {
  entry => Console.printLine(s"got ${entry.id}: ${entry.fields}")
}
```

```scala [Cats Effect]
client.xConsume[String, String, String]("workers", "w1", "stream:orders") {
  entry => IO.println(s"got ${entry.id}: ${entry.fields}")
}
```

```scala [Kyo]
client.xConsume[String, String, String]("workers", "w1", "stream:orders") {
  entry => Console.printLine(s"got ${entry.id}: ${entry.fields}")
}
```

:::

Beyond these, the full `X*` surface is available: trimming (`xTrim`), reverse range (`xRevRange`), blocking reads (`xRead`), claim and auto-claim (`xClaim`, `xAutoClaim`), pending inspection (`xPending`), and group management. See the API docs for the complete list.

# Pub/Sub

Subscribing yields a stream of messages in your ecosystem's native stream type: an Ox `Flow`, a ZIO `ZStream`, an fs2 `Stream`, a Kyo `Stream`, or a Pekko Streams `Source`. Each message carries the channel it arrived on and a payload decoded with a `ValueCodec`. Ending the stream, or closing its scope, unsubscribes.

## Classic channels

`subscribe` listens on one or more channels; `publish` sends to a channel. Here we subscribe, publish three messages, then take them back:

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
// Keep.both keeps the confirmation Future[Done] alongside the collected messages;
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

Pattern subscriptions are also available; they deliver a **pattern message** that additionally names the glob that matched.

::: tip Confirmed subscriptions
The plain `subscribe` returns the stream immediately and registers the subscription on the first pull, so a `publish` sequenced right after it can outrun the registration and be missed. Each backend has a way to wait for the server's SUBSCRIBE confirmation first (also in `p`/`s` forms):

- **ZIO / Kyo**: `subscribeScoped`, a scoped effect.
- **Cats Effect**: `subscribeResource`, a `Resource`.
- **Ox**: plain `subscribe` already blocks until confirmed.
- **Pekko**: plain `subscribe` returns a `Source` whose materialized `Future[Done]` completes once registered; await it before publishing.

The examples above use these. Confirmation closes the race on a standalone or master-replica server; in cluster mode it is best-effort, as on every backend. With the scoped and resource variants that scope owns the unsubscribe, so the subscription outlives the stream and is released when the scope closes; on Pekko, ending the `Source` (cancel, complete, or fail) unsubscribes.
:::

::: tip Connection isolation
All classic subscriptions share one **subscription connection**, created the first time you subscribe and closed when the last subscription ends. It is separate from the connection that carries your commands, so a slow consumer can backpressure its own subscriptions but can never stall command replies. The subscription connection re-issues every active subscription automatically on reconnect.
:::

## Sharded channels (cluster)

In a cluster, a **shard channel** keeps its traffic within the shard that owns the channel's slot: `sSubscribe` and `sPublish` target that owning node rather than broadcasting across the whole cluster. There is no pattern form, and a delivery surfaces as an ordinary message.

```scala [ZIO]
ZIO.scoped {
  for {
    stream   <- client.sSubscribeScoped[String]("orders")
    _        <- client.sPublish("orders", "placed")
    messages <- stream.take(1).runCollect
  } yield messages.map(_.payload).toList
}
```

The shape is the same on every backend, using each one's native stream type exactly as classic pub/sub does. Sage holds one sharded subscription connection per owning node and re-homes the affected subscriptions automatically when a slot migrates or a node fails over.

See [Configuration](/configuration) for how to connect to a cluster.

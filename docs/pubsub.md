# Pub/Sub

Subscribing yields a stream of messages in your ecosystem's native stream type: an Ox `Flow`, a ZIO `ZStream`, an fs2 `Stream`, or a Kyo `Stream`. Each message carries the channel it arrived on and a payload decoded with a `ValueCodec`. Ending the stream unsubscribes.

## Classic channels

`subscribe` listens on one or more channels; `publish` sends to a channel. Here a subscriber takes three messages while a publisher sends them:

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

Pattern subscriptions are also available; they deliver a **pattern message** that additionally names the glob that matched.

::: tip Connection isolation
All classic subscriptions share one **subscription connection**, created the first time you subscribe and closed when the last subscription ends. It is separate from the connection that carries your commands, so a slow consumer can backpressure its own subscriptions but can never stall command replies. The subscription connection re-issues every active subscription automatically on reconnect.
:::

## Sharded channels (cluster)

In a cluster, a **shard channel** keeps its traffic within the shard that owns the channel's slot: `sSubscribe` and `sPublish` target that owning node rather than broadcasting across the whole cluster. There is no pattern form, and a delivery surfaces as an ordinary message.

```scala [ZIO]
for {
  sub      <- client.sSubscribe[String]("orders").take(1).runCollect.fork
  _        <- ZIO.sleep(300.millis)
  _        <- client.sPublish("orders", "placed")
  messages <- sub.join
} yield messages.map(_.payload).toList
```

The shape is the same on every backend, using each one's native stream type exactly as classic pub/sub does. Sage holds one sharded subscription connection per owning node and re-homes the affected subscriptions automatically when a slot migrates or a node fails over.

See [Configuration](/configuration) for how to connect to a cluster.

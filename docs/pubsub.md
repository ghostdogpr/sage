# Pub/Sub

Subscribing returns the backend's native stream type. Ox returns `Flow`, ZIO returns `ZStream`, Cats Effect returns an fs2 `Stream`, Kyo returns `Stream`, Pekko returns `Source`, and the direct client returns a blocking `Iterator` that you close to unsubscribe. Each message contains its channel and a payload decoded by a `ValueCodec`. Ending the stream or closing its scope unsubscribes. If the server refuses a subscribed name, for example with `NOPERM` from an ACL, the stream fails with that `ServerError`, including when a reconnect subscribes the name again. Temporary errors such as `BUSY` or `LOADING` do not end the stream; Sage subscribes again after a backoff.

## Classic channels

`subscribe` listens on one or more channels; `publish` sends to a channel. Here we subscribe, publish three messages, then take them back:

::: code-group

```scala [Ox]
val news = client.subscribeScoped[String]("news")
(1 to 3).foreach(i => client.publish("news", s"item-$i"))
val messages = news.take(3).runToList()
```

```scala [Direct]
val messages = Using.resource(client.subscribe[String]("news")) { news =>
  (1 to 3).foreach(i => client.publish("news", s"item-$i"))
  news.take(3).map(_.payload).toList
}
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
// Wait for confirmation before publishing, or the message may be missed.
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

Pattern subscriptions return a pattern message that includes the matching glob.

::: tip Confirmed subscriptions
The plain `subscribe` returns the stream immediately and registers the subscription when the stream first requests a message. A message published before registration finishes may be missed. Each backend provides a way to wait for the server's SUBSCRIBE confirmation first (also in `p`/`s` forms):

- **ZIO / Kyo**: `subscribeScoped`, a scoped effect.
- **Cats Effect**: `subscribeResource`, a `Resource`.
- **Ox**: `subscribeScoped`, bound to the enclosing Ox scope.
- **Direct**: `subscribe` waits for confirmation before it returns. Close the iterator to unsubscribe, for example with `scala.util.Using`.
- **Pekko**: plain `subscribe` returns a `Source` whose materialized `Future[Done]` completes once registered; await it before publishing.

On a standalone or master-replica server, confirmation guarantees that the subscription is ready. In a cluster, this guarantee is best-effort. With the scoped and resource variants, the subscription remains active until the scope closes. On Pekko, ending the `Source` unsubscribes.
:::

::: tip Connection isolation
All classic subscriptions share one **subscription connection**, created the first time you subscribe and closed when the last subscription ends. Ordinary commands use a different connection. A slow consumer may delay its own subscriptions, but it does not delay command replies. The subscription connection re-issues every active subscription automatically on reconnect.
:::

## Callbacks on the direct client

The direct client can also call a function for each message instead of returning an iterator. `onMessage`, `onPatternMessage`, and `onShardMessage` subscribe, wait for confirmation, and deliver each message in order on a virtual thread that Sage starts. They return an `AutoCloseable`. Closing it unsubscribes and waits until the function is no longer running.

```scala
val subscription = client.onMessage[String]("orders") { message =>
  println(s"${message.channel}: ${message.payload}")
}
// later, for example when the application shuts down
subscription.close()
```

If the function throws, the failure goes to the delivery thread's uncaught exception handler and delivery continues with the next message. The delivery thread does not inherit the subscriber's inheritable thread-local values, so request or transaction context does not leak into it.

## Sharded channels (cluster)

In a cluster, a **shard channel** keeps its traffic within the shard that owns the channel's slot. `sSubscribe` and `sPublish` target that node instead of broadcasting across the whole cluster. Shard channels do not support pattern subscriptions, and they deliver ordinary messages.

```scala
// ZIO; the shape is the same on every backend
ZIO.scoped {
  for {
    stream   <- client.sSubscribeScoped[String]("orders")
    _        <- client.sPublish("orders", "placed")
    messages <- stream.take(1).runCollect
  } yield messages.map(_.payload).toList
}
```

Sage holds one sharded subscription connection per owning node. When a slot moves or a node fails over, Sage moves the affected subscriptions to the new node.

## Introspection

`pubsubChannels`, `pubsubShardChannels`, `pubsubNumSub`, `pubsubShardNumSub`, and `pubsubNumPat` report the current subscription state. Each server knows only about its own subscribers. In a cluster, Sage [runs these commands on every master](/configuration#commands-that-run-on-every-master) and combines the replies.

See [Configuration](/configuration) for how to connect to a cluster.

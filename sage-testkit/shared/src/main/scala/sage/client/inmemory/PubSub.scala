package sage.client.inmemory

import scala.collection.mutable

import sage.Bytes

/**
  * The pub/sub side of an in-memory server. Every method must be called, and every subscriber created, while holding the store lock.
  * Deliveries go into each subscription's queue, and a subscriber waiting for one is woken through [[Store.afterUnlock]] once the lock is
  * released.
  */
final private[sage] class Broker(store: Store) {
  import Broker.*

  private val subscriptions = mutable.LinkedHashSet.empty[Subscriber]

  /**
    * Delivers `payload` and returns the number of receivers. A message reaches the subscribers of the channel and of every matching
    * pattern. As on a server, a client counts once for the channel and once per matching pattern, however many of its subscriptions name
    * them, because the networked client subscribes each name once per client.
    */
  def publish(channel: String, payload: Bytes): Long = {
    val name       = Bytes.utf8(channel)
    val deliveries = subscriptions.iterator.flatMap { subscriber =>
      subscriber.kind match {
        case Kind.Channel => Option.when(subscriber.names.contains(channel))(subscriber -> Delivery(channel, channel, payload))
        case Kind.Pattern =>
          subscriber.names.iterator.filter(p => Glob.matches(Bytes.utf8(p), name)).map(p => subscriber -> Delivery(p, channel, payload))
      }
    }.toVector
    deliveries.foreach((subscriber, delivery) => subscriber.offer(delivery))
    deliveries.map((subscriber, delivery) => (subscriber.owner, subscriber.kind, delivery.matched)).distinct.length.toLong
  }

  def close(owner: AnyRef): Unit = subscriptions.filter(_.owner eq owner).foreach(_.close())

  /**
    * One subscription of the client `owner`, registered from its creation until it is closed. It reads its deliveries in order. `take`
    * returns `Some` with the oldest delivery, or `Some(None)` once the subscription is closed, which drops undelivered messages as the
    * networked client does. With nothing to return yet, it keeps `wakeUp` to run after the next delivery or the close, and returns `None`.
    */
  final class Subscriber(val owner: AnyRef, val kind: Kind, val names: Vector[String]) {
    subscriptions += this

    private val queue                       = mutable.Queue.empty[Delivery]
    private var waiting: Option[() => Unit] = None
    private var closed                      = false

    def offer(delivery: Delivery): Unit = {
      queue.enqueue(delivery)
      wake()
    }

    def take(wakeUp: () => Unit): Option[Option[Delivery]] =
      if (queue.nonEmpty) Some(Some(queue.dequeue()))
      else if (closed) Some(None)
      else {
        waiting = Some(wakeUp)
        None
      }

    def close(): Unit = {
      closed = true
      queue.clear()
      subscriptions -= this
      wake()
    }

    private def wake(): Unit = {
      waiting.foreach(store.afterUnlock)
      waiting = None
    }
  }
}

private[sage] object Broker {

  enum Kind {
    case Channel, Pattern
  }

  // `matched` is the subscribed channel or pattern that the message matched
  final case class Delivery(matched: String, channel: String, payload: Bytes)
}

private[inmemory] object PubSubCommands {

  val specs: Map[String, Spec] = Map("PUBLISH" -> Spec(3, call => Replies.int(call.store.pubsub.publish(call.text(0), call.args(1)))))
}

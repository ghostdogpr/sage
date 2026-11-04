package sage.client.inmemory

import scala.collection.mutable

import sage.Bytes
import sage.protocol.Frame

/**
  * The pub/sub side of an in-memory server. Every method must be called while holding the store lock. Deliveries go into each
  * subscription's queue, and a subscriber waiting for one is woken through [[Store.afterUnlock]] once the lock is released.
  */
final private[sage] class Broker(store: Store) {
  import Broker.*

  private val subscriptions = mutable.LinkedHashSet.empty[Subscriber]

  def add(subscriber: Subscriber): Unit = subscriptions += subscriber

  def remove(subscriber: Subscriber): Unit = subscriptions -= subscriber

  /**
    * Delivers `payload` and returns the number of receivers. A classic message reaches the subscribers of the channel and of every
    * matching pattern, counting one receiver per matching pattern subscription. A shard message reaches only shard subscribers.
    */
  def publish(shard: Boolean, channel: String, payload: Bytes): Long = {
    val name       = Bytes.utf8(channel)
    val deliveries = subscriptions.iterator.flatMap { subscriber =>
      (subscriber.kind, shard) match {
        case (Kind.Shard, true) | (Kind.Channel, false) =>
          Option.when(subscriber.names.contains(channel))(subscriber -> Delivery(None, channel, payload))
        case (Kind.Pattern, false)                      =>
          subscriber.names.iterator.filter(p => Glob.matches(Bytes.utf8(p), name)).map(p => subscriber -> Delivery(Some(p), channel, payload))
        case _                                          => Iterator.empty
      }
    }.toVector
    deliveries.foreach((subscriber, delivery) => subscriber.offer(delivery))
    deliveries.length.toLong
  }

  def channels(kind: Kind): Vector[String] =
    subscriptions.iterator.filter(_.kind == kind).flatMap(_.names).toVector.distinct

  def count(kind: Kind, name: String): Long = subscriptions.count(s => s.kind == kind && s.names.contains(name)).toLong

  /**
    * One subscription, which reads its deliveries in order. `next` hands the oldest delivery to `callback`, or keeps `callback` until a
    * delivery arrives. A closed subscription answers `None`.
    */
  final class Subscriber(val kind: Kind, val names: Vector[String]) {

    private val queue                             = mutable.Queue.empty[Delivery]
    private var waiting: Option[Delivery] => Unit = null
    private var closed                            = false

    def offer(delivery: Delivery): Unit =
      if (waiting == null) queue.enqueue(delivery)
      else {
        val callback = waiting
        waiting = null
        store.afterUnlock(() => callback(Some(delivery)))
      }

    def next(callback: Option[Delivery] => Unit): Unit =
      if (queue.nonEmpty) {
        val delivery = queue.dequeue()
        store.afterUnlock(() => callback(Some(delivery)))
      } else if (closed) store.afterUnlock(() => callback(None))
      else waiting = callback

    // removes a callback whose `next` was interrupted, so that the next call can register its own
    def cancel(callback: Option[Delivery] => Unit): Unit = if (waiting eq callback) waiting = null

    def close(): Unit = {
      closed = true
      remove(this)
      if (waiting != null) {
        val callback = waiting
        waiting = null
        store.afterUnlock(() => callback(None))
      }
    }
  }
}

private[sage] object Broker {

  enum Kind {
    case Channel, Pattern, Shard
  }

  // `pattern` is the matching pattern for a pattern subscription
  final case class Delivery(pattern: Option[String], channel: String, payload: Bytes)
}

private[inmemory] object PubSubCommands {
  import Broker.Kind
  import Replies.*

  val specs: Map[String, Spec] = Map(
    "PUBLISH"  -> Spec(3, call => int(call.store.pubsub.publish(shard = false, call.text(0), call.args(1)))),
    "SPUBLISH" -> Spec(3, call => int(call.store.pubsub.publish(shard = true, call.text(0), call.args(1)))),
    "PUBSUB"   -> Spec(-2, introspect)
  )

  private def introspect(call: Call): Frame = {
    val broker                    = call.store.pubsub
    def names(kind: Kind): Frame  = {
      val pattern = call.args.lift(1)
      if (call.args.length > 2) throw new Reject(Errors.syntax)
      Frame.Array(broker.channels(kind).filter(c => pattern.forall(Glob.matches(_, Bytes.utf8(c)))).map(bulk))
    }
    def counts(kind: Kind): Frame =
      Frame.Array(call.args.drop(1).flatMap(name => Vector(bulk(name), int(broker.count(kind, name.asUtf8String)))))
    call.keyword(0) match {
      case "CHANNELS"                        => names(Kind.Channel)
      case "SHARDCHANNELS"                   => names(Kind.Shard)
      case "NUMSUB"                          => counts(Kind.Channel)
      case "SHARDNUMSUB"                     => counts(Kind.Shard)
      case "NUMPAT" if call.args.length == 1 => int(broker.channels(Kind.Pattern).length.toLong)
      case _                                 => throw new Reject(Errors.syntax)
    }
  }
}

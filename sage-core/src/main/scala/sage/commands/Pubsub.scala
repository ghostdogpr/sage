package sage.commands

import sage.{Bytes, Message, PatternMessage}
import sage.SageException.DecodeError
import sage.codec.ValueCodec
import sage.protocol.Frame

/**
  * Pub/sub command definitions. `PUBLISH`, `SPUBLISH`, and `PUBSUB` use the usual request/reply flow. In a cluster, `SPUBLISH` uses its
  * channel as the routing key, while `PUBLISH` broadcasts to the cluster. Subscription commands such as `SUBSCRIBE` and `SSUBSCRIBE`
  * produce an open-ended sequence of push frames instead of one reply. This object therefore provides their encoders for use by a
  * subscription connection.
  */
private[sage] object Pubsub {

  def publish[V](channel: String, message: V)(using codec: ValueCodec[V]): Command[Long] =
    Command("PUBLISH", Command.NoKeys, Vector(Bytes.utf8(channel), codec.encode(message)), Decode.long)

  def sPublish[V](channel: String, message: V)(using codec: ValueCodec[V]): Command[Long] =
    Command("SPUBLISH", Command.FirstKey, Vector(Bytes.utf8(channel), codec.encode(message)), Decode.long)

  def pubsubChannels(pattern: Option[String] = None): Command[Vector[String]] =
    introspect(Bytes.utf8("CHANNELS") +: pattern.map(Bytes.utf8).toVector, Decode.vector(Decode.utf8String), Merge.distinct)

  def pubsubShardChannels(pattern: Option[String] = None): Command[Vector[String]] =
    introspect(Bytes.utf8("SHARDCHANNELS") +: pattern.map(Bytes.utf8).toVector, Decode.vector(Decode.utf8String), Merge.distinct)

  def pubsubNumSub(channels: String*): Command[Map[String, Long]] =
    introspect(Bytes.utf8("NUMSUB") +: channels.toVector.map(Bytes.utf8), numSub, sumByChannel)

  def pubsubShardNumSub(channels: String*): Command[Map[String, Long]] =
    introspect(Bytes.utf8("SHARDNUMSUB") +: channels.toVector.map(Bytes.utf8), numSub, sumByChannel)

  val pubsubNumPat: Command[Long] =
    introspect(Vector(Bytes.utf8("NUMPAT")), Decode.long, Merge.sum)

  /**
    * A `PUBSUB` introspection form. Each node reports only subscribers connected to that node. In a cluster, Sage queries every slot-owning
    * master and combines their replies. Replicas are not queried. Sage connects its own subscriptions to masters, but subscriptions created
    * outside Sage and connected to replicas are not counted.
    */
  private def introspect[Out](
    args: Vector[Bytes],
    decode: Frame => Either[DecodeError, Out],
    merge: (Frame, Frame) => Frame
  ): Command[Out] =
    Command("PUBSUB", Command.NoKeys, args, decode, allMasters = true, broadcast = BroadcastReduce.Fold(merge))

  /**
    * The three subscription kinds, each with its wire encoders: classic channels (`SUBSCRIBE`), glob patterns (`PSUBSCRIBE`), and shard
    * channels (`SSUBSCRIBE`).
    */
  enum Kind(subscribeVerb: String, unsubscribeVerb: String) {
    case Channel extends Kind("SUBSCRIBE", "UNSUBSCRIBE")
    case Pattern extends Kind("PSUBSCRIBE", "PUNSUBSCRIBE")
    case Shard   extends Kind("SSUBSCRIBE", "SUNSUBSCRIBE")

    // one name per command, so the server answers it with exactly one confirmation push or one error reply
    def subscribe(name: String): Command[Unit]    = Command(subscribeVerb, Command.NoKeys, Vector(Bytes.utf8(name)), _ => Right(()))
    def unsubscribe(name: String): Command[Frame] = Command(unsubscribeVerb, Command.NoKeys, Vector(Bytes.utf8(name)), Decode.frame)
  }

  // HELLO without arguments returns the connection's server information. It runs while the server is loading, stale or busy, and every
  // connection may run it, since its setup did.
  val helloInfo: Command[Frame] = Command("HELLO", Command.NoKeys, Vector.empty, Decode.frame)

  type Delivery = Message[Bytes] | PatternMessage[Bytes]

  /**
    * A classified pub/sub push frame: a confirmation, a shard channel unsubscription, or a delivery. Deliveries contain raw payload bytes,
    * which are decoded to the subscriber's value type at the stream boundary.
    */
  enum Event {
    // the reply to a SUBSCRIBE, PSUBSCRIBE, SSUBSCRIBE, UNSUBSCRIBE or PUNSUBSCRIBE, which the server never sends on its own
    case Confirmed
    // the server sends this push to confirm an SUNSUBSCRIBE and also when it drops a shard channel whose slot moved
    case ShardUnsubscribed(channel: String)
    // `subscription` is the channel or pattern under which the subscribers of `kind` are registered
    case Delivered(kind: Kind, subscription: String, delivery: Delivery)
  }

  /**
    * Classifies the elements of a pub/sub push frame. Returns `None` for malformed frames and for other push kinds, such as client-side
    * cache invalidations handled by the multiplexed connection.
    */
  def decode(elements: Vector[Frame]): Option[Event] =
    elements match {
      case Vector(Decode.Text("message"), Decode.Text(channel), Frame.BulkString(payload))                        =>
        Some(Event.Delivered(Kind.Channel, channel, Message(channel, payload)))
      case Vector(Decode.Text("smessage"), Decode.Text(channel), Frame.BulkString(payload))                       =>
        Some(Event.Delivered(Kind.Shard, channel, Message(channel, payload)))
      case Vector(Decode.Text("pmessage"), Decode.Text(pattern), Decode.Text(channel), Frame.BulkString(payload)) =>
        Some(Event.Delivered(Kind.Pattern, pattern, PatternMessage(pattern, channel, payload)))
      case Vector(Decode.Text("subscribe" | "psubscribe" | "ssubscribe" | "unsubscribe" | "punsubscribe"), _, _)  => Some(Event.Confirmed)
      case Vector(Decode.Text("sunsubscribe"), Decode.Text(channel), _)                                           => Some(Event.ShardUnsubscribed(channel))
      case _                                                                                                      => None
    }

  private val numSub: Frame => Either[DecodeError, Map[String, Long]] = {
    val pairs = Decode.flatPairsOf("array of channel/count pairs") {
      case (Frame.BulkString(channel), Frame.Integer(count)) => Right(channel.asUtf8String -> count)
      case (channel, count)                                  => Left(DecodeError("channel/count pair", s"${Frame.describe(channel)} and ${Frame.describe(count)}"))
    }
    pairs(_).map(_.toMap)
  }

  // sums the decoded maps, so a channel a master reports twice still counts once for that master
  private val sumByChannel = Merge.typed(
    numSub,
    counts => Frame.Array(counts.iterator.flatMap((c, n) => Iterator(Frame.BulkString(Bytes.utf8(c)), Frame.Integer(n))).toVector)
  ) { (x, y) =>
    y.foldLeft(x) { case (acc, (channel, count)) => acc.updated(channel, math.addExact(acc.getOrElse(channel, 0L), count)) }
  }
}

package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.ValueCodec
import sage.protocol.{Frame, RespWriter}

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
    introspect(Bytes.utf8("CHANNELS") +: pattern.map(Bytes.utf8).toVector, decodeStrings, Merge.distinctChannels)

  def pubsubShardChannels(pattern: Option[String] = None): Command[Vector[String]] =
    introspect(Bytes.utf8("SHARDCHANNELS") +: pattern.map(Bytes.utf8).toVector, decodeStrings, Merge.distinctChannels)

  def pubsubNumSub(channels: String*): Command[Map[String, Long]] =
    introspect(Bytes.utf8("NUMSUB") +: channels.toVector.map(Bytes.utf8), decodeNumSub, Merge.sumByChannel)

  def pubsubShardNumSub(channels: String*): Command[Map[String, Long]] =
    introspect(Bytes.utf8("SHARDNUMSUB") +: channels.toVector.map(Bytes.utf8), decodeNumSub, Merge.sumByChannel)

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

  def subscribe(channels: Vector[String]): Bytes    = RespWriter.writeCommand("SUBSCRIBE", channels.map(Bytes.utf8))
  def unsubscribe(channels: Vector[String]): Bytes  = RespWriter.writeCommand("UNSUBSCRIBE", channels.map(Bytes.utf8))
  def psubscribe(patterns: Vector[String]): Bytes   = RespWriter.writeCommand("PSUBSCRIBE", patterns.map(Bytes.utf8))
  def punsubscribe(patterns: Vector[String]): Bytes = RespWriter.writeCommand("PUNSUBSCRIBE", patterns.map(Bytes.utf8))
  def ssubscribe(channels: Vector[String]): Bytes   = RespWriter.writeCommand("SSUBSCRIBE", channels.map(Bytes.utf8))
  def sunsubscribe(channels: Vector[String]): Bytes = RespWriter.writeCommand("SUNSUBSCRIBE", channels.map(Bytes.utf8))

  /**
    * A classified pub/sub push frame. Confirmations include the current subscription count. Deliveries contain raw payload bytes, which are
    * decoded to the subscriber's value type at the stream boundary.
    */
  enum Event {
    case Subscribed(channel: String, count: Long)
    case Unsubscribed(channel: String, count: Long)
    case Message(channel: String, payload: Bytes)
    // kept separate from Message so a connection can route classic and sharded deliveries to different subscribers.
    case ShardMessage(channel: String, payload: Bytes)
    case PatternMessage(pattern: String, channel: String, payload: Bytes)
  }

  /**
    * Classifies the elements of a pub/sub push frame. Returns `None` for malformed frames and for other push kinds, such as client-side
    * cache invalidations handled by the multiplexed connection.
    */
  def decode(elements: Vector[Frame]): Option[Event] =
    elements match {
      case Vector(kind, a, b)           =>
        text(kind).flatMap {
          case "message"                                       =>
            for {
              ch <- text(a)
              p  <- bytes(b)
            } yield Event.Message(ch, p)
          case "smessage"                                      =>
            for {
              ch <- text(a)
              p  <- bytes(b)
            } yield Event.ShardMessage(ch, p)
          case "subscribe" | "psubscribe" | "ssubscribe"       =>
            for {
              ch <- text(a)
              c  <- int(b)
            } yield Event.Subscribed(ch, c)
          case "unsubscribe" | "punsubscribe" | "sunsubscribe" =>
            for {
              ch <- text(a)
              c  <- int(b)
            } yield Event.Unsubscribed(ch, c)
          case _                                               => None
        }
      case Vector(kind, p, ch, payload) =>
        text(kind).flatMap {
          case "pmessage" =>
            for {
              pat <- text(p)
              c   <- text(ch)
              pl  <- bytes(payload)
            } yield Event.PatternMessage(pat, c, pl)
          case _          => None
        }
      case _                            => None
    }

  private def text(frame: Frame): Option[String] =
    frame match {
      case Frame.BulkString(b)   => Some(b.asUtf8String)
      case Frame.SimpleString(s) => Some(s)
      case _                     => None
    }

  private def bytes(frame: Frame): Option[Bytes] =
    frame match {
      case Frame.BulkString(b) => Some(b)
      case _                   => None
    }

  private def int(frame: Frame): Option[Long] =
    frame match {
      case Frame.Integer(i) => Some(i)
      case _                => None
    }

  private def decodeStrings(frame: Frame): Either[DecodeError, Vector[String]] =
    frame match {
      case Frame.Array(elements) =>
        val builder = Vector.newBuilder[String]
        val it      = elements.iterator
        while (it.hasNext)
          it.next() match {
            case Frame.BulkString(b) => builder += b.asUtf8String
            case other               => return Left(DecodeError("bulk string", Frame.describe(other)))
          }
        Right(builder.result())
      case other                 => Left(DecodeError("array", Frame.describe(other)))
    }

  private def decodeNumSub(frame: Frame): Either[DecodeError, Map[String, Long]] =
    Merge
      .channelCounts(frame)
      .map(_.iterator.map { case (channel, count) => channel.value.asUtf8String -> count }.toMap)
      .toRight(DecodeError("array of channel/count pairs", Frame.describe(frame)))
}

package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.ValueCodec
import sage.protocol.{Frame, RespWriter}

/**
  * Pub/sub. `PUBLISH`, `SPUBLISH`, and `PUBSUB` are ordinary request/reply commands; `SPUBLISH` carries its channel as a routing key so a
  * cluster sends it to the slot's owner, while `PUBLISH` is keyless and broadcasts across the whole cluster bus. `SUBSCRIBE` and its
  * relatives (including the sharded `SSUBSCRIBE`/`SUNSUBSCRIBE`) are not ordinary commands — they deliver open-ended push frames with no
  * single typed reply — so only their wire encoders live here, written straight onto a Subscription Connection, never run.
  */
private[sage] object Pubsub {

  def publish[V](channel: String, message: V)(using codec: ValueCodec[V]): Command[Long] =
    Command("PUBLISH", Command.NoKeys, Vector(Bytes.utf8(channel), codec.encode(message)), Decode.long)

  // the channel is a routing key (FirstKey): a cluster hashes it to a slot and sends SPUBLISH to that slot's owner, like a keyed write
  def sPublish[V](channel: String, message: V)(using codec: ValueCodec[V]): Command[Long] =
    Command("SPUBLISH", Command.FirstKey, Vector(Bytes.utf8(channel), codec.encode(message)), Decode.long)

  def pubsubChannels(pattern: Option[String] = None): Command[Vector[String]] =
    Command("PUBSUB", Command.NoKeys, Bytes.utf8("CHANNELS") +: pattern.map(Bytes.utf8).toVector, decodeStrings)

  def pubsubShardChannels(pattern: Option[String] = None): Command[Vector[String]] =
    Command("PUBSUB", Command.NoKeys, Bytes.utf8("SHARDCHANNELS") +: pattern.map(Bytes.utf8).toVector, decodeStrings)

  def pubsubNumSub(channels: String*): Command[Map[String, Long]] =
    Command("PUBSUB", Command.NoKeys, Bytes.utf8("NUMSUB") +: channels.toVector.map(Bytes.utf8), decodeNumSub)

  def pubsubShardNumSub(channels: String*): Command[Map[String, Long]] =
    Command("PUBSUB", Command.NoKeys, Bytes.utf8("SHARDNUMSUB") +: channels.toVector.map(Bytes.utf8), decodeNumSub)

  val pubsubNumPat: Command[Long] =
    Command("PUBSUB", Command.NoKeys, Vector(Bytes.utf8("NUMPAT")), Decode.long)

  def subscribe(channels: Vector[String]): Bytes    = RespWriter.writeCommand("SUBSCRIBE", channels.map(Bytes.utf8))
  def unsubscribe(channels: Vector[String]): Bytes  = RespWriter.writeCommand("UNSUBSCRIBE", channels.map(Bytes.utf8))
  def psubscribe(patterns: Vector[String]): Bytes   = RespWriter.writeCommand("PSUBSCRIBE", patterns.map(Bytes.utf8))
  def punsubscribe(patterns: Vector[String]): Bytes = RespWriter.writeCommand("PUNSUBSCRIBE", patterns.map(Bytes.utf8))
  def ssubscribe(channels: Vector[String]): Bytes   = RespWriter.writeCommand("SSUBSCRIBE", channels.map(Bytes.utf8))
  def sunsubscribe(channels: Vector[String]): Bytes = RespWriter.writeCommand("SUNSUBSCRIBE", channels.map(Bytes.utf8))

  /**
    * A classified pub/sub push frame. Confirmations carry the running subscription count; deliveries carry the raw payload bytes, decoded
    * to the subscriber's value type at the stream boundary, not here.
    */
  enum Event {
    case Subscribed(channel: String, count: Long)
    case Unsubscribed(channel: String, count: Long)
    case Message(channel: String, payload: Bytes)
    // a sharded delivery: shape-identical to Message, kept distinct so a connection carrying both classic and shard sinks routes it correctly
    case ShardMessage(channel: String, payload: Bytes)
    case PatternMessage(pattern: String, channel: String, payload: Bytes)
  }

  /**
    * Classifies a push frame's elements. `None` for kinds the Subscription Connection does not consume (e.g. client-side-cache
    * invalidations, which ride the Multiplexed Connection) and for malformed frames.
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

  // NUMSUB replies a flat [channel, count, channel, count, …] array
  private def decodeNumSub(frame: Frame): Either[DecodeError, Map[String, Long]] =
    frame match {
      case Frame.Array(elements) if elements.length % 2 == 0 =>
        val builder = Map.newBuilder[String, Long]
        val pairs   = elements.grouped(2)
        while (pairs.hasNext)
          pairs.next() match {
            case Vector(Frame.BulkString(ch), Frame.Integer(count)) => builder += ch.asUtf8String -> count
            case other                                              => return Left(DecodeError("channel/count pair", other.map(Frame.describe).mkString(", ")))
          }
        Right(builder.result())
      case other => Left(DecodeError("array of channel/count pairs", Frame.describe(other)))
    }
}

package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.cluster.{Node, Shard, Slot, SlotRange}
import sage.protocol.Frame

private[sage] object Cluster {

  val slots: Command[Vector[Shard]] =
    Command("CLUSTER", keyIndices = Command.NoKeys, args = Vector(Bytes.utf8("SLOTS")), decode = decodeSlots)

  final private case class Range(master: Node, replicas: Vector[Node], slots: SlotRange)

  // CLUSTER SLOTS replies with an array of ranges, each `[start, end, master, replica*]` where a node is `[ip, port, id, meta?]` and the
  // first node is the master. The reply is the topology authority, so an unparseable entry fails the whole decode rather than dropping a
  // range and routing off a partial map. Ranges sharing a master are merged into one Shard, in first-seen master order.
  private def decodeSlots(frame: Frame): Either[DecodeError, Vector[Shard]] =
    Decode.vector(decodeRange)(frame).map(merge)

  private def decodeRange(frame: Frame): Either[DecodeError, Range] =
    frame match {
      case Frame.Array(elements) if elements.length >= 3 =>
        for {
          start <- slotOf(elements(0))
          end   <- slotOf(elements(1))
          nodes <- Decode.vector(decodeNode)(Frame.Array(elements.drop(2)))
        } yield Range(nodes.head, nodes.tail, SlotRange(start, end))
      case other                                         => Left(DecodeError("array of [start, end, master, replicas...]", Frame.describe(other)))
    }

  private def decodeNode(frame: Frame): Either[DecodeError, Node] =
    frame match {
      case Frame.Array(elements) if elements.length >= 2 =>
        for {
          host <- stringOf(elements(0))
          port <- intOf(elements(1))
        } yield Node(host, port)
      case other                                         => Left(DecodeError("node array [ip, port, id, ...]", Frame.describe(other)))
    }

  private def merge(ranges: Vector[Range]): Vector[Shard] =
    ranges.map(_.master).distinct.map { master =>
      val forMaster = ranges.filter(_.master == master)
      Shard(master, forMaster.flatMap(_.replicas).distinct, forMaster.map(_.slots))
    }

  private def slotOf(frame: Frame): Either[DecodeError, Slot] =
    intOf(frame).flatMap(index => Slot.at(index).toRight(DecodeError("slot in [0, 16384)", index.toString)))

  private def intOf(frame: Frame): Either[DecodeError, Int] =
    frame match {
      case Frame.Integer(value) if value.isValidInt =>
        Right(value.toInt) // guard the narrowing: a Long outside Int range must not wrap into a valid slot or port
      case Frame.Integer(value) => Left(DecodeError("integer within Int range", value.toString))
      case other                => Left(DecodeError("integer", Frame.describe(other)))
    }

  private def stringOf(frame: Frame): Either[DecodeError, String] =
    frame match {
      case Frame.BulkString(bytes)  => Right(bytes.asUtf8String)
      case Frame.SimpleString(text) => Right(text)
      case other                    => Left(DecodeError("string", Frame.describe(other)))
    }
}

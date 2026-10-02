package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.cluster.{Node, Slot, SlotRange}
import sage.protocol.Frame

private[sage] object Cluster {

  // Each range has the form [start, end, master, replica*], and each node has the form [ip, port, id, meta?]. The first node is the master. If
  // any entry cannot be decoded, reject the complete topology reply to avoid routing with incomplete information.
  def slots(queried: Node): Command[Vector[SlotRange]] =
    Command("CLUSTER", keyIndices = Command.NoKeys, args = Vector(Bytes.utf8("SLOTS")), decode = Decode.vector(decodeRange(queried)))

  private def decodeRange(queried: Node): Frame => Either[DecodeError, SlotRange] = {
    val node = decodeNode(queried)
    Decode.shape("array of [start, end, master, replicas...]") { case Frame.Array(startFrame +: endFrame +: masterFrame +: replicaFrames) =>
      for {
        start    <- slotOf(startFrame)
        end      <- slotOf(endFrame)
        master   <- node(masterFrame)
        replicas <- Decode.each(replicaFrames)(node)
      } yield SlotRange(start, end, master, replicas)
    }
  }

  // an empty or null endpoint means the node that answered CLUSTER SLOTS
  private def decodeNode(queried: Node): Frame => Either[DecodeError, Node] = Decode.shape("node array [ip, port, id, ...]") {
    case Frame.Array(hostFrame +: portFrame +: _) =>
      for {
        host <- endpointOf(hostFrame)
        port <- intOf(portFrame)
      } yield Node(if (host.isEmpty) queried.host else host, port)
  }

  private def slotOf(frame: Frame): Either[DecodeError, Slot] =
    intOf(frame).flatMap(index => Slot.at(index).toRight(DecodeError("slot in [0, 16384)", index.toString)))

  private val intOf: Frame => Either[DecodeError, Int] = Decode.shape("integer") {
    case Frame.Integer(value) if value.isValidInt =>
      Right(value.toInt) // guard the narrowing: a Long outside Int range must not wrap into a valid slot or port
    case Frame.Integer(value) => Left(DecodeError("integer within Int range", value.toString))
  }

  private val endpointOf: Frame => Either[DecodeError, String] = Decode.shape("string or null endpoint") {
    case Decode.Text(text) => Right(text)
    case Frame.Null        => Right("")
  }
}

package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.{Frame, RespWriter}

/**
  * A pure value describing one server command. `keyIndices` marks which `args` positions are keys — the routing view for the cluster slot
  * engine; defining keys as positions makes a key missing from the wire unrepresentable, and matches the positional key_specs in the
  * server's commands.json. `decode` never sees top-level error frames — [[Reply.run]] intercepts them.
  */
final case class Command[+Out](
  name: String,
  keyIndices: Vector[Int],
  args: Vector[Bytes],
  decode: Frame => Either[DecodeError, Out]
) {

  def map[B](f: Out => B): Command[B] = Command(name, keyIndices, args, frame => decode(frame).map(f))

  def encode: Bytes = RespWriter.writeCommand(name, args)
}

object Command {

  // shared by fixed-arity command constructors so no index vector is allocated per call
  val NoKeys: Vector[Int]   = Vector.empty
  val FirstKey: Vector[Int] = Vector(0)
}

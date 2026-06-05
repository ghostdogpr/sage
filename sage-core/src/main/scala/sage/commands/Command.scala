package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.{Frame, RespWriter}

/**
  * A pure value describing one server command. `keyIndices` marks which `args` positions are keys, for cluster routing. `decode` never
  * sees top-level error frames — [[Reply.run]] intercepts them.
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

  val NoKeys: Vector[Int]   = Vector.empty
  val FirstKey: Vector[Int] = Vector(0)
}

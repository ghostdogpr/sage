package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.{Frame, RespWriter}

/**
  * A pure value describing one server command. `keys` is the routing view for the cluster slot engine; each key also appears in `args` at
  * its wire position. `decode` never sees top-level error frames — [[Reply.run]] intercepts them.
  */
final case class Command[+Out](
  name: String,
  keys: Vector[Bytes],
  args: Vector[Bytes],
  decode: Frame => Either[DecodeError, Out]
) {

  def map[B](f: Out => B): Command[B] = Command(name, keys, args, frame => decode(frame).map(f))

  def encode: Bytes = RespWriter.writeCommand(name, args)
}

package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.{Frame, RespWriter}

/**
  * A pure value describing one server command: its wire encoding and its typed reply decoder. The single source of truth; the client's
  * per-command methods are sugar over it.
  *
  * `args` is the wire truth (everything after the command name); `keys` is the routing view consumed by the cluster slot engine, with each
  * key also present in `args` at its wire position. `decode` only ever sees non-error top-level frames: error replies are intercepted
  * uniformly by [[Reply.run]].
  */
final case class Command[+Out](
  name: String,
  keys: Vector[Bytes],
  args: Vector[Bytes],
  decode: Frame => Either[DecodeError, Out]
) {

  def map[B](f: Out => B): Command[B] = Command(name, keys, args, frame => decode(frame).map(f))

  /**
    * The command's wire bytes: a RESP3 array of bulk strings, multi-word names (`CONFIG GET`) one bulk string per word. Encoded directly,
    * without building intermediate frames — this runs once per command sent.
    */
  def encode: Bytes = RespWriter.writeCommand(name, args)
}

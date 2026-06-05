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
    * The command as a RESP3 array of bulk strings, as sent on the wire. Multi-word names (`CONFIG GET`) become one bulk string per word.
    */
  def toFrame: Frame =
    Frame.Array(name.split(' ').toVector.map(part => Frame.BulkString(Bytes.utf8(part))) ++ args.map(Frame.BulkString.apply))

  /**
    * The command's wire bytes.
    */
  def encode: Bytes = RespWriter.write(toFrame)
}

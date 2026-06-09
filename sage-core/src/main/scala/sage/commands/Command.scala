package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.{Frame, RespWriter}

/**
  * How a command must be carried by the runtime. `Ordinary` commands are auto-pipelined onto the Multiplexed Connection; `Blocking` ones
  * block the connection (`BLPOP`, …) and so run alone on a Dedicated Connection borrowed from the pool. The requirement is intrinsic to
  * the command, not to any backend, so it lives on the pure value rather than in runtime glue.
  */
enum Execution {
  case Ordinary, Blocking
}

/**
  * A pure value describing one server command. `keyIndices` marks which `args` positions are keys, for cluster routing. `decode` never
  * sees top-level error frames — [[Reply.run]] intercepts them.
  */
final case class Command[+Out](
  name: String,
  keyIndices: Vector[Int],
  args: Vector[Bytes],
  decode: Frame => Either[DecodeError, Out],
  execution: Execution = Execution.Ordinary
) {

  def map[B](f: Out => B): Command[B] = Command(name, keyIndices, args, frame => decode(frame).map(f), execution)

  def isBlocking: Boolean = execution == Execution.Blocking

  def encode: Bytes = RespWriter.writeCommand(name, args)
}

object Command {

  val NoKeys: Vector[Int]   = Vector.empty
  val FirstKey: Vector[Int] = Vector(0)
}

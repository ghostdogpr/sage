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
  *
  * `isReadOnly` marks side-effect-free reads (a future input to replica routing). `cacheable` is the narrower property client-side caching
  * needs: a read whose result is a pure function of the named keys' current state, so a server invalidation push covers every way it can
  * change. Time-varying reads (`TTL`, `OBJECT IDLETIME`) and non-deterministic ones (`SRANDMEMBER`) are read-only but **not** cacheable —
  * they change with no key write, so no invalidation would ever fire. Both are intrinsic metadata set by the builders.
  */
final case class Command[+Out](
  name: String,
  keyIndices: Vector[Int],
  args: Vector[Bytes],
  decode: Frame => Either[DecodeError, Out],
  execution: Execution = Execution.Ordinary,
  isReadOnly: Boolean = false,
  cacheable: Boolean = false
) {

  def map[B](f: Out => B): Command[B] = Command(name, keyIndices, args, frame => decode(frame).map(f), execution, isReadOnly, cacheable)

  def isBlocking: Boolean = execution == Execution.Blocking

  // the key bytes the server tracks for this command, in arg order — the reverse-index keys a cache invalidation evicts by
  def keys: Vector[Bytes] = keyIndices.map(args)

  def encode: Bytes = RespWriter.writeCommand(name, args)
}

object Command {

  val NoKeys: Vector[Int]   = Vector.empty
  val FirstKey: Vector[Int] = Vector(0)

  /**
    * A read-only command whose result is a pure function of its keys' state: read-only and client-side cacheable.
    */
  def read[Out](
    name: String,
    keyIndices: Vector[Int],
    args: Vector[Bytes],
    decode: Frame => Either[DecodeError, Out]
  ): Command[Out] = Command(name, keyIndices, args, decode, Execution.Ordinary, isReadOnly = true, cacheable = true)

  /**
    * A read-only command that must not be cached: its result varies with time or is non-deterministic, so no invalidation would evict it.
    */
  def readUncacheable[Out](
    name: String,
    keyIndices: Vector[Int],
    args: Vector[Bytes],
    decode: Frame => Either[DecodeError, Out]
  ): Command[Out] = Command(name, keyIndices, args, decode, Execution.Ordinary, isReadOnly = true, cacheable = false)
}

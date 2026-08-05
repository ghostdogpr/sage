package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.{Frame, RespWriter}

/**
  * Selects how the runtime executes a command. `Ordinary` commands use the auto-pipelined multiplexed connection. Commands that block a
  * connection, such as `BLPOP`, run alone on a dedicated pooled connection. This requirement belongs to the command value and is shared by
  * every backend.
  */
enum Execution {
  case Ordinary, Blocking
}

/**
  * Selects how to combine replies from an `allMasters` command. `First` returns the first reply when every node should return the same value,
  * such as the SHA from `SCRIPT LOAD`. `Concat` joins the array elements returned by each node, as required by `KEYS`. `Fold` combines replies
  * two at a time with the supplied function. This setting is used only when `allMasters` is true.
  */
enum BroadcastReduce {
  case First
  case Concat
  case Fold(combine: (Frame, Frame) => Frame)
}

/**
  * Stores one server command, including its encoded arguments and reply decoder. `keyIndices` marks the argument positions used as keys for
  * cluster routing. [[Reply.run]] handles top-level error frames before calling `decode`.
  *
  * `isReadOnly` marks side-effect-free reads for replica routing. `cacheable` is narrower: the result must depend only on the named keys'
  * current state, allowing server invalidations to cover every change. Time-varying reads (`TTL`, `OBJECT IDLETIME`) and non-deterministic
  * reads (`SRANDMEMBER`) are read-only but not cacheable because they can change without a key write. Builders set both properties.
  *
  * `allMasters` marks a keyless command whose effect or answer is local to a node. A cluster runs it on every slot-owning master (`SCRIPT LOAD`,
  * `FUNCTION LOAD` and their `FLUSH`/`DELETE`/`RESTORE` mutations, `FLUSHALL`/`FLUSHDB`,
  * `MEMORY PURGE`, and the `PUBSUB` introspection forms, which report only the subscribers attached to the node answering). Standalone
  * servers ignore this setting.
  *
  * `cursorBound` marks a command whose reply contains a continuation cursor valid only on the node that issued it (`SCAN`/`HSCAN`/`SSCAN`/
  * `ZSCAN`). Such a read is excluded from replica round-robin routing: iterating its pages across different replicas would feed a cursor to
  * a different node, which could skip or duplicate entries.
  *
  * `broadcast` selects the [[BroadcastReduce]] strategy for combining replies from an `allMasters` command. This supports identical
  * acknowledgements, concatenated key or channel lists, subscriber totals, and the lowest durability count reported by a shard. Broadcasts
  * use masters regardless of the `ReadFrom` policy because a replica contains data for only one shard. Standalone servers ignore this setting.
  *
  * `requiresClusterWideTxResult` marks a command whose result requires replies from every master. A cluster transaction uses one node and
  * rejects such commands before sending them. Only `DBSIZE` sets this property because it promises the cluster-wide key count. Other
  * broadcasts (`KEYS`, `FLUSHALL`, `SCRIPT LOAD`, `WAIT`) remain valid in a transaction with node-local semantics, matching Redis, which does
  * not flag them `no-multi`. Standalone servers ignore this setting.
  */
final case class Command[+Out](
  name: String,
  keyIndices: Vector[Int],
  args: Vector[Bytes],
  decode: Frame => Either[DecodeError, Out],
  execution: Execution = Execution.Ordinary,
  isReadOnly: Boolean = false,
  cacheable: Boolean = false,
  allMasters: Boolean = false,
  cursorBound: Boolean = false,
  broadcast: BroadcastReduce = BroadcastReduce.First,
  requiresClusterWideTxResult: Boolean = false
) {

  /**
    * Transforms the decoded result, leaving the wire encoding and routing metadata untouched.
    */
  def map[B](f: Out => B): Command[B] = withDecode(frame => decode(frame).map(f))

  /**
    * Whether this command requires a dedicated connection because it blocks the connection it uses.
    */
  def isBlocking: Boolean = execution == Execution.Blocking

  // rebuild by named fields to preserve their order and ensure future fields are copied explicitly.
  private def withDecode[B](decode: Frame => Either[DecodeError, B]): Command[B] =
    Command(
      name = name,
      keyIndices = keyIndices,
      args = args,
      decode = decode,
      execution = execution,
      isReadOnly = isReadOnly,
      cacheable = cacheable,
      allMasters = allMasters,
      cursorBound = cursorBound,
      broadcast = broadcast,
      requiresClusterWideTxResult = requiresClusterWideTxResult
    )

  /**
    * The command's key bytes in argument order. Client-side cache invalidations use these bytes to remove affected results.
    */
  def keys: Vector[Bytes] = keyIndices.map(args)

  /**
    * Whether any declared key index falls outside `args` — a builder bug, never expected at runtime.
    */
  def hasMalformedKeys: Boolean = keyIndices.exists(index => index < 0 || index >= args.length)

  /**
    * The full RESP3 wire encoding of this command.
    */
  def encode: Bytes = RespWriter.writeCommand(name, args)

  /**
    * This command's wire form with decoding replaced by the raw reply frame, preserving routing metadata. The cluster runtime uses it to
    * collect a broadcast's per-node replies and fold them before decoding once.
    */
  def rawFrame: Command[Frame] = withDecode(frame => Right(frame))
}

object Command {

  /**
    * Key-index marker for a keyless command.
    */
  val NoKeys: Vector[Int] = Vector.empty

  /**
    * Key-index marker for the common case of a single key in the first argument position.
    */
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
    * A read-only command with a node-local continuation cursor (`SCAN` and its `H`/`S`/`Z` variants). All pages remain pinned to the issuing
    * node. Cursor pages are not cacheable because they do not depend only on key state.
    */
  def readCursor[Out](
    name: String,
    keyIndices: Vector[Int],
    args: Vector[Bytes],
    decode: Frame => Either[DecodeError, Out]
  ): Command[Out] = Command(name, keyIndices, args, decode, Execution.Ordinary, isReadOnly = true, cacheable = false, cursorBound = true)

  /**
    * A read-only command whose time-varying or non-deterministic result cannot be invalidated reliably.
    */
  def readUncacheable[Out](
    name: String,
    keyIndices: Vector[Int],
    args: Vector[Bytes],
    decode: Frame => Either[DecodeError, Out]
  ): Command[Out] = Command(name, keyIndices, args, decode, Execution.Ordinary, isReadOnly = true, cacheable = false)
}

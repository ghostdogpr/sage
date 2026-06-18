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
  *
  * `allMasters` marks a keyless command whose effect is per-node and not replicated across shards, so a cluster must run it on every
  * slot-owning master rather than one (`SCRIPT LOAD`, `FUNCTION LOAD` and their `FLUSH`/`DELETE`/`RESTORE` mutations, and `FLUSHALL`/
  * `FLUSHDB`). Inert on a standalone server.
  *
  * `aggregate` refines an `allMasters` command whose reply is a per-node *view* rather than an identical acknowledgement (`KEYS`): the
  * cluster broadcasts it to every slot-owning master and concatenates the array replies into one, since no single node sees the whole
  * keyspace. The broadcast always targets masters regardless of the `ReadFrom` policy (a single replica would only see one shard's slice).
  * An `allMasters` command without `aggregate` keeps the first node's reply (every node returns the same `OK`/SHA). Inert on a standalone
  * server.
  *
  * `cursorBound` marks a command whose reply carries a continuation cursor valid only on the node that issued it (`SCAN`/`HSCAN`/`SSCAN`/
  * `ZSCAN`). Such a read is excluded from replica round-robin routing: iterating its pages across different replicas would feed a cursor to
  * a node that never minted it, skipping or duplicating entries.
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
  aggregate: Boolean = false
) {

  /**
    * Transforms the decoded result, leaving the wire encoding and routing metadata untouched.
    */
  def map[B](f: Out => B): Command[B] =
    Command(name, keyIndices, args, frame => decode(frame).map(f), execution, isReadOnly, cacheable, allMasters, cursorBound, aggregate)

  /**
    * Whether this command blocks its connection and so needs a Dedicated Connection.
    */
  def isBlocking: Boolean = execution == Execution.Blocking

  /**
    * The key bytes the server tracks for this command, in arg order — the keys a cache invalidation evicts by.
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
    * collect an `aggregate` broadcast's per-node replies and merge them before decoding once.
    */
  def rawFrame: Command[Frame] =
    Command(name, keyIndices, args, frame => Right(frame), execution, isReadOnly, cacheable, allMasters, cursorBound, aggregate)
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
    * A read-only command whose reply carries a node-local continuation cursor (`SCAN` and its `H`/`S`/`Z` variants): read-only but pinned to
    * its issuing node, so replica routing must not round-robin its pages. Not cacheable — a cursor page is not a pure function of key state.
    */
  def readCursor[Out](
    name: String,
    keyIndices: Vector[Int],
    args: Vector[Bytes],
    decode: Frame => Either[DecodeError, Out]
  ): Command[Out] = Command(name, keyIndices, args, decode, Execution.Ordinary, isReadOnly = true, cacheable = false, cursorBound = true)

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

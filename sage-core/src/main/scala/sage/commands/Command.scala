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
  * How a keyless `allMasters` broadcast folds its per-node replies into one. `First` keeps the first node's reply (identical
  * acknowledgements like `SCRIPT LOAD`'s SHA); `Concat` appends each node's array slice (`KEYS`); `Fold` reduces pairwise (a durability
  * barrier down to its weakest shard). A single policy rather than separate flags, so `Concat` and `Fold` cannot both be requested. Inert
  * unless `allMasters`.
  */
enum BroadcastReduce {
  case First
  case Concat
  case Fold(combine: (Frame, Frame) => Frame)
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
  * `cursorBound` marks a command whose reply carries a continuation cursor valid only on the node that issued it (`SCAN`/`HSCAN`/`SSCAN`/
  * `ZSCAN`). Such a read is excluded from replica round-robin routing: iterating its pages across different replicas would feed a cursor to
  * a node that never minted it, skipping or duplicating entries.
  *
  * `broadcast` chooses how an `allMasters` command's per-node replies fold into one: `First` keeps one node's identical acknowledgement,
  * `Concat` appends each node's array slice (`KEYS`, since no single node sees the whole keyspace), `Fold` reduces pairwise (a durability
  * barrier down to its weakest shard). The broadcast always targets masters regardless of the `ReadFrom` policy (a single replica would only
  * see one shard's slice). Inert on a standalone server.
  *
  * `requiresClusterWideTxResult` marks a command whose result is correct only when aggregated across every master, so a cluster `MULTI`/
  * `EXEC` — pinned to a single node — cannot produce it and rejects it before the wire. Only `DBSIZE` sets it: its contract is the cluster-wide
  * key count, which one shard cannot give. Other broadcasts (`KEYS`, `FLUSHALL`, `SCRIPT LOAD`, `WAIT`) stay transaction-legal with node-local
  * semantics, matching Redis, which does not flag them `no-multi`. Inert on a standalone server.
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
    * Whether this command blocks its connection and so needs a Dedicated Connection.
    */
  def isBlocking: Boolean = execution == Execution.Blocking

  // rebuilds the command with a new (possibly re-typed) decoder, carrying every field by name so a future field can't be dropped or misordered
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

package sage.commands

import sage.Bytes
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * Server-side Lua scripting. `EVAL`/`EVALSHA` and their `_RO` reads return the raw RESP3 [[Frame]], since a script's reply shape is
  * defined by user code. `SCRIPT LOAD`, `SCRIPT FLUSH` and `SCRIPT EXISTS` run on every master, because a cluster keeps no shared script
  * cache and a later key-routed `EVALSHA` must find the script wherever its keys live. `EXISTS` therefore reports a sha present only when
  * every master has it (a per-sha AND), so a `true` reply guarantees the key-routed `EVALSHA` cannot hit a master that lacks the script.
  */
private[sage] object Scripting {

  private val Load   = Bytes.utf8("LOAD")
  private val Exists = Bytes.utf8("EXISTS")
  private val Flush  = Bytes.utf8("FLUSH")
  private val Kill   = Bytes.utf8("KILL")
  private val Show   = Bytes.utf8("SHOW")

  def eval(script: String): Command[Frame] = evalCommand("EVAL", script, Vector.empty, Vector.empty, readOnly = false)

  def eval[K](script: String, keys: Seq[K])(using keyCodec: KeyCodec[K]): Command[Frame] =
    evalCommand("EVAL", script, keys.iterator.map(keyCodec.encode).toVector, Vector.empty, readOnly = false)

  def eval[K, V](script: String, keys: Seq[K], args: Seq[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Frame] =
    evalCommand("EVAL", script, keys.iterator.map(keyCodec.encode).toVector, args.iterator.map(valueCodec.encode).toVector, readOnly = false)

  def evalRo(script: String): Command[Frame] = evalCommand("EVAL_RO", script, Vector.empty, Vector.empty, readOnly = true)

  def evalRo[K](script: String, keys: Seq[K])(using keyCodec: KeyCodec[K]): Command[Frame] =
    evalCommand("EVAL_RO", script, keys.iterator.map(keyCodec.encode).toVector, Vector.empty, readOnly = true)

  def evalRo[K, V](script: String, keys: Seq[K], args: Seq[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Frame] =
    evalCommand("EVAL_RO", script, keys.iterator.map(keyCodec.encode).toVector, args.iterator.map(valueCodec.encode).toVector, readOnly = true)

  def evalSha(sha: String): Command[Frame] = evalCommand("EVALSHA", sha, Vector.empty, Vector.empty, readOnly = false)

  def evalSha[K](sha: String, keys: Seq[K])(using keyCodec: KeyCodec[K]): Command[Frame] =
    evalCommand("EVALSHA", sha, keys.iterator.map(keyCodec.encode).toVector, Vector.empty, readOnly = false)

  def evalSha[K, V](sha: String, keys: Seq[K], args: Seq[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Frame] =
    evalCommand("EVALSHA", sha, keys.iterator.map(keyCodec.encode).toVector, args.iterator.map(valueCodec.encode).toVector, readOnly = false)

  def evalShaRo(sha: String): Command[Frame] = evalCommand("EVALSHA_RO", sha, Vector.empty, Vector.empty, readOnly = true)

  def evalShaRo[K](sha: String, keys: Seq[K])(using keyCodec: KeyCodec[K]): Command[Frame] =
    evalCommand("EVALSHA_RO", sha, keys.iterator.map(keyCodec.encode).toVector, Vector.empty, readOnly = true)

  def evalShaRo[K, V](sha: String, keys: Seq[K], args: Seq[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Frame] =
    evalCommand("EVALSHA_RO", sha, keys.iterator.map(keyCodec.encode).toVector, args.iterator.map(valueCodec.encode).toVector, readOnly = true)

  private def evalCommand(name: String, script: String, keys: Vector[Bytes], args: Vector[Bytes], readOnly: Boolean): Command[Frame] = {
    val allArgs    = (Bytes.utf8(script) +: Bytes.utf8(keys.length.toString) +: keys) ++ args
    val keyIndices = Vector.range(2, 2 + keys.length)
    Command(name, keyIndices, allArgs, Decode.frame, Execution.Ordinary, isReadOnly = readOnly, cacheable = false)
  }

  def scriptLoad(script: String): Command[String] =
    Command("SCRIPT", Command.NoKeys, Vector(Load, Bytes.utf8(script)), Decode.utf8String, allMasters = true)

  private val existsAnd: (Frame, Frame) => Frame = (a, b) =>
    (a, b) match {
      case (Frame.Array(xs), Frame.Array(ys)) if xs.length == ys.length =>
        Frame.Array(xs.lazyZip(ys).map {
          case (Frame.Integer(x), Frame.Integer(y)) => Frame.Integer(if (x == 1L && y == 1L) 1L else 0L)
          case (Frame.Integer(_), bad)              => bad
          case (bad, _)                             => bad
        })
      case (Frame.Array(_), bad)                                        => bad
      case (bad, _)                                                     => bad
    }

  def scriptExists(first: String, rest: String*): Command[Vector[Boolean]] =
    Command(
      "SCRIPT",
      Command.NoKeys,
      Exists +: (first +: rest).iterator.map(Bytes.utf8).toVector,
      Decode.vector(Decode.flag),
      allMasters = true,
      broadcast = BroadcastReduce.Fold(existsAnd)
    )

  def scriptFlush(mode: Option[FlushMode] = None): Command[Unit] =
    Command("SCRIPT", Command.NoKeys, Flush +: FlushMode.args(mode), Decode.ok, allMasters = true)

  val scriptKill: Command[Unit] = Command("SCRIPT", Command.NoKeys, Vector(Kill), Decode.ok)

  // Valkey-only: returns the source of a script previously loaded by its SHA
  def scriptShow(sha: String): Command[String] =
    Command("SCRIPT", Command.NoKeys, Vector(Show, Bytes.utf8(sha)), Decode.utf8String)
}

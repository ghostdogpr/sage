package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.KeyArgs.ScriptVerb
import sage.protocol.Frame

/**
  * Server-side Lua scripting. `EVAL`/`EVALSHA` and their `_RO` reads return the raw RESP3 [[Frame]], since a script's reply shape is
  * defined by user code. `SCRIPT LOAD`, `SCRIPT FLUSH` and `SCRIPT EXISTS` run on every master, because a cluster keeps no shared script
  * cache and a later key-routed `EVALSHA` must find the script wherever its keys live. `EXISTS` therefore reports a sha present only when
  * every master has it (a per-sha AND). A `true` reply guarantees that key-routed `EVALSHA` calls will find the script on their master.
  */
private[sage] object Scripting {

  private val Load   = Bytes.utf8("LOAD")
  private val Exists = Bytes.utf8("EXISTS")
  private val Flush  = Bytes.utf8("FLUSH")
  private val Kill   = Bytes.utf8("KILL")
  private val Show   = Bytes.utf8("SHOW")

  def eval(script: String): Command[Frame] = KeyArgs.scriptCall(ScriptVerb.Eval, script, Seq.empty[Bytes], Seq.empty[Bytes])

  def eval[K](script: String, keys: Seq[K])(using KeyCodec[K]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.Eval, script, keys, Seq.empty[Bytes])

  def eval[K, V](script: String, keys: Seq[K], args: Seq[V])(using KeyCodec[K], ValueCodec[V]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.Eval, script, keys, args)

  def evalRo(script: String): Command[Frame] = KeyArgs.scriptCall(ScriptVerb.EvalRo, script, Seq.empty[Bytes], Seq.empty[Bytes])

  def evalRo[K](script: String, keys: Seq[K])(using KeyCodec[K]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.EvalRo, script, keys, Seq.empty[Bytes])

  def evalRo[K, V](script: String, keys: Seq[K], args: Seq[V])(using KeyCodec[K], ValueCodec[V]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.EvalRo, script, keys, args)

  def evalSha(sha: String): Command[Frame] = KeyArgs.scriptCall(ScriptVerb.EvalSha, sha, Seq.empty[Bytes], Seq.empty[Bytes])

  def evalSha[K](sha: String, keys: Seq[K])(using KeyCodec[K]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.EvalSha, sha, keys, Seq.empty[Bytes])

  def evalSha[K, V](sha: String, keys: Seq[K], args: Seq[V])(using KeyCodec[K], ValueCodec[V]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.EvalSha, sha, keys, args)

  def evalShaRo(sha: String): Command[Frame] = KeyArgs.scriptCall(ScriptVerb.EvalShaRo, sha, Seq.empty[Bytes], Seq.empty[Bytes])

  def evalShaRo[K](sha: String, keys: Seq[K])(using KeyCodec[K]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.EvalShaRo, sha, keys, Seq.empty[Bytes])

  def evalShaRo[K, V](sha: String, keys: Seq[K], args: Seq[V])(using KeyCodec[K], ValueCodec[V]): Command[Frame] =
    KeyArgs.scriptCall(ScriptVerb.EvalShaRo, sha, keys, args)

  def scriptLoad(script: String): Command[String] =
    Command("SCRIPT", Command.NoKeys, Vector(Load, Bytes.utf8(script)), Decode.utf8String, allMasters = true)

  private val existsFlags = Decode.vector(Decode.flag)

  private val existsAnd =
    Merge.typed[Vector[Boolean]](existsFlags, flags => Frame.Array(flags.map(f => Frame.Integer(if (f) 1L else 0L)))) { (xs, ys) =>
      if (xs.length == ys.length) xs.lazyZip(ys).map(_ && _)
      else throw DecodeError("SCRIPT EXISTS per-master flag arrays of equal length", s"${xs.length} and ${ys.length} flags")
    }

  def scriptExists(first: String, rest: String*): Command[Vector[Boolean]] =
    Command(
      "SCRIPT",
      Command.NoKeys,
      Exists +: (first +: rest).iterator.map(Bytes.utf8).toVector,
      existsFlags,
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

// a Lua script that declares exactly one key, sent by digest (EVALSHA) or by body (EVAL)
final private[sage] class SingleKeyScript(source: String) {
  private val body   = Bytes.utf8(source)
  val sha: String    = java.security.MessageDigest.getInstance("SHA-1").digest(body.toArray).iterator.map(b => f"${b & 0xff}%02x").mkString
  private val digest = Bytes.utf8(sha)

  def verb(cached: Boolean): String     = if (cached) "EVALSHA" else "EVAL"
  def reference(cached: Boolean): Bytes = if (cached) digest else body
}

private[sage] object SingleKeyScript {
  // the arguments are the script reference, numkeys = 1, the key, then ARGV
  val NumKeys: Bytes          = Bytes.utf8("1")
  val KeyIndices: Vector[Int] = Vector(2)

  // length framing distinguishes namespace `a` with key `b:c` from namespace `a:b` with key `c`
  def namespaced(namespace: String): Bytes => Bytes = {
    val ns     = Bytes.utf8(namespace)
    val prefix = Bytes.concat(Vector(Bytes.utf8(s"${ns.length}:"), ns, Bytes.utf8(":")))
    key => Bytes.concat(Vector(prefix, key))
  }
}

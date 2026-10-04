package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

private[commands] object KeyArgs {

  def allKeys[K, Out](name: String, keys: Vector[K], decode: Frame => Either[DecodeError, Out], readOnly: Boolean = false)(
    using keyCodec: KeyCodec[K]
  ): Command[Out] = {
    val args = keys.map(keyCodec.encode)
    if (readOnly) Command.read(name, args.indices.toVector, args, decode)
    else Command(name, args.indices.toVector, args, decode)
  }

  // the `numkeys key…` block: encoded keys prefixed by their count, with 1-based key positions
  def numKeyed[K](keys: Vector[K])(using keyCodec: KeyCodec[K]): (Vector[Int], Vector[Bytes]) = {
    val encoded = keys.map(keyCodec.encode)
    (Vector.tabulate(encoded.size)(_ + 1), Args.long(encoded.size) +: encoded)
  }

  // `leading numkeys key…`, the layout of BLMPOP, BZMPOP, the sorted-set *STORE commands, EVAL and FCALL
  def numKeyedAfter[K](leading: Bytes, keys: Seq[K])(using keyCodec: KeyCodec[K]): (Vector[Int], Vector[Bytes]) = {
    val encoded = keys.iterator.map(keyCodec.encode).toVector
    (Vector.range(2, 2 + encoded.length), leading +: Args.long(encoded.length) +: encoded)
  }

  enum ScriptVerb(val wire: String, val readOnly: Boolean) {
    case Eval      extends ScriptVerb("EVAL", false)
    case EvalRo    extends ScriptVerb("EVAL_RO", true)
    case EvalSha   extends ScriptVerb("EVALSHA", false)
    case EvalShaRo extends ScriptVerb("EVALSHA_RO", true)
    case FCall     extends ScriptVerb("FCALL", false)
    case FCallRo   extends ScriptVerb("FCALL_RO", true)
  }

  def scriptCall[K, V](verb: ScriptVerb, target: String, keys: Seq[K], args: Seq[V])(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Frame] = {
    val (indices, prefix) = numKeyedAfter(Bytes.utf8(target), keys)
    Command(verb.wire, indices, prefix ++ args.iterator.map(valueCodec.encode), Decode.frame, isReadOnly = verb.readOnly)
  }

  // `[timeout] numkeys key… where [COUNT count]`, the layout of LMPOP, ZMPOP and their blocking forms
  def multiPop[K, A](
    name: String,
    timeout: Option[BlockTimeout],
    keys: Vector[K],
    where: Bytes,
    count: Option[Long],
    items: Frame => Either[DecodeError, A],
    label: String
  )(using keyCodec: KeyCodec[K]): Command[Option[(K, A)]] = {
    val (indices, prefix) = timeout match {
      case None          => numKeyed(keys)
      case Some(timeout) => numKeyedAfter(BlockTimeout.wire(timeout), keys)
    }
    val decode            = Decode.nullable(Decode.array2(Decode.key[K], items, label)(_ -> _))
    Command(
      name,
      indices,
      (prefix :+ where) ++ Args.optLong(Args.Count, count),
      decode,
      if (timeout.isEmpty) Execution.Ordinary else Execution.Blocking
    )
  }

  def interCard[K](name: String, keys: Vector[K], limit: Option[Long])(using keyCodec: KeyCodec[K]): Command[Long] = {
    val (indices, prefix) = numKeyed(keys)
    Command.read(name, indices, prefix ++ Args.optLong(Args.LimitWord, limit), Decode.long)
  }

  def keyScan[K, A](name: String, key: K, cursor: ScanCursor, pattern: Option[String], count: Option[Long], tail: Vector[Bytes] = Vector.empty)(
    items: Frame => Either[DecodeError, Vector[A]]
  )(using keyCodec: KeyCodec[K]): Command[ScanPage[A]] =
    Command.readCursor(
      name,
      Command.FirstKey,
      (Vector(keyCodec.encode(key), ScanCursor.bytes(cursor)) ++ Args.scanOptions(pattern, count)) ++ tail,
      Decode.scanPage(items)
    )

  def blockingPop[K, A](name: String, keys: Vector[K], timeout: BlockTimeout, decode: Frame => Either[DecodeError, A])(
    using keyCodec: KeyCodec[K]
  ): Command[Option[A]] = {
    val encoded = keys.map(keyCodec.encode)
    Command(name, Vector.range(0, encoded.size), encoded :+ BlockTimeout.wire(timeout), Decode.nullable(decode), Execution.Blocking)
  }
}

package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.Args.{LimitWord, Match, Max, Min, Rev, WithValues}
import sage.protocol.Frame

/**
  * An `ARGREP` predicate over an array's text values. `Exact` compares the whole value, `Match` searches for a substring, `Glob` uses a glob
  * pattern, and `Re` uses a regular expression. The array grep command searches stored strings and does not decode them with a value codec.
  */
enum ArMatch {
  case Exact(value: String)
  case Match(substring: String)
  case Glob(pattern: String)
  case Re(pattern: String)
}

/**
  * How multiple `ARGREP` predicates combine. `Or` is the server default.
  */
enum ArGrepCombine {
  case And, Or
}

/**
  * `ARINFO`: an Array's metadata. `count`/`len`/`nextInsertIndex` are the user-meaningful invariants; the structural fields are decoded
  * leniently since the Array type ships as a Redis preview and its internal layout reporting may still change.
  */
final case class ArrayInfo(
  count: Long,
  len: Long,
  nextInsertIndex: Long,
  slices: Option[Long],
  directorySize: Option[Long],
  superDirEntries: Option[Long],
  sliceSize: Option[Long]
)

/**
  * `ARINFO ... FULL`: [[ArrayInfo]] plus per-slice fill statistics, all decoded leniently.
  */
final case class ArrayInfoFull(
  count: Long,
  len: Long,
  nextInsertIndex: Long,
  slices: Option[Long],
  directorySize: Option[Long],
  superDirEntries: Option[Long],
  sliceSize: Option[Long],
  denseSlices: Option[Long],
  sparseSlices: Option[Long],
  avgDenseSize: Option[Double],
  avgDenseFill: Option[Double],
  avgSparseSize: Option[Double]
)

/**
  * The Array data type (`AR*`): a sparse, integer-indexed map of index to value with a write cursor and a ring-buffer mode. Redis-only
  * (no Valkey counterpart) and shipped as a preview. Every command is keyed at the first argument. Indices are `Long`; the type's
  * documented `2^64` index space above `Long.MaxValue` is not addressable because this API uses `Long` for offsets.
  */
private[sage] object Arrays {

  private val NoCaseWord = Bytes.utf8("NOCASE")
  private val And        = Bytes.utf8("AND")
  private val Or         = Bytes.utf8("OR")
  private val combineArg = Args.keywords(ArGrepCombine.values)
  private val Full       = Bytes.utf8("FULL")
  private val Exact      = Bytes.utf8("EXACT")
  private val Glob       = Bytes.utf8("GLOB")
  private val Re         = Bytes.utf8("RE")
  private val Sum        = Bytes.utf8("SUM")
  private val Xor        = Bytes.utf8("XOR")
  private val Used       = Bytes.utf8("USED")

  private def idx(i: Long): Bytes = Args.long(i)

  def arSet[K, V](key: K, index: Long, first: V, rest: V*)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Long] =
    Command("ARSET", Command.FirstKey, k.encode(key) +: idx(index) +: (first +: rest.toVector).map(v.encode), Decode.long)

  def arMSet[K, V](key: K, first: (Long, V), rest: (Long, V)*)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Long] =
    Command(
      "ARMSET",
      Command.FirstKey,
      k.encode(key) +: (first +: rest.toVector).flatMap { case (i, value) => Vector(idx(i), v.encode(value)) },
      Decode.long
    )

  def arGet[K, V](key: K, index: Long)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Option[V]] =
    Command.read("ARGET", Command.FirstKey, Vector(k.encode(key), idx(index)), Decode.optionalValue)

  def arMGet[K, V](key: K, first: Long, rest: Long*)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Vector[Option[V]]] =
    Command.read("ARMGET", Command.FirstKey, Args.keyThen(key, first, rest)(idx), Decode.vector(Decode.optionalValue))

  def arLen[K](key: K)(using k: KeyCodec[K]): Command[Long] =
    Command.read("ARLEN", Command.FirstKey, Vector(k.encode(key)), Decode.long)

  def arCount[K](key: K)(using k: KeyCodec[K]): Command[Long] =
    Command.read("ARCOUNT", Command.FirstKey, Vector(k.encode(key)), Decode.long)

  def arGetRange[K, V](key: K, start: Long, end: Long)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Vector[Option[V]]] =
    Command.read("ARGETRANGE", Command.FirstKey, Vector(k.encode(key), idx(start), idx(end)), Decode.vector(Decode.optionalValue))

  def arRing[K, V](key: K, size: Long, first: V, rest: V*)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Long] =
    Command("ARRING", Command.FirstKey, k.encode(key) +: idx(size) +: (first +: rest.toVector).map(v.encode), Decode.long)

  def arLastItems[K, V](key: K, count: Long, rev: Boolean = false)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Vector[V]] =
    Command.read(
      "ARLASTITEMS",
      Command.FirstKey,
      Vector(k.encode(key), idx(count)) ++ Args.flag(rev, Rev),
      Decode.vector(Decode.value)
    )

  def arDel[K](key: K, first: Long, rest: Long*)(using k: KeyCodec[K]): Command[Long] =
    Command("ARDEL", Command.FirstKey, Args.keyThen(key, first, rest)(idx), Decode.long)

  def arDelRange[K](key: K, first: (Long, Long), rest: (Long, Long)*)(using k: KeyCodec[K]): Command[Long] =
    Command(
      "ARDELRANGE",
      Command.FirstKey,
      k.encode(key) +: (first +: rest.toVector).flatMap { case (s, e) => Vector(idx(s), idx(e)) },
      Decode.long
    )

  def arInsert[K, V](key: K, first: V, rest: V*)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Long] =
    Command("ARINSERT", Command.FirstKey, Args.keyThen(key, first, rest)(v.encode), Decode.long)

  def arNext[K](key: K)(using k: KeyCodec[K]): Command[Option[Long]] =
    Command.read("ARNEXT", Command.FirstKey, Vector(k.encode(key)), Decode.optionalLong)

  def arSeek[K](key: K, index: Long)(using k: KeyCodec[K]): Command[Boolean] =
    Command("ARSEEK", Command.FirstKey, Vector(k.encode(key), idx(index)), Decode.flag)

  def arScan[K, V](key: K, start: Long, end: Long, limit: Option[Long] = None)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Vector[(Long, V)]] =
    Command.read(
      "ARSCAN",
      Command.FirstKey,
      Vector(k.encode(key), idx(start), idx(end)) ++ Args.optLong(LimitWord, limit),
      indexValuePairs
    )

  def arGrep[K](key: K, start: Long, end: Long, combine: ArGrepCombine = ArGrepCombine.Or, limit: Option[Long] = None, noCase: Boolean = false)(
    first: ArMatch,
    rest: ArMatch*
  )(using k: KeyCodec[K]): Command[Vector[Long]] =
    Command.read(
      "ARGREP",
      Command.FirstKey,
      grepArgs(k.encode(key), start, end, first, rest, combine, limit, noCase, withValues = false),
      Decode.vector(Decode.long)
    )

  def arGrepWithValues[K, V](
    key: K,
    start: Long,
    end: Long,
    combine: ArGrepCombine = ArGrepCombine.Or,
    limit: Option[Long] = None,
    noCase: Boolean = false
  )(first: ArMatch, rest: ArMatch*)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Vector[(Long, V)]] =
    Command.read(
      "ARGREP",
      Command.FirstKey,
      grepArgs(k.encode(key), start, end, first, rest, combine, limit, noCase, withValues = true),
      indexValuePairs
    )

  def arOpSum[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Double]] = arop(key, start, end, Sum, Decode.optionalDouble)
  def arOpMin[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Double]] = arop(key, start, end, Min, Decode.optionalDouble)
  def arOpMax[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Double]] = arop(key, start, end, Max, Decode.optionalDouble)
  def arOpAnd[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Long]]   = arop(key, start, end, And, Decode.optionalLong)
  def arOpOr[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Long]]    = arop(key, start, end, Or, Decode.optionalLong)
  def arOpXor[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Long]]   = arop(key, start, end, Xor, Decode.optionalLong)

  def arOpUsed[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Long] = arop(key, start, end, Used, Decode.long)

  def arOpMatch[K, V](key: K, start: Long, end: Long, value: V)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Long] =
    Command.read("AROP", Command.FirstKey, Vector(k.encode(key), idx(start), idx(end), Match, v.encode(value)), Decode.long)

  def arInfo[K](key: K)(using k: KeyCodec[K]): Command[ArrayInfo] =
    Command.read("ARINFO", Command.FirstKey, Vector(k.encode(key)), decodeInfo)

  def arInfoFull[K](key: K)(using k: KeyCodec[K]): Command[ArrayInfoFull] =
    Command.read("ARINFO", Command.FirstKey, Vector(k.encode(key), Full), decodeInfoFull)

  // --- helpers ---------------------------------------------------------------------------------------------------------------------------

  private def arop[K, A](key: K, start: Long, end: Long, op: Bytes, decode: Frame => Either[DecodeError, A])(using k: KeyCodec[K]): Command[A] =
    Command.read("AROP", Command.FirstKey, Vector(k.encode(key), idx(start), idx(end), op), decode)

  private def grepArgs(
    key: Bytes,
    start: Long,
    end: Long,
    first: ArMatch,
    rest: Seq[ArMatch],
    combine: ArGrepCombine,
    limit: Option[Long],
    noCase: Boolean,
    withValues: Boolean
  ): Vector[Bytes] = {
    val predTokens = rest.foldLeft(predicateArgs(first))((acc, p) => (acc :+ combineArg(combine)) ++ predicateArgs(p))
    Vector(key, idx(start), idx(end)) ++ predTokens ++
      Args.optLong(LimitWord, limit) ++
      Args.flag(withValues, WithValues) ++
      Args.flag(noCase, NoCaseWord)
  }

  private def predicateArgs(predicate: ArMatch): Vector[Bytes] =
    predicate match {
      case ArMatch.Exact(value)     => Vector(Exact, Bytes.utf8(value))
      case ArMatch.Match(substring) => Vector(Match, Bytes.utf8(substring))
      case ArMatch.Glob(pattern)    => Vector(Glob, Bytes.utf8(pattern))
      case ArMatch.Re(pattern)      => Vector(Re, Bytes.utf8(pattern))
    }

  // ARSCAN and ARGREP WITHVALUES reply with an array of [index, value] pairs (index an integer; empty slots skipped)
  private def indexValuePairs[V](using v: ValueCodec[V]): Frame => Either[DecodeError, Vector[(Long, V)]] =
    Decode.vector {
      case Frame.Array(Vector(Frame.Integer(index), valueFrame)) => Decode.value(valueFrame).map(index -> _)
      case other                                                 => Left(DecodeError("[index, value] pair", Frame.describe(other)))
    }

  private val decodeInfoFull: Frame => Either[DecodeError, ArrayInfoFull] =
    Decode.fields { fields =>
      for {
        count <- fields.required("count", Decode.long)
        len   <- fields.required("len", Decode.long)
        next  <- fields.required("next-insert-index", Decode.long)
      } yield ArrayInfoFull(
        count,
        len,
        next,
        optLong(fields, "slices"),
        optLong(fields, "directory-size"),
        optLong(fields, "super-dir-entries"),
        optLong(fields, "slice-size"),
        optLong(fields, "dense-slices"),
        optLong(fields, "sparse-slices"),
        optDouble(fields, "avg-dense-size"),
        optDouble(fields, "avg-dense-fill"),
        optDouble(fields, "avg-sparse-size")
      )
    }

  private val decodeInfo: Frame => Either[DecodeError, ArrayInfo] =
    frame => decodeInfoFull(frame).map(f => ArrayInfo(f.count, f.len, f.nextInsertIndex, f.slices, f.directorySize, f.superDirEntries, f.sliceSize))

  private def optLong(fields: Decode.Fields, name: String): Option[Long] =
    fields.get(name).collect { case Frame.Integer(n) => n }

  private def optDouble(fields: Decode.Fields, name: String): Option[Double] =
    fields.get(name).collect {
      case Frame.Double(d)  => d
      case Frame.Integer(n) => n.toDouble
    }
}

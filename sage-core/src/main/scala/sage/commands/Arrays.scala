package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * An `ARGREP` predicate over an Array's textual values: `Exact` whole-value equality, `Match` substring containment, `Glob` a glob
  * pattern, `Re` a regular expression. Patterns are textual (not value-codec-decoded) — Array grep is a search over the stored strings.
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
  * leniently (ADR-0024) since the Array type ships as a Redis preview and its internal layout reporting may still change.
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
  * `ARINFO ... FULL`: [[ArrayInfo]] plus per-slice fill statistics, all decoded leniently (ADR-0024).
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
  * documented `2^64` index space above `Long.MaxValue` is not addressable, matching how the rest of the API carries offsets.
  */
private[sage] object Arrays {

  private val Rev        = Bytes.utf8("REV")
  private val WithValues = Bytes.utf8("WITHVALUES")
  private val LimitWord  = Bytes.utf8("LIMIT")
  private val NoCaseWord = Bytes.utf8("NOCASE")
  private val And        = Bytes.utf8("AND")
  private val Or         = Bytes.utf8("OR")
  private val Full       = Bytes.utf8("FULL")
  private val Exact      = Bytes.utf8("EXACT")
  private val MatchWord  = Bytes.utf8("MATCH")
  private val Glob       = Bytes.utf8("GLOB")
  private val Re         = Bytes.utf8("RE")
  private val Sum        = Bytes.utf8("SUM")
  private val Min        = Bytes.utf8("MIN")
  private val Max        = Bytes.utf8("MAX")
  private val Xor        = Bytes.utf8("XOR")
  private val Used       = Bytes.utf8("USED")

  private def idx(i: Long): Bytes = Bytes.utf8(i.toString)

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
    Command.read("ARMGET", Command.FirstKey, k.encode(key) +: (first +: rest.toVector).map(idx), Decode.vector(Decode.optionalValue))

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
      Vector(k.encode(key), idx(count)) ++ (if (rev) Vector(Rev) else Vector.empty),
      Decode.vector(Decode.value)
    )

  def arDel[K](key: K, first: Long, rest: Long*)(using k: KeyCodec[K]): Command[Long] =
    Command("ARDEL", Command.FirstKey, k.encode(key) +: (first +: rest.toVector).map(idx), Decode.long)

  def arDelRange[K](key: K, first: (Long, Long), rest: (Long, Long)*)(using k: KeyCodec[K]): Command[Long] =
    Command(
      "ARDELRANGE",
      Command.FirstKey,
      k.encode(key) +: (first +: rest.toVector).flatMap { case (s, e) => Vector(idx(s), idx(e)) },
      Decode.long
    )

  def arInsert[K, V](key: K, first: V, rest: V*)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Long] =
    Command("ARINSERT", Command.FirstKey, k.encode(key) +: (first +: rest.toVector).map(v.encode), Decode.long)

  def arNext[K](key: K)(using k: KeyCodec[K]): Command[Option[Long]] =
    Command.read("ARNEXT", Command.FirstKey, Vector(k.encode(key)), Decode.optionalLong)

  def arSeek[K](key: K, index: Long)(using k: KeyCodec[K]): Command[Boolean] =
    Command("ARSEEK", Command.FirstKey, Vector(k.encode(key), idx(index)), Decode.flag)

  def arScan[K, V](key: K, start: Long, end: Long, limit: Option[Long] = None)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Vector[(Long, V)]] =
    Command.read(
      "ARSCAN",
      Command.FirstKey,
      Vector(k.encode(key), idx(start), idx(end)) ++ limit.toVector.flatMap(l => Vector(LimitWord, idx(l))),
      indexValuePairs
    )

  def arGrep[K](key: K, start: Long, end: Long, combine: ArGrepCombine = ArGrepCombine.Or, limit: Option[Long] = None, noCase: Boolean = false)(
    first: ArMatch,
    rest: ArMatch*
  )(using k: KeyCodec[K]): Command[Vector[Long]] =
    Command.read(
      "ARGREP",
      Command.FirstKey,
      grepArgs(k.encode(key), start, end, first +: rest.toVector, combine, limit, noCase, withValues = false),
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
      grepArgs(k.encode(key), start, end, first +: rest.toVector, combine, limit, noCase, withValues = true),
      indexValuePairs
    )

  def arOpSum[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Double]] = aropDouble(key, start, end, Sum)
  def arOpMin[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Double]] = aropDouble(key, start, end, Min)
  def arOpMax[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Double]] = aropDouble(key, start, end, Max)
  def arOpAnd[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Long]]   = aropLong(key, start, end, And)
  def arOpOr[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Long]]    = aropLong(key, start, end, Or)
  def arOpXor[K](key: K, start: Long, end: Long)(using KeyCodec[K]): Command[Option[Long]]   = aropLong(key, start, end, Xor)

  def arOpUsed[K](key: K, start: Long, end: Long)(using k: KeyCodec[K]): Command[Long] =
    Command.read("AROP", Command.FirstKey, Vector(k.encode(key), idx(start), idx(end), Used), Decode.long)

  def arOpMatch[K, V](key: K, start: Long, end: Long, value: V)(using k: KeyCodec[K], v: ValueCodec[V]): Command[Long] =
    Command.read("AROP", Command.FirstKey, Vector(k.encode(key), idx(start), idx(end), MatchWord, v.encode(value)), Decode.long)

  def arInfo[K](key: K)(using k: KeyCodec[K]): Command[ArrayInfo] =
    Command.read("ARINFO", Command.FirstKey, Vector(k.encode(key)), decodeInfo)

  def arInfoFull[K](key: K)(using k: KeyCodec[K]): Command[ArrayInfoFull] =
    Command.read("ARINFO", Command.FirstKey, Vector(k.encode(key), Full), decodeInfoFull)

  // --- helpers ---------------------------------------------------------------------------------------------------------------------------

  private def aropDouble[K](key: K, start: Long, end: Long, op: Bytes)(using k: KeyCodec[K]): Command[Option[Double]] =
    Command.read("AROP", Command.FirstKey, Vector(k.encode(key), idx(start), idx(end), op), Decode.optionalDouble)

  private def aropLong[K](key: K, start: Long, end: Long, op: Bytes)(using k: KeyCodec[K]): Command[Option[Long]] =
    Command.read("AROP", Command.FirstKey, Vector(k.encode(key), idx(start), idx(end), op), Decode.optionalLong)

  private def grepArgs(
    key: Bytes,
    start: Long,
    end: Long,
    predicates: Vector[ArMatch],
    combine: ArGrepCombine,
    limit: Option[Long],
    noCase: Boolean,
    withValues: Boolean
  ): Vector[Bytes] = {
    val combineToken = combine match {
      case ArGrepCombine.And => And
      case ArGrepCombine.Or  => Or
    }
    val predTokens   = predicates.map(predicateArgs).reduce((a, b) => (a :+ combineToken) ++ b)
    Vector(key, idx(start), idx(end)) ++ predTokens ++
      limit.toVector.flatMap(l => Vector(LimitWord, idx(l))) ++
      (if (withValues) Vector(WithValues) else Vector.empty) ++
      (if (noCase) Vector(NoCaseWord) else Vector.empty)
  }

  private def predicateArgs(predicate: ArMatch): Vector[Bytes] =
    predicate match {
      case ArMatch.Exact(value)     => Vector(Exact, Bytes.utf8(value))
      case ArMatch.Match(substring) => Vector(MatchWord, Bytes.utf8(substring))
      case ArMatch.Glob(pattern)    => Vector(Glob, Bytes.utf8(pattern))
      case ArMatch.Re(pattern)      => Vector(Re, Bytes.utf8(pattern))
    }

  // ARSCAN and ARGREP WITHVALUES reply with an array of [index, value] pairs (index an integer; empty slots skipped)
  private def indexValuePairs[V](using v: ValueCodec[V]): Frame => Either[DecodeError, Vector[(Long, V)]] =
    Decode.vector {
      case Frame.Array(Vector(Frame.Integer(index), valueFrame)) => Decode.value(valueFrame).map(index -> _)
      case other                                                 => Left(DecodeError("[index, value] pair", Frame.describe(other)))
    }

  private val decodeInfo: Frame => Either[DecodeError, ArrayInfo] =
    frame =>
      Decode.fieldMap(frame).flatMap { fields =>
        for {
          count <- longField(fields, "count")
          len   <- longField(fields, "len")
          next  <- longField(fields, "next-insert-index")
        } yield ArrayInfo(
          count,
          len,
          next,
          optLong(fields, "slices"),
          optLong(fields, "directory-size"),
          optLong(fields, "super-dir-entries"),
          optLong(fields, "slice-size")
        )
      }

  private val decodeInfoFull: Frame => Either[DecodeError, ArrayInfoFull] =
    frame =>
      Decode.fieldMap(frame).flatMap { fields =>
        for {
          count <- longField(fields, "count")
          len   <- longField(fields, "len")
          next  <- longField(fields, "next-insert-index")
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

  private def longField(fields: Map[String, Frame], name: String): Either[DecodeError, Long] =
    fields.get(name) match {
      case Some(Frame.Integer(n)) => Right(n)
      case Some(other)            => Left(DecodeError(s"ARINFO $name integer", Frame.describe(other)))
      case None                   => Left(DecodeError(s"ARINFO $name", "absent"))
    }

  private def optLong(fields: Map[String, Frame], name: String): Option[Long] =
    fields.get(name).collect { case Frame.Integer(n) => n }

  private def optDouble(fields: Map[String, Frame], name: String): Option[Double] =
    fields.get(name).collect {
      case Frame.Double(d)  => d
      case Frame.Integer(n) => n.toDouble
    }
}

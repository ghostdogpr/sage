package sage.commands

import java.time.Instant

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{Doubles, KeyCodec, Primitives, ValueCodec}
import sage.protocol.Frame

private[commands] object Decode {

  // a decoder for the frames `accept` handles; any other frame fails with a mismatch against `expected`
  def shape[A](expected: String)(accept: PartialFunction[Frame, Either[DecodeError, A]]): Frame => Either[DecodeError, A] = {
    val mismatch: Frame => Either[DecodeError, A] = other => Left(DecodeError(expected, Frame.describe(other)))
    frame => accept.applyOrElse(frame, mismatch)
  }

  val frame: Frame => Either[DecodeError, Frame] = Right(_)

  val long: Frame => Either[DecodeError, Long] = shape("integer") { case Frame.Integer(value) =>
    Right(value)
  }

  val millisDuration: Frame => Either[DecodeError, FiniteDuration] = shape("integer") { case Frame.Integer(ms) =>
    Right(FiniteDuration(ms, MILLISECONDS))
  }

  val millisInstant: Frame => Either[DecodeError, Instant] = shape("integer") { case Frame.Integer(ms) => Right(Instant.ofEpochMilli(ms)) }

  def decimal(expected: String): Frame => Either[DecodeError, Long] =
    shape(expected) { case Frame.BulkString(text) => Primitives.decodeLong(expected, Long.MinValue, Long.MaxValue)(text) }

  val flag: Frame => Either[DecodeError, Boolean] = shape("integer 0 or 1") {
    case Frame.Integer(0) => Right(false)
    case Frame.Integer(1) => Right(true)
  }

  val ok: Frame => Either[DecodeError, Unit] = shape("simple string 'OK'") { case Frame.SimpleString("OK") =>
    Right(())
  }

  val okOrNull: Frame => Either[DecodeError, Boolean] = shape("simple string 'OK' or null") {
    case Frame.SimpleString("OK") => Right(true)
    case Frame.Null               => Right(false)
  }

  // Decode the integer format shared by TTL, EXPIRETIME, HTTL, and related commands. -2 means absent, -1 means no expiry, and a non-negative
  // value contains the expiry result.
  def expiryInteger[A](absent: A, noExpiry: A, expected: String)(present: Long => A): Frame => Either[DecodeError, A] = {
    case Frame.Integer(-2)                    => Right(absent)
    case Frame.Integer(-1)                    => Right(noExpiry)
    case Frame.Integer(amount) if amount >= 0 => Right(present(amount))
    case other                                => Left(DecodeError(expected, Frame.describe(other)))
  }

  def value[V](using codec: ValueCodec[V]): Frame => Either[DecodeError, V] = {
    case Frame.BulkString(bytes) => codec.decode(bytes)
    case other                   => Left(DecodeError("bulk string", Frame.describe(other)))
  }

  def optionalValue[V](using codec: ValueCodec[V]): Frame => Either[DecodeError, Option[V]] = {
    case Frame.Null              => Right(None)
    case Frame.BulkString(bytes) => codec.decode(bytes).map(Some(_))
    case other                   => Left(DecodeError("bulk string or null", Frame.describe(other)))
  }

  val utf8String: Frame => Either[DecodeError, String] = shape("bulk string") { case Frame.BulkString(bytes) =>
    Right(bytes.asUtf8String)
  }

  // text however the server framed it: simple, bulk, or the RESP3 verbatim form INFO/CLIENT INFO/CLUSTER NODES use
  val text: Frame => Either[DecodeError, String] = shape("string") {
    case Frame.SimpleString(value)      => Right(value)
    case Frame.BulkString(bytes)        => Right(bytes.asUtf8String)
    case Frame.VerbatimString(_, bytes) => Right(bytes.asUtf8String)
  }

  val optionalUtf8String: Frame => Either[DecodeError, Option[String]] = shape("bulk string or null") {
    case Frame.Null              => Right(None)
    case Frame.BulkString(bytes) => Right(Some(bytes.asUtf8String))
  }

  val bytes: Frame => Either[DecodeError, Bytes] = shape("bulk string") { case Frame.BulkString(value) =>
    Right(value)
  }

  val optionalBytes: Frame => Either[DecodeError, Option[Bytes]] = shape("bulk string or null") {
    case Frame.Null              => Right(None)
    case Frame.BulkString(value) => Right(Some(value))
  }

  def key[K](using codec: KeyCodec[K]): Frame => Either[DecodeError, K] = {
    case Frame.BulkString(bytes) => codec.decode(bytes)
    case other                   => Left(DecodeError("bulk string", Frame.describe(other)))
  }

  def optionalKey[K](using codec: KeyCodec[K]): Frame => Either[DecodeError, Option[K]] = {
    case Frame.Null              => Right(None)
    case Frame.BulkString(bytes) => codec.decode(bytes).map(Some(_))
    case other                   => Left(DecodeError("bulk string or null", Frame.describe(other)))
  }

  val optionalLong: Frame => Either[DecodeError, Option[Long]] = shape("integer or null") {
    case Frame.Null           => Right(None)
    case Frame.Integer(value) => Right(Some(value))
  }

  // a double however the server framed it: a RESP3 Double (scores), or a bulk string (INCRBYFLOAT, ZSCAN scores, geo under RESP2)
  val double: Frame => Either[DecodeError, Double] = shape("double") {
    case Frame.Double(value)     => Right(value)
    case Frame.BulkString(bytes) =>
      val text = bytes.asUtf8String
      Doubles.parse(text).toRight(DecodeError("double", s"bulk string '$text'"))
  }

  // GEODIST replies the distance as a double, or null when a member is absent
  val optionalDouble: Frame => Either[DecodeError, Option[Double]] = nullable(double)

  def nullable[A](decode: Frame => Either[DecodeError, A]): Frame => Either[DecodeError, Option[A]] = {
    case Frame.Null => Right(None)
    case other      => decode(other).map(Some(_))
  }

  private def buildEach[A, B, C](items: IterableOnce[A], builder: mutable.Builder[B, C])(
    f: A => Either[DecodeError, B]
  ): Either[DecodeError, C] = {
    val known = items.knownSize
    if (known > 0) builder.sizeHint(known)
    val it    = items.iterator
    while (it.hasNext)
      f(it.next()) match {
        case Right(value) => builder += value
        case Left(error)  => return Left(error)
      }
    Right(builder.result())
  }

  def mapEntries[A, K, V](items: IterableOnce[A])(f: A => Either[DecodeError, (K, V)]): Either[DecodeError, Map[K, V]] =
    buildEach(items, Map.newBuilder[K, V])(f)

  def each[A, B](items: IterableOnce[A])(f: A => Either[DecodeError, B]): Either[DecodeError, Vector[B]] =
    buildEach(items, Vector.newBuilder[B])(f)

  def flatPairsOf[B](label: String)(f: (Frame, Frame) => Either[DecodeError, B]): Frame => Either[DecodeError, Vector[B]] = {
    case array: Frame.Array => buildPairs(array, label)(f)
    case other              => Left(DecodeError(label, Frame.describe(other)))
  }

  // steps a flat alternating array two elements at a time, without grouped(2)'s throwaway 2-element Vector per pair
  private def buildPairs[B](array: Frame.Array, label: String)(f: (Frame, Frame) => Either[DecodeError, B]): Either[DecodeError, Vector[B]] = {
    val elements = array.elements
    if (elements.length % 2 != 0) return Left(DecodeError(label, Frame.describe(array)))
    val builder = Vector.newBuilder[B]
    builder.sizeHint(elements.length / 2)
    var i       = 0
    while (i < elements.length) {
      f(elements(i), elements(i + 1)) match {
        case Right(value) => builder += value
        case Left(error)  => return Left(error)
      }
      i += 2
    }
    Right(builder.result())
  }

  // a fixed-arity RESP Array decoded position by position; `label` is the structural description reported on a shape mismatch
  def array2[A, B, R](a: Frame => Either[DecodeError, A], b: Frame => Either[DecodeError, B], label: String)(
    combine: (A, B) => R
  ): Frame => Either[DecodeError, R] = {
    case Frame.Array(Vector(fa, fb)) =>
      for {
        x <- a(fa)
        y <- b(fb)
      } yield combine(x, y)
    case other                       => Left(DecodeError(label, Frame.describe(other)))
  }

  def array3[A, B, C, R](
    a: Frame => Either[DecodeError, A],
    b: Frame => Either[DecodeError, B],
    c: Frame => Either[DecodeError, C],
    label: String
  )(combine: (A, B, C) => R): Frame => Either[DecodeError, R] = {
    case Frame.Array(Vector(fa, fb, fc)) =>
      for {
        x <- a(fa)
        y <- b(fb)
        z <- c(fc)
      } yield combine(x, y, z)
    case other                           => Left(DecodeError(label, Frame.describe(other)))
  }

  def array4[A, B, C, D, R](
    a: Frame => Either[DecodeError, A],
    b: Frame => Either[DecodeError, B],
    c: Frame => Either[DecodeError, C],
    d: Frame => Either[DecodeError, D],
    label: String
  )(combine: (A, B, C, D) => R): Frame => Either[DecodeError, R] = {
    case Frame.Array(Vector(fa, fb, fc, fd)) =>
      for {
        w <- a(fa)
        x <- b(fb)
        y <- c(fc)
        z <- d(fd)
      } yield combine(w, x, y, z)
    case other                               => Left(DecodeError(label, Frame.describe(other)))
  }

  def byLowerName[E](cases: E*): Map[String, E] = cases.iterator.map(value => value.toString.toLowerCase(java.util.Locale.ROOT) -> value).toMap

  def vector[A](element: Frame => Either[DecodeError, A], expected: String = "array"): Frame => Either[DecodeError, Vector[A]] = {
    case Frame.Array(elements) => buildEach(elements, Vector.newBuilder[A])(element)
    case other                 => Left(DecodeError(expected, Frame.describe(other)))
  }

  // a missing list replies null where a present one replies an array; a stored list is never empty, so null collapses to an empty vector
  def orEmpty[A](decode: Frame => Either[DecodeError, Vector[A]]): Frame => Either[DecodeError, Vector[A]] = {
    case Frame.Null => Right(Vector.empty)
    case other      => decode(other)
  }

  // a RESP3 map, or the flat RESP2 array of alternating key/value some introspection replies still use; non-string keys are dropped
  val fieldMap: Frame => Either[DecodeError, Map[String, Frame]] = shape("map") {
    case Frame.Map(entries) => Right(entries.collect { case (Text(k), v) => k -> v }.toMap)
    case array: Frame.Array => buildPairs(array, "map")((k, v) => Right(Text.unapply(k).map(_ -> v))).map(_.flatten.toMap)
  }

  def fields[A](read: Fields => Either[DecodeError, A]): Frame => Either[DecodeError, A] = frame => fieldMap(frame).flatMap(m => read(new Fields(m)))

  def fieldValues[A](value: Frame => Either[DecodeError, A]): Frame => Either[DecodeError, Map[String, A]] =
    frame => fieldMap(frame).flatMap(mapEntries(_) { case (name, field) => value(field).map(name -> _) })

  // text framed as a bulk or simple string
  object Text {
    def unapply(frame: Frame): Option[String] =
      frame match {
        case Frame.BulkString(bytes)  => Some(bytes.asUtf8String)
        case Frame.SimpleString(name) => Some(name)
        case _                        => None
      }
  }

  /**
    * A lenient view over an introspection reply map: read fields by known name, ignore the rest.
    */
  final class Fields(table: Map[String, Frame]) {

    def get(name: String): Option[Frame] = table.get(name)

    def required[A](name: String, decode: Frame => Either[DecodeError, A]): Either[DecodeError, A] =
      table.get(name).toRight(DecodeError(s"field '$name'", "absent")).flatMap(decode)

    // a field that is core to the reply but whose absence on some server we tolerate with a default rather than failing the whole decode
    def requiredOr[A](name: String, decode: Frame => Either[DecodeError, A], fallback: A): Either[DecodeError, A] =
      table.get(name) match {
        case None | Some(Frame.Null) => Right(fallback)
        case Some(frame)             => decode(frame)
      }

    def optional[A](name: String, decode: Frame => Either[DecodeError, A]): Either[DecodeError, Option[A]] =
      table.get(name) match {
        case None | Some(Frame.Null) => Right(None)
        case Some(frame)             => decode(frame).map(Some(_))
      }

    def optionalVector[A](name: String, element: Frame => Either[DecodeError, A]): Either[DecodeError, Vector[A]] =
      requiredOr(name, vector(element), Vector.empty)
  }

  def pair[A, B](a: Frame => Either[DecodeError, A], b: Frame => Either[DecodeError, B]): (Frame, Frame) => Either[DecodeError, (A, B)] =
    (x, y) => a(x).flatMap(first => b(y).map(first -> _))

  def map[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Map[K, V]] = {
    val entry = pair(key[K], value[V]).tupled
    shape("map") { case Frame.Map(entries) => mapEntries(entries)(entry) }
  }

  // HSCAN's items are a flat field, value, field, value, … array; HRANDFIELD WITHVALUES nests each pair in its own array.
  def flatPairs[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Vector[(K, V)]] =
    flatPairsOf("array of field/value pairs")(pair(key[K], value[V]))

  def nestedPairs[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Vector[(K, V)]] =
    vector(array2(key[K], value[V], "field/value pair")(_ -> _), "array of field/value pairs")

  private val scanCursor: Frame => Either[DecodeError, Option[ScanCursor]] = shape("cursor bulk string") { case Frame.BulkString(bytes) =>
    Right(if (bytes.sameBytes(ScanCursor.bytes(ScanCursor.start))) None else Some(ScanCursor.wrap(bytes)))
  }

  def scanPage[A](items: Frame => Either[DecodeError, Vector[A]]): Frame => Either[DecodeError, ScanPage[A]] =
    array2(scanCursor, items, "array of cursor and items")((next, decoded) => ScanPage(decoded, next))

  // RESP3 returns set-typed replies (SMEMBERS, SINTER, …) as a Set frame, never an Array
  def set[V](using ValueCodec[V]): Frame => Either[DecodeError, Set[V]] = {
    case Frame.Set(elements) => buildEach(elements, Set.newBuilder[V])(value)
    case other               => Left(DecodeError("set", Frame.describe(other)))
  }

  val optionalScore: Frame => Either[DecodeError, Option[Double]] = shape("double or null") {
    case Frame.Null          => Right(None)
    case Frame.Double(value) => Right(Some(value))
  }

  private def memberScore[V](using ValueCodec[V]): Frame => Either[DecodeError, (V, Double)] =
    array2(value[V], double, "member/score pair")(_ -> _)

  // RESP3 nests each member with its Double score in a two-element array (ZRANGE WITHSCORES, ZPOPMIN count, …)
  def scoredMembers[V](using ValueCodec[V]): Frame => Either[DecodeError, Vector[(V, Double)]] =
    vector(memberScore[V], "array of member/score pairs")

  // ZPOPMIN/ZPOPMAX without a count: a flat [member, score], or an empty array when the key is absent
  def optionalScoredMember[V](using ValueCodec[V]): Frame => Either[DecodeError, Option[(V, Double)]] = {
    case Frame.Null                      => Right(None)
    case Frame.Array(Vector())           => Right(None)
    case arr @ Frame.Array(Vector(_, _)) => memberScore[V](arr).map(Some(_))
    case other                           => Left(DecodeError("member/score pair or empty array", Frame.describe(other)))
  }

  // ZSCAN's items are a flat member, score, member, score array with scores as bulk strings, not RESP3 doubles
  def scoredMembersFlat[V](using ValueCodec[V]): Frame => Either[DecodeError, Vector[(V, Double)]] =
    flatPairsOf("array of member/score pairs")(pair(value[V], double))
}

private[commands] object TimeArgs {

  def wholeSeconds(duration: FiniteDuration): Boolean = duration.toNanos % 1000000000L == 0

  def wholeSeconds(timestamp: Instant): Boolean = timestamp.getNano == 0

  // Keep second precision for whole seconds. Round finer durations up to the next millisecond so the encoded expiry is not earlier than the
  // requested time. Rounding down would encode a sub-millisecond duration as 0, which expires immediately.
  def millis(duration: FiniteDuration): Long = Math.ceilDiv(duration.toNanos, 1000000L)

  // for arguments where the wire value 0 means "no timeout" or "no expiry"
  def positiveMillis(duration: FiniteDuration): Long = Math.max(1L, millis(duration))

  def positiveMillis(timestamp: Instant): Long = Math.max(1L, millis(timestamp))

  // Use saturating arithmetic for an Instant outside the millisecond range. This avoids an overflow while building the command and preserves
  // upward rounding at the maximum value.
  def millis(timestamp: Instant): Long =
    satAdd(satMul(timestamp.getEpochSecond, 1000L), Math.ceilDiv(timestamp.getNano.toLong, 1000000L))

  private def satMul(a: Long, b: Long): Long =
    try Math.multiplyExact(a, b)
    catch { case _: ArithmeticException => if ((a ^ b) < 0L) Long.MinValue else Long.MaxValue }

  private def satAdd(a: Long, b: Long): Long =
    try Math.addExact(a, b)
    catch { case _: ArithmeticException => if (a < 0L) Long.MinValue else Long.MaxValue }

  def relative(duration: FiniteDuration): Vector[Bytes] =
    if (wholeSeconds(duration)) Vector(Ex, Args.long(duration.toSeconds))
    else Vector(Px, Args.long(millis(duration)))

  def absolute(timestamp: Instant): Vector[Bytes] =
    if (wholeSeconds(timestamp)) Vector(ExAt, Args.long(timestamp.getEpochSecond))
    else Vector(PxAt, Args.long(millis(timestamp)))

  def expireCommand(secName: String, msName: String, duration: FiniteDuration): (String, Long) =
    if (wholeSeconds(duration)) (secName, duration.toSeconds) else (msName, millis(duration))

  def expireCommand(secName: String, msName: String, timestamp: Instant): (String, Long) =
    if (wholeSeconds(timestamp)) (secName, timestamp.getEpochSecond) else (msName, millis(timestamp))

  private val Ex   = Bytes.utf8("EX")
  private val Px   = Bytes.utf8("PX")
  private val ExAt = Bytes.utf8("EXAT")
  private val PxAt = Bytes.utf8("PXAT")
}

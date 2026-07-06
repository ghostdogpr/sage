package sage.commands

import java.time.Instant

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{Doubles, KeyCodec, ValueCodec}
import sage.protocol.Frame

private[commands] object Decode {

  val frame: Frame => Either[DecodeError, Frame] = Right(_)

  val long: Frame => Either[DecodeError, Long] = {
    case Frame.Integer(value) => Right(value)
    case other                => Left(DecodeError("integer", Frame.describe(other)))
  }

  val flag: Frame => Either[DecodeError, Boolean] = {
    case Frame.Integer(0) => Right(false)
    case Frame.Integer(1) => Right(true)
    case other            => Left(DecodeError("integer 0 or 1", Frame.describe(other)))
  }

  val ok: Frame => Either[DecodeError, Unit] = {
    case Frame.SimpleString("OK") => Right(())
    case other                    => Left(DecodeError("simple string 'OK'", Frame.describe(other)))
  }

  val double: Frame => Either[DecodeError, Double] = {
    case Frame.BulkString(bytes) =>
      val text = bytes.asUtf8String
      Doubles.parse(text).toRight(DecodeError("double bulk string", s"bulk string '$text'"))
    case other                   => Left(DecodeError("double bulk string", Frame.describe(other)))
  }

  // shared TTL/expiry shape (TTL, EXPIRETIME, HTTL, …): `-2` absent, `-1` no expiry, else present
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

  val utf8String: Frame => Either[DecodeError, String] = {
    case Frame.BulkString(bytes) => Right(bytes.asUtf8String)
    case other                   => Left(DecodeError("bulk string", Frame.describe(other)))
  }

  // text however the server framed it: simple, bulk, or the RESP3 verbatim form INFO/CLIENT INFO/CLUSTER NODES use
  val text: Frame => Either[DecodeError, String] = {
    case Frame.SimpleString(value)      => Right(value)
    case Frame.BulkString(bytes)        => Right(bytes.asUtf8String)
    case Frame.VerbatimString(_, bytes) => Right(bytes.asUtf8String)
    case other                          => Left(DecodeError("string", Frame.describe(other)))
  }

  val optionalUtf8String: Frame => Either[DecodeError, Option[String]] = {
    case Frame.Null              => Right(None)
    case Frame.BulkString(bytes) => Right(Some(bytes.asUtf8String))
    case other                   => Left(DecodeError("bulk string or null", Frame.describe(other)))
  }

  val bytes: Frame => Either[DecodeError, Bytes] = {
    case Frame.BulkString(value) => Right(value)
    case other                   => Left(DecodeError("bulk string", Frame.describe(other)))
  }

  val optionalBytes: Frame => Either[DecodeError, Option[Bytes]] = {
    case Frame.Null              => Right(None)
    case Frame.BulkString(value) => Right(Some(value))
    case other                   => Left(DecodeError("bulk string or null", Frame.describe(other)))
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

  val optionalLong: Frame => Either[DecodeError, Option[Long]] = {
    case Frame.Null           => Right(None)
    case Frame.Integer(value) => Right(Some(value))
    case other                => Left(DecodeError("integer or null", Frame.describe(other)))
  }

  // a double however the server framed it: a RESP3 Double, or a bulk string under RESP2 (geo coordinates, distances)
  val lenientDouble: Frame => Either[DecodeError, Double] = {
    case Frame.Double(value)     => Right(value)
    case Frame.BulkString(bytes) =>
      val text = bytes.asUtf8String
      Doubles.parse(text).toRight(DecodeError("double", s"bulk string '$text'"))
    case other                   => Left(DecodeError("double", Frame.describe(other)))
  }

  // GEODIST replies the distance as a double, or null when a member is absent
  val optionalDouble: Frame => Either[DecodeError, Option[Double]] = {
    case Frame.Null => Right(None)
    case other      => lenientDouble(other).map(Some(_))
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

  def each[A, B](items: IterableOnce[A])(f: A => Either[DecodeError, B]): Either[DecodeError, Vector[B]] =
    buildEach(items, Vector.newBuilder[B])(f)

  // steps a flat alternating array two elements at a time, without grouped(2)'s throwaway 2-element Vector per pair; caller guarantees even length
  private def buildPairs[B, C](elements: Vector[Frame], builder: mutable.Builder[B, C])(
    f: (Frame, Frame) => Either[DecodeError, B]
  ): Either[DecodeError, C] = {
    builder.sizeHint(elements.length / 2)
    var i = 0
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

  def vector[A](element: Frame => Either[DecodeError, A]): Frame => Either[DecodeError, Vector[A]] = {
    case Frame.Array(elements) => buildEach(elements, Vector.newBuilder[A])(element)
    case other                 => Left(DecodeError("array", Frame.describe(other)))
  }

  // a missing list replies null where a present one replies an array; a stored list is never empty, so null collapses to an empty vector
  def vectorOrEmpty[A](element: Frame => Either[DecodeError, A]): Frame => Either[DecodeError, Vector[A]] = {
    val decodeVector = vector(element)
    frame =>
      frame match {
        case Frame.Null => Right(Vector.empty)
        case other      => decodeVector(other)
      }
  }

  // a RESP3 map, or the flat RESP2 array of alternating key/value some introspection replies still use; non-string keys are dropped
  val fieldMap: Frame => Either[DecodeError, Map[String, Frame]] = {
    case Frame.Map(entries) => Right(entries.collect { case (Frame.BulkString(k), v) => k.asUtf8String -> v }.toMap)
    case Frame.Array(elements) if elements.length % 2 == 0 =>
      val builder = Map.newBuilder[String, Frame]
      builder.sizeHint(elements.length / 2)
      var i       = 0
      while (i < elements.length) {
        elements(i) match {
          case Frame.BulkString(k) => builder += (k.asUtf8String -> elements(i + 1))
          case _                   => ()
        }
        i += 2
      }
      Right(builder.result())
    case other => Left(DecodeError("map", Frame.describe(other)))
  }

  def map[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Map[K, V]] = {
    case Frame.Map(entries) =>
      buildEach(entries, Map.newBuilder[K, V]) { case (fieldFrame, valueFrame) =>
        for {
          field <- key(fieldFrame)
          value <- this.value(valueFrame)
        } yield field -> value
      }
    case other              => Left(DecodeError("map", Frame.describe(other)))
  }

  // HSCAN's items are a flat field, value, field, value, … array; HRANDFIELD WITHVALUES nests each pair in its own array.
  def flatPairs[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Vector[(K, V)]] = {
    case Frame.Array(elements) if elements.length % 2 == 0 =>
      buildPairs(elements, Vector.newBuilder[(K, V)]) { (fieldFrame, valueFrame) =>
        for {
          field <- key(fieldFrame)
          value <- this.value(valueFrame)
        } yield field -> value
      }
    case other => Left(DecodeError("array of field/value pairs", Frame.describe(other)))
  }

  def nestedPairs[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Vector[(K, V)]] = {
    val pair = array2(key[K], value[V], "field/value pair")(_ -> _)
    frame =>
      frame match {
        case Frame.Array(rows) => each(rows)(pair)
        case other             => Left(DecodeError("array of field/value pairs", Frame.describe(other)))
      }
  }

  private val scanCursor: Frame => Either[DecodeError, Option[ScanCursor]] = {
    case Frame.BulkString(bytes) =>
      Right(if (bytes.sameBytes(ScanCursor.bytes(ScanCursor.start))) None else Some(ScanCursor.wrap(bytes)))
    case other                   => Left(DecodeError("cursor bulk string", Frame.describe(other)))
  }

  def scanPage[A](items: Frame => Either[DecodeError, Vector[A]]): Frame => Either[DecodeError, ScanPage[A]] =
    array2(scanCursor, items, "array of cursor and items")((next, decoded) => ScanPage(decoded, next))

  // RESP3 returns set-typed replies (SMEMBERS, SINTER, …) as a Set frame, never an Array
  def set[V](using ValueCodec[V]): Frame => Either[DecodeError, Set[V]] = {
    case Frame.Set(elements) => buildEach(elements, Set.newBuilder[V])(value)
    case other               => Left(DecodeError("set", Frame.describe(other)))
  }

  val score: Frame => Either[DecodeError, Double] = {
    case Frame.Double(value) => Right(value)
    case other               => Left(DecodeError("double", Frame.describe(other)))
  }

  val optionalScore: Frame => Either[DecodeError, Option[Double]] = {
    case Frame.Null          => Right(None)
    case Frame.Double(value) => Right(Some(value))
    case other               => Left(DecodeError("double or null", Frame.describe(other)))
  }

  private def memberScore[V](using ValueCodec[V]): Frame => Either[DecodeError, (V, Double)] =
    array2(value[V], score, "member/score pair")(_ -> _)

  // RESP3 nests each member with its Double score in a two-element array (ZRANGE WITHSCORES, ZPOPMIN count, …)
  def scoredMembers[V](using ValueCodec[V]): Frame => Either[DecodeError, Vector[(V, Double)]] = {
    case Frame.Array(rows) => each(rows)(memberScore[V])
    case other             => Left(DecodeError("array of member/score pairs", Frame.describe(other)))
  }

  // ZPOPMIN/ZPOPMAX without a count: a flat [member, score], or an empty array when the key is absent
  def optionalScoredMember[V](using ValueCodec[V]): Frame => Either[DecodeError, Option[(V, Double)]] = {
    case Frame.Null                      => Right(None)
    case Frame.Array(Vector())           => Right(None)
    case arr @ Frame.Array(Vector(_, _)) => memberScore[V](arr).map(Some(_))
    case other                           => Left(DecodeError("member/score pair or empty array", Frame.describe(other)))
  }

  // ZSCAN's items are a flat member, score, member, score array with scores as bulk strings, not RESP3 doubles
  def scoredMembersFlat[V](using ValueCodec[V]): Frame => Either[DecodeError, Vector[(V, Double)]] = {
    case Frame.Array(elements) if elements.length % 2 == 0 =>
      buildPairs(elements, Vector.newBuilder[(V, Double)]) { (memberFrame, scoreFrame) =>
        for {
          member <- value(memberFrame)
          s      <- double(scoreFrame)
        } yield member -> s
      }
    case other => Left(DecodeError("array of member/score pairs", Frame.describe(other)))
  }
}

private[commands] object TimeArgs {

  def wholeSeconds(duration: FiniteDuration): Boolean = duration.toNanos % 1000000000L == 0

  def wholeSeconds(timestamp: Instant): Boolean = timestamp.getNano == 0

  // whole seconds keep the second-precision wire form; anything finer rounds up to the next millisecond —
  // an expiry must never land earlier than asked, and truncation would turn a sub-millisecond expiry into 0 (immediate)
  def millis(duration: FiniteDuration): Long = Math.ceilDiv(duration.toNanos, 1000000L)

  // saturate rather than overflow: an extreme Instant must not throw while building a command, and clamping up never lands earlier than asked
  def millis(timestamp: Instant): Long =
    satAdd(satMul(timestamp.getEpochSecond, 1000L), Math.ceilDiv(timestamp.getNano.toLong, 1000000L))

  private def satMul(a: Long, b: Long): Long =
    try Math.multiplyExact(a, b)
    catch { case _: ArithmeticException => if ((a ^ b) < 0L) Long.MinValue else Long.MaxValue }

  private def satAdd(a: Long, b: Long): Long =
    try Math.addExact(a, b)
    catch { case _: ArithmeticException => if (a < 0L) Long.MinValue else Long.MaxValue }

  def relative(duration: FiniteDuration): Vector[Bytes] =
    if (wholeSeconds(duration)) Vector(Ex, Bytes.utf8(duration.toSeconds.toString))
    else Vector(Px, Bytes.utf8(millis(duration).toString))

  def absolute(timestamp: Instant): Vector[Bytes] =
    if (wholeSeconds(timestamp)) Vector(ExAt, Bytes.utf8(timestamp.getEpochSecond.toString))
    else Vector(PxAt, Bytes.utf8(millis(timestamp).toString))

  def expireCommand(secName: String, msName: String, duration: FiniteDuration): (String, Long) =
    if (wholeSeconds(duration)) (secName, duration.toSeconds) else (msName, millis(duration))

  def expireCommand(secName: String, msName: String, timestamp: Instant): (String, Long) =
    if (wholeSeconds(timestamp)) (secName, timestamp.getEpochSecond) else (msName, millis(timestamp))

  private val Ex   = Bytes.utf8("EX")
  private val Px   = Bytes.utf8("PX")
  private val ExAt = Bytes.utf8("EXAT")
  private val PxAt = Bytes.utf8("PXAT")
}

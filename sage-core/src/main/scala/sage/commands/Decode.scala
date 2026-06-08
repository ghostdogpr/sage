package sage.commands

import java.time.Instant

import scala.concurrent.duration.FiniteDuration

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

private[commands] object Decode {

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
      text.toDoubleOption.toRight(DecodeError("double bulk string", s"bulk string '$text'"))
    case other                   => Left(DecodeError("double bulk string", Frame.describe(other)))
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

  def vector[A](element: Frame => Either[DecodeError, A]): Frame => Either[DecodeError, Vector[A]] = {
    case Frame.Array(elements) =>
      elements.foldLeft[Either[DecodeError, Vector[A]]](Right(Vector.empty)) { (acc, frame) =>
        acc.flatMap(decoded => element(frame).map(decoded :+ _))
      }
    case other                 => Left(DecodeError("array", Frame.describe(other)))
  }

  // a missing list replies null where a present one replies an array; a stored list is never empty, so null collapses to an empty vector
  def vectorOrEmpty[A](element: Frame => Either[DecodeError, A]): Frame => Either[DecodeError, Vector[A]] = {
    case Frame.Null => Right(Vector.empty)
    case other      => vector(element)(other)
  }

  def map[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Map[K, V]] = {
    case Frame.Map(entries) =>
      entries.foldLeft[Either[DecodeError, Map[K, V]]](Right(Map.empty)) { case (acc, (fieldFrame, valueFrame)) =>
        for {
          decoded <- acc
          field   <- key(fieldFrame)
          value   <- this.value(valueFrame)
        } yield decoded + (field -> value)
      }
    case other              => Left(DecodeError("map", Frame.describe(other)))
  }

  // HSCAN's items are a flat field, value, field, value, … array; HRANDFIELD WITHVALUES nests each pair in its own array.
  def flatPairs[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Vector[(K, V)]] = {
    case Frame.Array(elements) if elements.length % 2 == 0 =>
      elements.grouped(2).foldLeft[Either[DecodeError, Vector[(K, V)]]](Right(Vector.empty)) { (acc, pair) =>
        for {
          decoded <- acc
          field   <- key(pair(0))
          value   <- this.value(pair(1))
        } yield decoded :+ (field -> value)
      }
    case other => Left(DecodeError("array of field/value pairs", Frame.describe(other)))
  }

  def nestedPairs[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Vector[(K, V)]] = {
    case Frame.Array(rows) =>
      rows.foldLeft[Either[DecodeError, Vector[(K, V)]]](Right(Vector.empty)) { (acc, row) =>
        acc.flatMap { decoded =>
          row match {
            case Frame.Array(Vector(fieldFrame, valueFrame)) =>
              for {
                field <- key(fieldFrame)
                value <- this.value(valueFrame)
              } yield decoded :+ (field -> value)
            case other                                       => Left(DecodeError("field/value pair", Frame.describe(other)))
          }
        }
      }
    case other             => Left(DecodeError("array of field/value pairs", Frame.describe(other)))
  }

  def scanPage[A](items: Frame => Either[DecodeError, Vector[A]]): Frame => Either[DecodeError, ScanPage[A]] = {
    case Frame.Array(Vector(cursorFrame, itemsFrame)) =>
      for {
        next    <- cursorFrame match {
                     case Frame.BulkString(bytes) =>
                       Right(if (bytes.sameBytes(ScanCursor.bytes(ScanCursor.start))) None else Some(ScanCursor.wrap(bytes)))
                     case other                   => Left(DecodeError("cursor bulk string", Frame.describe(other)))
                   }
        decoded <- items(itemsFrame)
      } yield ScanPage(decoded, next)
    case other                                        => Left(DecodeError("array of cursor and items", Frame.describe(other)))
  }

  // RESP3 returns set-typed replies (SMEMBERS, SINTER, …) as a Set frame, never an Array
  def set[V](using ValueCodec[V]): Frame => Either[DecodeError, Set[V]] = {
    case Frame.Set(elements) =>
      elements.foldLeft[Either[DecodeError, Set[V]]](Right(Set.empty)) { (acc, frame) =>
        acc.flatMap(decoded => value(frame).map(decoded + _))
      }
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

  // RESP3 nests each member with its Double score in a two-element array (ZRANGE WITHSCORES, ZPOPMIN count, …)
  def scoredMembers[V](using ValueCodec[V]): Frame => Either[DecodeError, Vector[(V, Double)]] = {
    case Frame.Array(rows) =>
      rows.foldLeft[Either[DecodeError, Vector[(V, Double)]]](Right(Vector.empty)) { (acc, row) =>
        acc.flatMap { decoded =>
          row match {
            case Frame.Array(Vector(memberFrame, scoreFrame)) =>
              for {
                member <- value(memberFrame)
                s      <- score(scoreFrame)
              } yield decoded :+ (member -> s)
            case other                                        => Left(DecodeError("member/score pair", Frame.describe(other)))
          }
        }
      }
    case other             => Left(DecodeError("array of member/score pairs", Frame.describe(other)))
  }

  // ZPOPMIN/ZPOPMAX without a count: a flat [member, score], or an empty array when the key is absent
  def optionalScoredMember[V](using ValueCodec[V]): Frame => Either[DecodeError, Option[(V, Double)]] = {
    case Frame.Null                                   => Right(None)
    case Frame.Array(Vector())                        => Right(None)
    case Frame.Array(Vector(memberFrame, scoreFrame)) =>
      for {
        member <- value(memberFrame)
        s      <- score(scoreFrame)
      } yield Some(member -> s)
    case other                                        => Left(DecodeError("member/score pair or empty array", Frame.describe(other)))
  }

  // ZSCAN's items are a flat member, score, member, score array with scores as bulk strings, not RESP3 doubles
  def scoredMembersFlat[V](using ValueCodec[V]): Frame => Either[DecodeError, Vector[(V, Double)]] = {
    case Frame.Array(elements) if elements.length % 2 == 0 =>
      elements.grouped(2).foldLeft[Either[DecodeError, Vector[(V, Double)]]](Right(Vector.empty)) { (acc, pair) =>
        for {
          decoded <- acc
          member  <- value(pair(0))
          s       <- scoreText(pair(1))
        } yield decoded :+ (member -> s)
      }
    case other => Left(DecodeError("array of member/score pairs", Frame.describe(other)))
  }

  private def scoreText(frame: Frame): Either[DecodeError, Double] =
    frame match {
      case Frame.BulkString(bytes) => parseScore(bytes.asUtf8String)
      case other                   => Left(DecodeError("score bulk string", Frame.describe(other)))
    }

  private def parseScore(text: String): Either[DecodeError, Double] =
    text match {
      case "inf" | "+inf" => Right(Double.PositiveInfinity)
      case "-inf"         => Right(Double.NegativeInfinity)
      case "nan"          => Right(Double.NaN)
      case other          => other.toDoubleOption.toRight(DecodeError("double", s"bulk string '$other'"))
    }
}

private[commands] object TimeArgs {

  def wholeSeconds(duration: FiniteDuration): Boolean = duration.toNanos % 1000000000L == 0

  def wholeSeconds(timestamp: Instant): Boolean = timestamp.getNano == 0

  // whole seconds keep the second-precision wire form; anything finer rounds up to the next millisecond —
  // an expiry must never land earlier than asked, and truncation would turn a sub-millisecond expiry into 0 (immediate)
  def millis(duration: FiniteDuration): Long = Math.ceilDiv(duration.toNanos, 1000000L)

  def millis(timestamp: Instant): Long =
    Math.addExact(Math.multiplyExact(timestamp.getEpochSecond, 1000L), Math.ceilDiv(timestamp.getNano.toLong, 1000000L))

  def relative(duration: FiniteDuration): Vector[Bytes] =
    if (wholeSeconds(duration)) Vector(Ex, Bytes.utf8(duration.toSeconds.toString))
    else Vector(Px, Bytes.utf8(millis(duration).toString))

  def absolute(timestamp: Instant): Vector[Bytes] =
    if (wholeSeconds(timestamp)) Vector(ExAt, Bytes.utf8(timestamp.getEpochSecond.toString))
    else Vector(PxAt, Bytes.utf8(millis(timestamp).toString))

  private val Ex   = Bytes.utf8("EX")
  private val Px   = Bytes.utf8("PX")
  private val ExAt = Bytes.utf8("EXAT")
  private val PxAt = Bytes.utf8("PXAT")
}

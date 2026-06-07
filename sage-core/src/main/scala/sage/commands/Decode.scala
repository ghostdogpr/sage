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

  def vector[A](element: Frame => Either[DecodeError, A]): Frame => Either[DecodeError, Vector[A]] = {
    case Frame.Array(elements) =>
      elements.foldLeft[Either[DecodeError, Vector[A]]](Right(Vector.empty)) { (acc, frame) =>
        acc.flatMap(decoded => element(frame).map(decoded :+ _))
      }
    case other                 => Left(DecodeError("array", Frame.describe(other)))
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

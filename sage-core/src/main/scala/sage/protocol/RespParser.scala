package sage.protocol

import sage.Bytes
import sage.SageException.ProtocolError

/**
  * Incremental RESP3 parser: feed it bytes as they arrive, get back every frame completed so far.
  *
  * One instance per connection; not thread-safe. Tolerates input split at arbitrary byte boundaries across feeds — incomplete trailing
  * input is retained and resumed on the next feed. After a `ProtocolError` the parser is poisoned (RESP3 has no resynchronization point)
  * and the connection must be discarded (ADR-0006).
  *
  * RESP2-compat null forms (`$-1`, `*-1`) are tolerated and parsed as [[Frame.Null]]. Streamed types (`?` lengths, `;` chunks) are not
  * supported: no server sends them.
  */
final class RespParser {

  import RespParser.*

  private var buffer: Array[Byte]            = Array.emptyByteArray
  private var failure: Option[ProtocolError] = None

  /**
    * Appends `bytes` to the unconsumed input and returns every frame completed by them, in order.
    */
  def feed(bytes: Bytes): Either[ProtocolError, Vector[Frame]] =
    failure match {
      case Some(error) => Left(error)
      case None        =>
        append(bytes)
        var frames                       = Vector.empty[Frame]
        var pos                          = 0
        var done                         = false
        var error: Option[ProtocolError] = None
        while (!done)
          parseFrame(pos) match {
            case Parsed(frame, next) =>
              frames :+= frame
              pos = next
            case Incomplete          =>
              done = true
            case Failed(message)     =>
              error = Some(ProtocolError(message))
              done = true
          }
        error match {
          case Some(protocolError) =>
            failure = Some(protocolError)
            buffer = Array.emptyByteArray
            Left(protocolError)
          case None                =>
            if (pos > 0) buffer = buffer.drop(pos)
            Right(frames)
        }
    }

  private def append(bytes: Bytes): Unit =
    buffer =
      if (buffer.isEmpty) bytes.toArray
      else {
        val incoming = bytes.toArray
        val merged   = java.util.Arrays.copyOf(buffer, buffer.length + incoming.length)
        System.arraycopy(incoming, 0, merged, buffer.length, incoming.length)
        merged
      }

  private def parseFrame(pos: Int): Result =
    if (pos >= buffer.length) Incomplete
    else {
      buffer(pos).toChar match {
        case '+'   => withLine(pos + 1)((line, next) => Parsed(Frame.SimpleString(line), next))
        case '-'   => withLine(pos + 1)((line, next) => Parsed(Frame.SimpleError(line), next))
        case ':'   =>
          withLine(pos + 1) { (line, next) =>
            line.toLongOption match {
              case Some(value) => Parsed(Frame.Integer(value), next)
              case None        => Failed(s"invalid integer: '$line'")
            }
          }
        case ','   =>
          withLine(pos + 1) { (line, next) =>
            parseDouble(line) match {
              case Some(value) => Parsed(Frame.Double(value), next)
              case None        => Failed(s"invalid double: '$line'")
            }
          }
        case '#'   =>
          withLine(pos + 1) { (line, next) =>
            line match {
              case "t" => Parsed(Frame.Bool(true), next)
              case "f" => Parsed(Frame.Bool(false), next)
              case _   => Failed(s"invalid boolean: '$line'")
            }
          }
        case '('   =>
          withLine(pos + 1) { (line, next) =>
            try Parsed(Frame.BigNumber(BigInt(line)), next)
            catch { case _: NumberFormatException => Failed(s"invalid big number: '$line'") }
          }
        case '_'   =>
          withLine(pos + 1) { (line, next) =>
            if (line.isEmpty) Parsed(Frame.Null, next) else Failed(s"unexpected content in null frame: '$line'")
          }
        case '$'   =>
          withLine(pos + 1) { (line, start) =>
            line.toIntOption match {
              case Some(-1)                    => Parsed(Frame.Null, start)
              case Some(length) if length >= 0 => withPayload(start, length)(Frame.BulkString.apply)
              case _                           => Failed(s"invalid bulk string length: '$line'")
            }
          }
        case '!'   =>
          withLine(pos + 1) { (line, start) =>
            line.toIntOption match {
              case Some(length) if length >= 0 => withPayload(start, length)(Frame.BulkError.apply)
              case _                           => Failed(s"invalid bulk error length: '$line'")
            }
          }
        case '='   =>
          withLine(pos + 1) { (line, start) =>
            line.toIntOption match {
              case Some(length) if length >= 4 =>
                if (buffer.length < start + length + 2) Incomplete
                else if (buffer(start + length) != '\r' || buffer(start + length + 1) != '\n') Failed("missing CRLF after bulk payload")
                else if (buffer(start + 3) != ':') Failed("verbatim string missing ':' separator")
                else Parsed(Frame.VerbatimString(stringAt(start, start + 3), bytesAt(start + 4, start + length)), start + length + 2)
              case _                           => Failed(s"invalid verbatim string length: '$line'")
            }
          }
        case '*'   =>
          withCount(pos + 1, allowNull = true) { (count, start) =>
            if (count == -1) Parsed(Frame.Null, start)
            else withElements(start, count)((elements, next) => Parsed(Frame.Array(elements), next))
          }
        case '~'   =>
          withCount(pos + 1) { (count, start) =>
            withElements(start, count)((elements, next) => Parsed(Frame.Set(elements), next))
          }
        case '>'   =>
          withCount(pos + 1) { (count, start) =>
            withElements(start, count)((elements, next) => Parsed(Frame.Push(elements), next))
          }
        case '%'   =>
          withCount(pos + 1) { (count, start) =>
            withElements(start, count * 2)((elements, next) => Parsed(Frame.Map(paired(elements)), next))
          }
        case '|'   =>
          withCount(pos + 1) { (count, start) =>
            withElements(start, count * 2)((elements, next) => Parsed(Frame.Attribute(paired(elements)), next))
          }
        case other =>
          Failed(f"unknown frame type byte 0x${other.toByte}%02x")
      }
    }

  /**
    * Index of the next CRLF's '\r' at or after `from`, or -1 if the input ends first.
    */
  private def findCrlf(from: Int): Int = {
    var i = from
    while (i + 1 < buffer.length && (buffer(i) != '\r' || buffer(i + 1) != '\n'))
      i += 1
    if (i + 1 < buffer.length) i else -1
  }

  private def withLine(pos: Int)(use: (String, Int) => Result): Result = {
    val cr = findCrlf(pos)
    if (cr < 0) Incomplete else use(stringAt(pos, cr), cr + 2)
  }

  private def withCount(pos: Int, allowNull: Boolean = false)(use: (Int, Int) => Result): Result =
    withLine(pos) { (line, next) =>
      line.toIntOption match {
        case Some(count) if count >= 0 => use(count, next)
        case Some(-1) if allowNull     => use(-1, next)
        case _                         => Failed(s"invalid aggregate length: '$line'")
      }
    }

  private def withPayload(start: Int, length: Int)(make: Bytes => Frame): Result =
    if (buffer.length < start + length + 2) Incomplete
    else if (buffer(start + length) != '\r' || buffer(start + length + 1) != '\n') Failed("missing CRLF after bulk payload")
    else Parsed(make(bytesAt(start, start + length)), start + length + 2)

  private def withElements(start: Int, count: Int)(use: (Vector[Frame], Int) => Result): Result = {
    var elements = Vector.empty[Frame]
    var pos      = start
    var i        = 0
    while (i < count)
      parseFrame(pos) match {
        case Parsed(frame, next) =>
          elements :+= frame
          pos = next
          i += 1
        case Incomplete          => return Incomplete
        case failed: Failed      => return failed
      }
    use(elements, pos)
  }

  private def paired(elements: Vector[Frame]): Vector[(Frame, Frame)] =
    elements.grouped(2).collect { case Seq(key, value) => (key, value) }.toVector

  private def stringAt(from: Int, until: Int): String =
    new String(buffer, from, until - from, java.nio.charset.StandardCharsets.UTF_8)

  private def bytesAt(from: Int, until: Int): Bytes =
    Bytes.wrap(IArray.unsafeFromArray(java.util.Arrays.copyOfRange(buffer, from, until)))

  private def parseDouble(text: String): Option[scala.Double] =
    text match {
      case "inf" | "+inf" => Some(scala.Double.PositiveInfinity)
      case "-inf"         => Some(scala.Double.NegativeInfinity)
      case "nan"          => Some(scala.Double.NaN)
      case other          => other.toDoubleOption
    }
}

object RespParser {

  sealed private trait Result
  final private case class Parsed(frame: Frame, next: Int) extends Result
  private case object Incomplete                           extends Result
  final private case class Failed(message: String)         extends Result
}

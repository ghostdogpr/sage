package sage.protocol

import scala.annotation.switch

import sage.Bytes
import sage.SageException.ProtocolError

/**
  * Incremental RESP3 parser: feed it bytes as they arrive, get back every frame completed so far.
  *
  * One instance per connection; not thread-safe. Tolerates input split at arbitrary byte boundaries across feeds — incomplete trailing
  * input is retained and resumed on the next feed. After a `ProtocolError` the parser is poisoned (RESP3 has no resynchronization point)
  * and the connection must be discarded (ADR-0006).
  *
  * This sits on every reply's hot path, so it avoids allocation beyond the frames themselves: a single compacting buffer (no copy per
  * feed), integers and length headers parsed directly from bytes, and parse outcomes signalled through fields (`cursor`, `failMessage`)
  * rather than wrapper objects.
  *
  * RESP2-compat null forms (`$-1`, `*-1`) are tolerated and parsed as [[Frame.Null]]. Streamed types (`?` lengths, `;` chunks) are not
  * supported: no server sends them.
  */
final class RespParser {

  private var buf: Array[Byte]       = Array.emptyByteArray
  private var readPos: Int           = 0 // start of unconsumed input
  private var writePos: Int          = 0 // end of valid input
  private var failure: ProtocolError = null

  // out-fields for parseFrame, avoiding a result-wrapper allocation per frame:
  // on success, `cursor` is the position right after the parsed frame; on failure, `failMessage` is set and null is returned;
  // a null return with no failMessage means the input is incomplete.
  private var cursor: Int         = 0
  private var failMessage: String = null

  // out-field for readLong, avoiding an Option allocation per number
  private var numberOk: Boolean = false

  /**
    * Appends `bytes` to the unconsumed input and returns every frame completed by them, in order.
    */
  def feed(bytes: Bytes): Either[ProtocolError, Vector[Frame]] =
    if (failure != null) Left(failure)
    else {
      append(bytes)
      var frames = Vector.empty[Frame]
      var done   = false
      while (!done) {
        val frame = parseFrame(readPos)
        if (frame == null) done = true
        else {
          frames :+= frame
          readPos = cursor
        }
      }
      if (failMessage != null) {
        val error = ProtocolError(failMessage)
        failure = error
        buf = Array.emptyByteArray
        readPos = 0
        writePos = 0
        Left(error)
      } else {
        if (readPos == writePos) { // everything consumed: reset the window so the buffer never needs compacting
          readPos = 0
          writePos = 0
        }
        Right(frames)
      }
    }

  /**
    * Appends into the single reusable buffer: compacts in place when the front has been consumed, grows geometrically when full.
    */
  private def append(bytes: Bytes): Unit = {
    val incoming = bytes.unsafeArray // read-only view, never mutated
    val unparsed = writePos - readPos
    if (buf.length - writePos < incoming.length) {
      val needed = unparsed + incoming.length
      if (buf.length >= needed) {
        System.arraycopy(buf, readPos, buf, 0, unparsed)
      } else {
        var capacity = math.max(buf.length * 2, 256)
        while (capacity < needed) capacity *= 2
        val grown    = new Array[Byte](capacity)
        System.arraycopy(buf, readPos, grown, 0, unparsed)
        buf = grown
      }
      readPos = 0
      writePos = unparsed
    }
    System.arraycopy(incoming, 0, buf, writePos, incoming.length)
    writePos += incoming.length
  }

  /**
    * Parses one frame at `pos`. Returns null when the input is incomplete (no `failMessage`) or invalid (`failMessage` set).
    */
  private def parseFrame(pos: Int): Frame =
    if (pos >= writePos) null
    else {
      (buf(pos).toChar: @switch) match {
        case '+'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) null
          else {
            cursor = cr + 2
            Frame.SimpleString(stringAt(pos + 1, cr))
          }
        case '-'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) null
          else {
            cursor = cr + 2
            Frame.SimpleError(stringAt(pos + 1, cr))
          }
        case ':'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) null
          else {
            val value = readLong(pos + 1, cr)
            if (!numberOk) fail(s"invalid integer: '${stringAt(pos + 1, cr)}'")
            else {
              cursor = cr + 2
              Frame.Integer(value)
            }
          }
        case ','   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) null
          else {
            val text = stringAt(pos + 1, cr)
            try {
              val value = text match {
                case "inf" | "+inf" => java.lang.Double.POSITIVE_INFINITY
                case "-inf"         => java.lang.Double.NEGATIVE_INFINITY
                case "nan"          => java.lang.Double.NaN
                case other          => java.lang.Double.parseDouble(other)
              }
              cursor = cr + 2
              Frame.Double(value)
            } catch { case _: NumberFormatException => fail(s"invalid double: '$text'") }
          }
        case '#'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) null
          else if (cr != pos + 2) fail(s"invalid boolean: '${stringAt(pos + 1, cr)}'")
          else {
            buf(pos + 1).toChar match {
              case 't' =>
                cursor = cr + 2
                Frame.Bool(true)
              case 'f' =>
                cursor = cr + 2
                Frame.Bool(false)
              case _   => fail(s"invalid boolean: '${stringAt(pos + 1, cr)}'")
            }
          }
        case '('   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) null
          else {
            val text = stringAt(pos + 1, cr)
            try {
              val value = BigInt(text)
              cursor = cr + 2
              Frame.BigNumber(value)
            } catch { case _: NumberFormatException => fail(s"invalid big number: '$text'") }
          }
        case '_'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) null
          else if (cr != pos + 1) fail(s"unexpected content in null frame: '${stringAt(pos + 1, cr)}'")
          else {
            cursor = cr + 2
            Frame.Null
          }
        case '$'   =>
          val length = readLength(pos + 1, allowNull = true)
          if (length == Incomplete) null
          else if (length == Invalid) fail(s"invalid bulk string length: '${headerText(pos + 1)}'")
          else if (length == -1) {
            // cursor was set by readLength
            Frame.Null
          } else {
            val start = cursor
            if (writePos < start + length + 2) null
            else if (buf(start + length) != '\r' || buf(start + length + 1) != '\n') fail("missing CRLF after bulk payload")
            else {
              cursor = start + length + 2
              Frame.BulkString(bytesAt(start, start + length))
            }
          }
        case '!'   =>
          val length = readLength(pos + 1, allowNull = false)
          if (length == Incomplete) null
          else if (length == Invalid) fail(s"invalid bulk error length: '${headerText(pos + 1)}'")
          else {
            val start = cursor
            if (writePos < start + length + 2) null
            else if (buf(start + length) != '\r' || buf(start + length + 1) != '\n') fail("missing CRLF after bulk payload")
            else {
              cursor = start + length + 2
              Frame.BulkError(bytesAt(start, start + length))
            }
          }
        case '='   =>
          val length = readLength(pos + 1, allowNull = false)
          if (length == Incomplete) null
          else if (length == Invalid || length < 4) fail(s"invalid verbatim string length: '${headerText(pos + 1)}'")
          else {
            val start = cursor
            if (writePos < start + length + 2) null
            else if (buf(start + length) != '\r' || buf(start + length + 1) != '\n') fail("missing CRLF after bulk payload")
            else if (buf(start + 3) != ':') fail("verbatim string missing ':' separator")
            else {
              cursor = start + length + 2
              Frame.VerbatimString(stringAt(start, start + 3), bytesAt(start + 4, start + length))
            }
          }
        case '*'   =>
          val count = readLength(pos + 1, allowNull = true)
          if (count == Incomplete) null
          else if (count == Invalid) fail(s"invalid aggregate length: '${headerText(pos + 1)}'")
          else if (count == -1) Frame.Null
          else {
            val elements = parseElements(cursor, count)
            if (elements == null) null else Frame.Array(elements)
          }
        case '~'   =>
          val count = readLength(pos + 1, allowNull = false)
          if (count == Incomplete) null
          else if (count == Invalid) fail(s"invalid aggregate length: '${headerText(pos + 1)}'")
          else {
            val elements = parseElements(cursor, count)
            if (elements == null) null else Frame.Set(elements)
          }
        case '>'   =>
          val count = readLength(pos + 1, allowNull = false)
          if (count == Incomplete) null
          else if (count == Invalid) fail(s"invalid aggregate length: '${headerText(pos + 1)}'")
          else {
            val elements = parseElements(cursor, count)
            if (elements == null) null else Frame.Push(elements)
          }
        case '%'   =>
          val count = readLength(pos + 1, allowNull = false)
          if (count == Incomplete) null
          else if (count == Invalid) fail(s"invalid aggregate length: '${headerText(pos + 1)}'")
          else {
            val entries = parsePairs(cursor, count)
            if (entries == null) null else Frame.Map(entries)
          }
        case '|'   =>
          val count = readLength(pos + 1, allowNull = false)
          if (count == Incomplete) null
          else if (count == Invalid) fail(s"invalid aggregate length: '${headerText(pos + 1)}'")
          else {
            val entries = parsePairs(cursor, count)
            if (entries == null) null else Frame.Attribute(entries)
          }
        case other =>
          fail(f"unknown frame type byte 0x${other.toByte}%02x")
      }
    }

  /**
    * Sentinel returned by readLength when the header's CRLF has not arrived yet.
    */
  private inline def Incomplete: Int = Int.MinValue

  /**
    * Sentinel returned by readLength when the header is not a valid length.
    */
  private inline def Invalid: Int = Int.MinValue + 1

  /**
    * Reads a length header at `pos` up to its CRLF and sets `cursor` past it. Returns the length, -1 for the RESP2 null marker (only if
    * `allowNull`), or a sentinel.
    */
  private def readLength(pos: Int, allowNull: Boolean): Int = {
    val cr = findCrlf(pos)
    if (cr < 0) Incomplete
    else {
      val value = readLong(pos, cr)
      if (!numberOk || value > Int.MaxValue || value < -1 || (value == -1 && !allowNull)) Invalid
      else {
        cursor = cr + 2
        value.toInt
      }
    }
  }

  private def headerText(pos: Int): String = stringAt(pos, findCrlf(pos))

  private def parseElements(start: Int, count: Int): Vector[Frame] = {
    var elements = Vector.empty[Frame]
    var pos      = start
    var i        = 0
    while (i < count) {
      val frame = parseFrame(pos)
      if (frame == null) return null
      elements :+= frame
      pos = cursor
      i += 1
    }
    cursor = pos
    elements
  }

  private def parsePairs(start: Int, count: Int): Vector[(Frame, Frame)] = {
    var entries = Vector.empty[(Frame, Frame)]
    var pos     = start
    var i       = 0
    while (i < count) {
      val key   = parseFrame(pos)
      if (key == null) return null
      val value = parseFrame(cursor)
      if (value == null) return null
      entries :+= ((key, value))
      pos = cursor
      i += 1
    }
    cursor = pos
    entries
  }

  /**
    * Index of the next CRLF's '\r' at or after `from`, or -1 if the input ends first.
    */
  private def findCrlf(from: Int): Int = {
    var i     = from
    val limit = writePos - 1
    while (i < limit && (buf(i) != '\r' || buf(i + 1) != '\n'))
      i += 1
    if (i < limit) i else -1
  }

  /**
    * Parses a decimal long directly from the buffer, signalling validity through `numberOk` — no String or Option allocation on the fast
    * path. Falls back to String parsing past 18 digits, where overflow becomes possible.
    */
  private def readLong(from: Int, until: Int): Long = {
    numberOk = false
    var i        = from
    var negative = false
    if (i < until && buf(i) == '-') {
      negative = true
      i += 1
    } else if (i < until && buf(i) == '+') {
      i += 1
    }
    if (i >= until || until - i > 18) return readLongSlow(from, until)
    var value    = 0L
    while (i < until) {
      val digit = buf(i) - '0'
      if (digit < 0 || digit > 9) return 0L
      value = value * 10 + digit
      i += 1
    }
    numberOk = true
    if (negative) -value else value
  }

  private def readLongSlow(from: Int, until: Int): Long =
    stringAt(from, until).toLongOption match {
      case Some(value) =>
        numberOk = true
        value
      case None        => 0L
    }

  private def stringAt(from: Int, until: Int): String =
    new String(buf, from, until - from, java.nio.charset.StandardCharsets.UTF_8)

  private def bytesAt(from: Int, until: Int): Bytes =
    Bytes.wrap(IArray.unsafeFromArray(java.util.Arrays.copyOfRange(buf, from, until)))

  private def fail(message: String): Frame = {
    failMessage = message
    null
  }
}

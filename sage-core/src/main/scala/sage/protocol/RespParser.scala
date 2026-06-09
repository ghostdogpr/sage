package sage.protocol

import scala.annotation.switch

import sage.Bytes
import sage.SageException.ProtocolError

/**
  * Incremental RESP3 parser: feed it bytes as they arrive, get back every frame completed so far. One instance per connection; not
  * thread-safe. After a `ProtocolError` the parser is poisoned — RESP3 has no resynchronization point — and the connection must be
  * discarded. RESP2 null forms (`$-1`, `*-1`) parse as [[Frame.Null]]; streamed types are not supported (no server sends them).
  */
final class RespParser {

  private var buf: Array[Byte]       = Array.emptyByteArray
  private var readPos: Int           = 0 // start of unconsumed input
  private var writePos: Int          = 0 // end of valid input
  private var failure: ProtocolError = null

  // parseFrame out-fields, avoiding a result-wrapper allocation per frame: frame returned + `cursor` past it = parsed;
  // null + `failMessage` = invalid; null without = incomplete
  private var cursor: Int         = 0
  private var failMessage: String = null

  // readLong validity out-field
  private var numberOk: Boolean = false

  /**
    * Returns every frame completed by `bytes`, in order.
    */
  def feed(bytes: Bytes): Either[ProtocolError, Vector[Frame]] = {
    val frames = Vector.newBuilder[Frame]
    val array  = bytes.unsafeArray
    feed(array, 0, array.length)(frames += _) match {
      case Some(error) => Left(error)
      case None        => Right(frames.result())
    }
  }

  /**
    * Parses every frame completed by `array(offset until offset + length)`, passing each to `onFrame` in order. Avoids the input copy and
    * frame Vector of the [[feed]] overload: the slice is copied straight into the internal buffer (never retained) and frames stream out.
    */
  def feed(array: Array[Byte], offset: Int, length: Int)(onFrame: Frame => Unit): Option[ProtocolError] =
    if (failure != null) Some(failure)
    else if (!append(array, offset, length)) Some(poison("input exceeds the maximum buffer size"))
    else {
      var done = false
      while (!done) {
        val frame = parseFrame(readPos, 0)
        if (frame == null) done = true
        else {
          onFrame(frame)
          readPos = cursor
        }
      }
      if (failMessage != null) Some(poison(failMessage))
      else {
        if (readPos == writePos) {
          readPos = 0
          writePos = 0
        }
        None
      }
    }

  private def poison(message: String): ProtocolError = {
    val error = ProtocolError(message)
    failure = error
    buf = Array.emptyByteArray
    readPos = 0
    writePos = 0
    error
  }

  // compacts in place when the consumed front frees enough room, grows geometrically otherwise; Long arithmetic so capacity
  // computations cannot overflow, false when the unconsumed input would exceed the maximum array size
  private def append(incoming: Array[Byte], offset: Int, length: Int): Boolean = {
    val unparsed = writePos - readPos
    val needed   = unparsed.toLong + length
    if (needed > MaxBuffer) false
    else {
      if (buf.length - writePos < length) {
        if (buf.length >= needed) {
          System.arraycopy(buf, readPos, buf, 0, unparsed)
        } else {
          var capacity = math.max(buf.length.toLong * 2, 256L)
          while (capacity < needed) capacity *= 2
          val grown    = new Array[Byte](math.min(capacity, MaxBuffer).toInt)
          System.arraycopy(buf, readPos, grown, 0, unparsed)
          buf = grown
        }
        readPos = 0
        writePos = unparsed
      }
      System.arraycopy(incoming, offset, buf, writePos, length)
      writePos += length
      true
    }
  }

  // attributes are out-of-band metadata prefixing a value, valid at any nesting level; skip them iteratively (not recursively, so a long
  // attribute chain cannot overflow the stack) and return the value they annotate, so an attribute never occupies an aggregate slot
  private def parseFrame(pos: Int, depth: Int): Frame = {
    var p = pos
    while (p < writePos && buf(p) == '|') {
      val after = skipAttribute(p + 1, depth)
      if (after < 0) return null // Incomplete, or Invalid with failMessage set
      p = after
    }
    parseValue(p, depth)
  }

  // consumes a `|` attribute's pairs without materializing them; returns the position past it, or Incomplete / Invalid (failMessage set)
  private def skipAttribute(pos: Int, depth: Int): Int = {
    val count = readLength(pos, allowNull = false)
    if (count == Incomplete) Incomplete
    else if (count == Invalid) { failMessage = s"invalid attribute length: '${headerText(pos)}'"; Invalid }
    else {
      var p = cursor
      var i = 0
      while (i < count) {
        val key   = parseFrame(p, depth + 1)
        if (key == null) return if (failMessage != null) Invalid else Incomplete
        val value = parseFrame(cursor, depth + 1)
        if (value == null) return if (failMessage != null) Invalid else Incomplete
        p = cursor
        i += 1
      }
      p
    }
  }

  private def parseValue(pos: Int, depth: Int): Frame =
    if (pos >= writePos) null
    else if (depth > MaxDepth) fail(s"aggregate nesting exceeds $MaxDepth levels")
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
          else if (length == -1) Frame.Null
          else {
            val start = cursor
            val end   = payloadEnd(start, length)
            if (end < 0) null
            else {
              cursor = end
              Frame.BulkString(bytesAt(start, start + length))
            }
          }
        case '!'   =>
          val length = readLength(pos + 1, allowNull = false)
          if (length == Incomplete) null
          else if (length == Invalid) fail(s"invalid bulk error length: '${headerText(pos + 1)}'")
          else {
            val start = cursor
            val end   = payloadEnd(start, length)
            if (end < 0) null
            else {
              cursor = end
              Frame.BulkError(bytesAt(start, start + length))
            }
          }
        case '='   =>
          val length = readLength(pos + 1, allowNull = false)
          if (length == Incomplete) null
          else if (length < 4) fail(s"invalid verbatim string length: '${headerText(pos + 1)}'")
          else {
            val start = cursor
            val end   = payloadEnd(start, length)
            if (end < 0) null
            else if (buf(start + 3) != ':') fail("verbatim string missing ':' separator")
            else {
              cursor = end
              Frame.VerbatimString(stringAt(start, start + 3), bytesAt(start + 4, start + length))
            }
          }
        case '*'   => parseAggregate(pos + 1, allowNull = true, Frame.Array.apply, depth)
        case '~'   => parseAggregate(pos + 1, allowNull = false, Frame.Set.apply, depth)
        case '>'   => parseAggregate(pos + 1, allowNull = false, Frame.Push.apply, depth)
        case '%'   => parsePairAggregate(pos + 1, Frame.Map.apply, depth)
        case other =>
          fail(f"unknown frame type byte 0x${other.toByte}%02x")
      }
    }

  // readLength/payloadEnd sentinels
  private inline def Incomplete: Int = Int.MinValue
  private inline def Invalid: Int    = Int.MinValue + 1

  // reads a length header up to its CRLF, setting `cursor` past it; -1 is the RESP2 null marker; '+' is signed-integer
  // syntax that the length grammar does not permit
  private def readLength(pos: Int, allowNull: Boolean): Int = {
    val cr = findCrlf(pos)
    if (cr < 0) Incomplete
    else if (buf(pos) == '+') Invalid
    else {
      val value = readLong(pos, cr)
      if (!numberOk || value > Int.MaxValue || value < -1 || (value == -1 && !allowNull)) Invalid
      else {
        cursor = cr + 2
        value.toInt
      }
    }
  }

  // bounds-checks a length-prefixed payload (Long arithmetic: `start + length + 2` can overflow Int); returns the position
  // past its trailing CRLF, Incomplete, or Invalid with `failMessage` set
  private def payloadEnd(start: Int, length: Int): Int =
    if ((writePos - start).toLong < length.toLong + 2) Incomplete
    else if (buf(start + length) != '\r' || buf(start + length + 1) != '\n') {
      failMessage = "missing CRLF after bulk payload"
      Invalid
    } else start + length + 2

  private def headerText(pos: Int): String = stringAt(pos, findCrlf(pos))

  private def parseAggregate(pos: Int, allowNull: Boolean, make: Vector[Frame] => Frame, depth: Int): Frame = {
    val count = readLength(pos, allowNull)
    if (count == Incomplete) null
    else if (count == Invalid) fail(s"invalid aggregate length: '${headerText(pos)}'")
    else if (count == -1) Frame.Null
    else {
      val elements = parseElements(cursor, count, depth)
      if (elements == null) null else make(elements)
    }
  }

  private def parsePairAggregate(pos: Int, make: Vector[(Frame, Frame)] => Frame, depth: Int): Frame = {
    val count = readLength(pos, allowNull = false)
    if (count == Incomplete) null
    else if (count == Invalid) fail(s"invalid aggregate length: '${headerText(pos)}'")
    else {
      val entries = parsePairs(cursor, count, depth)
      if (entries == null) null else make(entries)
    }
  }

  private def parseElements(start: Int, count: Int, depth: Int): Vector[Frame] = {
    val elements = Vector.newBuilder[Frame]
    var pos      = start
    var i        = 0
    while (i < count) {
      val frame = parseFrame(pos, depth + 1)
      if (frame == null) return null
      elements += frame
      pos = cursor
      i += 1
    }
    cursor = pos
    elements.result()
  }

  private def parsePairs(start: Int, count: Int, depth: Int): Vector[(Frame, Frame)] = {
    val entries = Vector.newBuilder[(Frame, Frame)]
    var pos     = start
    var i       = 0
    while (i < count) {
      val key   = parseFrame(pos, depth + 1)
      if (key == null) return null
      val value = parseFrame(cursor, depth + 1)
      if (value == null) return null
      entries += ((key, value))
      pos = cursor
      i += 1
    }
    cursor = pos
    entries.result()
  }

  // index of the next CRLF's '\r', or -1 if the input ends first
  private def findCrlf(from: Int): Int = {
    var i     = from
    val limit = writePos - 1
    while (i < limit && (buf(i) != '\r' || buf(i + 1) != '\n'))
      i += 1
    if (i < limit) i else -1
  }

  // parses a decimal long from the buffer, signalling validity via `numberOk`; falls back to String parsing past 18 digits,
  // where overflow becomes possible
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

  // largest unconsumed input the parser will buffer (the JVM's max array size)
  private inline def MaxBuffer: Long = Int.MaxValue - 8

  // bound on aggregate nesting so a hostile reply poisons cleanly instead of overflowing the JVM stack; real replies are shallow
  private inline def MaxDepth: Int = 512
}

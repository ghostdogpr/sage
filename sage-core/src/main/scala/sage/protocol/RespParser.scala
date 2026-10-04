package sage.protocol

import scala.annotation.switch
import scala.collection.mutable

import sage.Bytes
import sage.SageException.ProtocolError

/**
  * Incremental RESP3 parser. Each call accepts newly received bytes and returns any frames completed by them. A parser belongs to one
  * connection and is not thread-safe. RESP3 has no resynchronization point, so a `ProtocolError` makes the parser unusable and the
  * connection must be closed. RESP2 null forms (`$-1`, `*-1`) parse as [[Frame.Null]]. Streamed types are not supported because Redis and
  * Valkey do not send them.
  *
  * An explicit stack holds partially parsed aggregates between calls. This avoids recursion and avoids scanning the start of a large reply
  * again when it arrives over several reads. Each completed element advances `readPos`, allowing the input buffer to retain only the
  * unparsed bytes.
  */
final private[sage] class RespParser {

  private var buf: Array[Byte]       = Array.emptyByteArray
  private var readPos: Int           = 0
  private var writePos: Int          = 0
  private var failure: ProtocolError = null

  // out-field, avoiding a result-wrapper allocation per parsed value
  private var produced: Frame = null

  private var numberOk: Boolean = false

  private var stack      = new Array[Agg](8)
  private var stackDepth = 0

  // track the largest buffer requirement for the current read and how many completed smaller reads followed an oversized allocation
  private var highWater: Int   = 0
  private var quietDrains: Int = 0

  private[protocol] def unsafeBuffer: Array[Byte] = buf

  /**
    * Parses every frame completed by `array(offset until offset + length)` and passes each one to `onFrame` in order. The slice is copied
    * into the parser's internal buffer.
    */
  def feed(array: Array[Byte], offset: Int, length: Int)(onFrame: Frame => Unit): Option[ProtocolError] =
    if (failure != null) Some(failure)
    else if (!append(array, offset, length)) Some(poison("input exceeds the maximum buffer size"))
    else {
      parseLoop(onFrame)
      if (failure != null) Some(failure)
      else {
        // partial aggregates remain on the stack, so an empty input buffer can be reset between reads.
        if (readPos == writePos) {
          readPos = 0
          writePos = 0
          // release an unusually large buffer after several smaller reads, while reusing it for consecutive large replies.
          if (buf.length > MaxRetainedBuffer) {
            if (highWater > MaxRetainedBuffer) quietDrains = 0
            else {
              quietDrains += 1
              if (quietDrains >= ShrinkAfterDrains) {
                buf = Array.emptyByteArray
                quietDrains = 0
              }
            }
          }
          highWater = 0
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
    stackDepth = 0
    error
  }

  // Compacts in place when the consumed front frees enough room, grows geometrically otherwise; Long arithmetic so capacity
  // computations cannot overflow, false when the unconsumed input would exceed the maximum array size
  private def append(incoming: Array[Byte], offset: Int, length: Int): Boolean = {
    val unparsed = writePos - readPos
    val needed   = unparsed.toLong + length
    if (needed > MaxBuffer) false
    else {
      if (needed > highWater) highWater = needed.toInt
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

  private def parseLoop(onFrame: Frame => Unit): Unit = {
    var status = Opened
    while (status == Produced || status == Opened) {
      // close completed aggregates first: this also finalizes an empty aggregate (zero children) the instant it is opened
      while (stackDepth > 0 && complete(stack(stackDepth - 1))) {
        stackDepth -= 1
        val top = stack(stackDepth)
        stack(stackDepth) = null
        // an attribute yields no value; the value it prefixes is produced next, for the same slot
        if (top.kind != Attr) deliver(build(top), onFrame)
      }
      status = produceValue()
      if (status == Produced) deliver(produced, onFrame)
    }
  }

  private def deliver(value: Frame, onFrame: Frame => Unit): Unit =
    if (stackDepth == 0) onFrame(value) else addChild(stack(stackDepth - 1), value)

  private def complete(agg: Agg): Boolean = agg.remaining == 0 && agg.pendingKey == null

  private def addChild(agg: Agg, value: Frame): Unit =
    agg.kind match {
      case Map | Attr =>
        if (agg.pendingKey == null) agg.pendingKey = value
        else {
          agg.pairs += ((agg.pendingKey, value))
          agg.pendingKey = null
          agg.remaining -= 1
        }
      case _          =>
        agg.elements += value
        agg.remaining -= 1
    }

  private def build(agg: Agg): Frame =
    agg.kind match {
      case Arr  => Frame.Array(agg.elements.result())
      case Set  => Frame.Set(agg.elements.result())
      case Push => Frame.Push(agg.elements.result())
      case Map  => Frame.Map(agg.pairs.result())
      case _    => Frame.Null // Attr is never built because the parser discards completed attributes before this point.
    }

  // Produces one value at `readPos`: Produced (`produced` set, `readPos` advanced), Opened (header pushed), Incomplete (`readPos` unmoved),
  // or Invalid (the parser is poisoned)
  private def produceValue(): Int =
    if (readPos >= writePos) Incomplete
    else {
      val pos = readPos
      val cr  = findCrlf(pos + 1)
      if (cr < 0) { if (FrameTypes.indexOf(buf(pos).toInt) >= 0) Incomplete else unknownType(buf(pos)) }
      else
        (buf(pos).toChar: @switch) match {
          case '+'   => leaf(cr + 2, Frame.SimpleString(stringAt(pos + 1, cr)))
          case '-'   => leaf(cr + 2, Frame.SimpleError(stringAt(pos + 1, cr)))
          case ':'   =>
            val value = readLong(pos + 1, cr)
            if (!numberOk) fail(s"invalid integer: '${stringAt(pos + 1, cr)}'") else leaf(cr + 2, Frame.Integer(value))
          case ','   =>
            val text = stringAt(pos + 1, cr)
            try {
              val value = text match {
                case "inf" | "+inf" => java.lang.Double.POSITIVE_INFINITY
                case "-inf"         => java.lang.Double.NEGATIVE_INFINITY
                case "nan"          => java.lang.Double.NaN
                case other          => java.lang.Double.parseDouble(other)
              }
              leaf(cr + 2, Frame.Double(value))
            } catch { case _: NumberFormatException => fail(s"invalid double: '$text'") }
          case '#'   =>
            if (cr != pos + 2) fail(s"invalid boolean: '${stringAt(pos + 1, cr)}'")
            else
              buf(pos + 1).toChar match {
                case 't' => leaf(cr + 2, Frame.Bool(true))
                case 'f' => leaf(cr + 2, Frame.Bool(false))
                case _   => fail(s"invalid boolean: '${stringAt(pos + 1, cr)}'")
              }
          case '('   =>
            val text = stringAt(pos + 1, cr)
            try leaf(cr + 2, Frame.BigNumber(BigInt(text)))
            catch { case _: NumberFormatException => fail(s"invalid big number: '$text'") }
          case '_'   =>
            if (cr != pos + 1) fail(s"unexpected content in null frame: '${stringAt(pos + 1, cr)}'")
            else leaf(cr + 2, Frame.Null)
          case '$'   => bulk(pos, cr, '$', -1, "invalid bulk string length")
          case '!'   => bulk(pos, cr, '!', 0, "invalid bulk error length")
          case '='   => bulk(pos, cr, '=', 4, "invalid verbatim string length")
          case '*'   => open(pos, cr, Arr, -1, "invalid array length")
          case '~'   => open(pos, cr, Set, 0, "invalid set length")
          case '>'   => open(pos, cr, Push, 0, "invalid push length")
          case '%'   => open(pos, cr, Map, 0, "invalid map length")
          case '|'   => open(pos, cr, Attr, 0, "invalid attribute length")
          case other => unknownType(other.toByte)
        }
    }

  private def unknownType(byte: Byte): Int = fail(f"unknown frame type byte 0x$byte%02x")

  private def leaf(end: Int, frame: Frame): Int = {
    readPos = end
    produced = frame
    Produced
  }

  private def bulk(pos: Int, cr: Int, kind: Char, minLength: Int, lengthError: String): Int = {
    val length = readLength(pos + 1, cr)
    if (length < minLength) fail(s"$lengthError: '${stringAt(pos + 1, cr)}'")
    else if (length == -1) leaf(cr + 2, Frame.Null)
    else {
      val start = cr + 2
      val end   = payloadEnd(start, length)
      if (end < 0) end // Incomplete, or Invalid after payloadEnd poisoned the parser
      else if (kind == '$') leaf(end, Frame.BulkString(bytesAt(start, start + length)))
      else if (kind == '!') leaf(end, Frame.BulkError(bytesAt(start, start + length)))
      else if (buf(start + 3) != ':') fail("verbatim string missing ':' separator")
      else leaf(end, Frame.VerbatimString(stringAt(start, start + 3), bytesAt(start + 4, start + length)))
    }
  }

  private def open(pos: Int, cr: Int, kind: Byte, minCount: Int, lengthError: String): Int = {
    val count = readLength(pos + 1, cr)
    if (count < minCount) fail(s"$lengthError: '${stringAt(pos + 1, cr)}'")
    else if (count == -1) leaf(cr + 2, Frame.Null)
    else {
      readPos = cr + 2
      val agg = new Agg(kind, count)
      if (kind == Map || kind == Attr) {
        agg.pairs = Vector.newBuilder[(Frame, Frame)]
        agg.pairs.sizeHint(count)
      } else {
        agg.elements = Vector.newBuilder[Frame]
        agg.elements.sizeHint(count)
      }
      push(agg)
    }
  }

  // check depth when opening an aggregate. A leaf at the deepest allowed level remains valid.
  private def push(agg: Agg): Int =
    if (stackDepth >= MaxDepth) fail(s"aggregate nesting exceeds $MaxDepth levels")
    else {
      if (stackDepth == stack.length) {
        val grown = new Array[Agg](math.min(stack.length * 2, MaxDepth))
        System.arraycopy(stack, 0, grown, 0, stackDepth)
        stack = grown
      }
      stack(stackDepth) = agg
      stackDepth += 1
      Opened
    }

  // -1 is the RESP2 null marker; '+' is signed-integer syntax that the length grammar does not permit
  private def readLength(pos: Int, cr: Int): Int =
    if (buf(pos) == '+') Invalid
    else {
      val value = readLong(pos, cr)
      if (!numberOk || value > Int.MaxValue || value < -1) Invalid else value.toInt
    }

  // Long arithmetic: `start + length + 2` can overflow Int
  private def payloadEnd(start: Int, length: Int): Int =
    if ((writePos - start).toLong < length.toLong + 2) Incomplete
    else if (buf(start + length) != '\r' || buf(start + length + 1) != '\n') fail("missing CRLF after bulk payload")
    else start + length + 2

  // index of the next CRLF's '\r', or -1 if the input ends first
  private def findCrlf(from: Int): Int = {
    var i     = from
    val limit = writePos - 1
    while (i < limit && (buf(i) != '\r' || buf(i + 1) != '\n'))
      i += 1
    if (i < limit) i else -1
  }

  // falls back to String parsing past 18 digits, where the fast-path accumulation could overflow
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

  private def fail(message: String): Int = {
    poison(message)
    Invalid
  }

  // Incomplete/Invalid also serve as readLength/payloadEnd sentinels, so they must stay below -1 and every valid Int position/length
  final private val Incomplete: Int = Int.MinValue
  final private val Invalid: Int    = Int.MinValue + 1
  final private val Produced: Int   = Int.MinValue + 2
  final private val Opened: Int     = Int.MinValue + 3

  private val FrameTypes = "+-:,#(_$!=*~>%|"

  final private val Arr: Byte  = 0
  final private val Set: Byte  = 1
  final private val Push: Byte = 2
  final private val Map: Byte  = 3
  final private val Attr: Byte = 4

  // largest unconsumed input the parser will buffer (the JVM's max array size)
  private inline def MaxBuffer: Long = Int.MaxValue - 8

  // largest buffer retained after all input has been parsed
  private inline def MaxRetainedBuffer: Int = 1 << 20

  // number of consecutive completed reads below MaxRetainedBuffer before releasing an oversized buffer
  private inline def ShrinkAfterDrains: Int = 8

  // reject excessive aggregate nesting before it can exhaust memory. Normal server replies are much shallower.
  private inline def MaxDepth: Int = 512

  // for Map/Attr `remaining` counts pairs, not elements
  final private class Agg(val kind: Byte, var remaining: Int) {
    var elements: mutable.Builder[Frame, Vector[Frame]]                = null
    var pairs: mutable.Builder[(Frame, Frame), Vector[(Frame, Frame)]] = null
    var pendingKey: Frame                                              = null
  }
}

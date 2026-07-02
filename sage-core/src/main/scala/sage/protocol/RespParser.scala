package sage.protocol

import scala.annotation.switch
import scala.collection.mutable

import sage.Bytes
import sage.SageException.ProtocolError

/**
  * Incremental RESP3 parser: feed it bytes as they arrive, get back every frame completed so far. One instance per connection; not
  * thread-safe. After a `ProtocolError` the parser is poisoned — RESP3 has no resynchronization point — and the connection must be
  * discarded. RESP2 null forms (`$-1`, `*-1`) parse as [[Frame.Null]]; streamed types are not supported (no server sends them).
  *
  * Aggregates parse incrementally against an explicit stack of open frames rather than recursively: a feed that ends mid-aggregate keeps
  * the partial builders on the stack and resumes from there, so a large reply split across many reads is parsed once, not re-scanned from
  * the top on every read. Because each completed element advances `readPos`, the input buffer only ever holds the unparsed tail, never the
  * whole aggregate.
  */
final private[sage] class RespParser {

  private var buf: Array[Byte]       = Array.emptyByteArray
  private var readPos: Int           = 0
  private var writePos: Int          = 0
  private var failure: ProtocolError = null

  // out-fields, avoiding a result-wrapper allocation per parsed value
  private var cursor: Int         = 0
  private var produced: Frame     = null
  private var failMessage: String = null

  private var numberOk: Boolean = false

  private var stack      = new Array[Agg](8)
  private var stackDepth = 0

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
      parseLoop(onFrame)
      if (failMessage != null) Some(poison(failMessage))
      else {
        // the stack carries partial frames independently of the buffer, so the buffer can reset whenever its bytes are spent
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
    stackDepth = 0
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

  private def parseLoop(onFrame: Frame => Unit): Unit = {
    var running = true
    while (running) {
      // close completed aggregates first: this also finalizes an empty aggregate (zero children) the instant it is opened
      var closing = true
      while (closing)
        if (stackDepth > 0 && complete(stack(stackDepth - 1))) {
          val top = stack(stackDepth - 1)
          stack(stackDepth - 1) = null
          stackDepth -= 1
          if (top.kind == Attr) closing = false // an attribute yields no value; the value it prefixes is produced next, for the same slot
          else {
            val value = build(top)
            if (stackDepth == 0) { onFrame(value); closing = false }
            else addChild(stack(stackDepth - 1), value) // re-check: the parent may now be complete too
          }
        } else closing = false

      val status = produceValue()
      if (status == Produced) {
        val value = produced
        if (stackDepth == 0) onFrame(value) else addChild(stack(stackDepth - 1), value)
      } else if (status == Opened) () // re-loop: the close pass finalizes it if empty, otherwise its children are produced next
      else running = false
    }
  }

  private def complete(agg: Agg): Boolean = agg.remaining == 0 && agg.pendingKey == null

  private def addChild(agg: Agg, value: Frame): Unit =
    agg.kind match {
      case Map  =>
        if (agg.pendingKey == null) agg.pendingKey = value
        else {
          agg.pairs += ((agg.pendingKey, value))
          agg.pendingKey = null
          agg.remaining -= 1
        }
      case Attr => // discarded metadata: count off each pair without materializing it
        if (agg.pendingKey == null) agg.pendingKey = value
        else {
          agg.pendingKey = null
          agg.remaining -= 1
        }
      case _    =>
        agg.elements += value
        agg.remaining -= 1
    }

  private def build(agg: Agg): Frame =
    agg.kind match {
      case Arr  => Frame.Array(agg.elements.result())
      case Set  => Frame.Set(agg.elements.result())
      case Push => Frame.Push(agg.elements.result())
      case Map  => Frame.Map(agg.pairs.result())
      case _    => Frame.Null // Attr is never built — completed attributes are discarded before this point
    }

  // produces one value at `readPos`: Produced (`produced` set, `readPos` advanced), Opened (header pushed), Incomplete (`readPos` unmoved),
  // or Invalid (`failMessage` set)
  private def produceValue(): Int =
    if (readPos >= writePos) Incomplete
    else {
      val pos = readPos
      (buf(pos).toChar: @switch) match {
        case '+'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) Incomplete else leaf(cr + 2, Frame.SimpleString(stringAt(pos + 1, cr)))
        case '-'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) Incomplete else leaf(cr + 2, Frame.SimpleError(stringAt(pos + 1, cr)))
        case ':'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) Incomplete
          else {
            val value = readLong(pos + 1, cr)
            if (!numberOk) fail(s"invalid integer: '${stringAt(pos + 1, cr)}'") else leaf(cr + 2, Frame.Integer(value))
          }
        case ','   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) Incomplete
          else {
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
          }
        case '#'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) Incomplete
          else if (cr != pos + 2) fail(s"invalid boolean: '${stringAt(pos + 1, cr)}'")
          else
            buf(pos + 1).toChar match {
              case 't' => leaf(cr + 2, Frame.Bool(true))
              case 'f' => leaf(cr + 2, Frame.Bool(false))
              case _   => fail(s"invalid boolean: '${stringAt(pos + 1, cr)}'")
            }
        case '('   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) Incomplete
          else {
            val text = stringAt(pos + 1, cr)
            try leaf(cr + 2, Frame.BigNumber(BigInt(text)))
            catch { case _: NumberFormatException => fail(s"invalid big number: '$text'") }
          }
        case '_'   =>
          val cr = findCrlf(pos + 1)
          if (cr < 0) Incomplete
          else if (cr != pos + 1) fail(s"unexpected content in null frame: '${stringAt(pos + 1, cr)}'")
          else leaf(cr + 2, Frame.Null)
        case '$'   => bulk(pos, allowNull = true, "invalid bulk string length", isError = false)
        case '!'   => bulk(pos, allowNull = false, "invalid bulk error length", isError = true)
        case '='   => verbatim(pos)
        case '*'   => openElements(pos, Arr, allowNull = true)
        case '~'   => openElements(pos, Set, allowNull = false)
        case '>'   => openElements(pos, Push, allowNull = false)
        case '%'   => openPairs(pos, Map)
        case '|'   => openPairs(pos, Attr)
        case other =>
          fail(f"unknown frame type byte 0x${other.toByte}%02x")
      }
    }

  private def leaf(end: Int, frame: Frame): Int = {
    readPos = end
    produced = frame
    Produced
  }

  private def bulk(pos: Int, allowNull: Boolean, lengthError: String, isError: Boolean): Int = {
    val length = readLength(pos + 1, allowNull)
    if (length == Incomplete) Incomplete
    else if (length == Invalid) fail(s"$lengthError: '${headerText(pos + 1)}'")
    else if (length == -1) leaf(cursor, Frame.Null)
    else {
      val start = cursor
      val end   = payloadEnd(start, length)
      if (end == Incomplete) Incomplete
      else if (end == Invalid) Invalid // payloadEnd set failMessage
      else {
        val bytes = bytesAt(start, start + length)
        leaf(end, if (isError) Frame.BulkError(bytes) else Frame.BulkString(bytes))
      }
    }
  }

  private def verbatim(pos: Int): Int = {
    val length = readLength(pos + 1, allowNull = false)
    if (length == Incomplete) Incomplete
    else if (length < 4) fail(s"invalid verbatim string length: '${headerText(pos + 1)}'") // an Invalid sentinel is < 4 too
    else {
      val start = cursor
      val end   = payloadEnd(start, length)
      if (end == Incomplete) Incomplete
      else if (end == Invalid) Invalid
      else if (buf(start + 3) != ':') fail("verbatim string missing ':' separator")
      else leaf(end, Frame.VerbatimString(stringAt(start, start + 3), bytesAt(start + 4, start + length)))
    }
  }

  private def openElements(pos: Int, kind: Byte, allowNull: Boolean): Int = {
    val count = readLength(pos + 1, allowNull)
    if (count == Incomplete) Incomplete
    else if (count == Invalid) fail(s"${lengthErrorFor(kind)}: '${headerText(pos + 1)}'")
    else if (count == -1) leaf(cursor, Frame.Null)
    else {
      readPos = cursor
      val agg = new Agg(kind, count)
      agg.elements = Vector.newBuilder[Frame]
      agg.elements.sizeHint(count)
      push(agg)
    }
  }

  private def openPairs(pos: Int, kind: Byte): Int = {
    val count = readLength(pos + 1, allowNull = false)
    if (count == Incomplete) Incomplete
    else if (count == Invalid) fail(s"${lengthErrorFor(kind)}: '${headerText(pos + 1)}'")
    else {
      readPos = cursor
      val agg = new Agg(kind, count)
      if (kind == Map) { agg.pairs = Vector.newBuilder[(Frame, Frame)]; agg.pairs.sizeHint(count) }
      push(agg)
    }
  }

  // bounds nesting at open time (not on every value), so a leaf at the deepest allowed level is still accepted
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

  // Long arithmetic: `start + length + 2` can overflow Int
  private def payloadEnd(start: Int, length: Int): Int =
    if ((writePos - start).toLong < length.toLong + 2) Incomplete
    else if (buf(start + length) != '\r' || buf(start + length + 1) != '\n') {
      failMessage = "missing CRLF after bulk payload"
      Invalid
    } else start + length + 2

  private def headerText(pos: Int): String = stringAt(pos, findCrlf(pos))

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
    failMessage = message
    Invalid
  }

  // Incomplete/Invalid also serve as readLength/payloadEnd sentinels, so they must stay distinct from any valid Int position/length
  final private val Incomplete: Int = Int.MinValue
  final private val Invalid: Int    = Int.MinValue + 1
  final private val Produced: Int   = Int.MinValue + 2
  final private val Opened: Int     = Int.MinValue + 3

  final private val Arr: Byte  = 0
  final private val Set: Byte  = 1
  final private val Push: Byte = 2
  final private val Map: Byte  = 3
  final private val Attr: Byte = 4

  private def lengthErrorFor(kind: Byte): String = kind match {
    case Arr  => "invalid array length"
    case Set  => "invalid set length"
    case Push => "invalid push length"
    case Map  => "invalid map length"
    case Attr => "invalid attribute length"
    case _    => "invalid aggregate length"
  }

  // largest unconsumed input the parser will buffer (the JVM's max array size)
  private inline def MaxBuffer: Long = Int.MaxValue - 8

  // bound on aggregate nesting so a hostile reply poisons cleanly instead of overflowing the JVM stack; real replies are shallow
  private inline def MaxDepth: Int = 512

  // for Map/Attr `remaining` counts pairs, not elements
  final private class Agg(val kind: Byte, var remaining: Int) {
    var elements: mutable.Builder[Frame, Vector[Frame]]                = null
    var pairs: mutable.Builder[(Frame, Frame), Vector[(Frame, Frame)]] = null
    var pendingKey: Frame                                              = null
  }
}

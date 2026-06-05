package sage.protocol

import java.nio.charset.StandardCharsets

import sage.Bytes

/**
  * Writes a [[Frame]] to its RESP3 wire form. The dual of [[RespParser]].
  *
  * On every command's hot path, so it writes into an unsynchronized growable sink (java.io.ByteArrayOutputStream locks on every call) and
  * emits integers as digits directly, without going through String.
  */
object RespWriter {

  def write(frame: Frame): Bytes = {
    val sink = new Sink(64)
    writeFrame(frame, sink)
    sink.result()
  }

  /**
    * Encodes a command — name words plus pre-encoded args, one bulk string each — without building the intermediate frames.
    */
  private[sage] def writeCommand(name: String, args: Vector[Bytes]): Bytes = {
    val words = name.split(' ')
    val sink  = new Sink(64)
    sink.writeByte('*')
    sink.writeLong(words.length.toLong + args.length)
    sink.writeCrlf()
    var i     = 0
    while (i < words.length) {
      writeBulk('$', Bytes.utf8(words(i)), sink)
      i += 1
    }
    val it    = args.iterator
    while (it.hasNext)
      writeBulk('$', it.next(), sink)
    sink.result()
  }

  private def writeFrame(frame: Frame, sink: Sink): Unit =
    frame match {
      case Frame.SimpleString(value)           => writeLine('+', value, sink)
      case Frame.SimpleError(value)            => writeLine('-', value, sink)
      case Frame.Integer(value)                =>
        sink.writeByte(':')
        sink.writeLong(value)
        sink.writeCrlf()
      case Frame.BulkString(value)             => writeBulk('$', value, sink)
      case Frame.Array(elements)               => writeAggregate('*', elements, sink)
      case Frame.Null                          =>
        sink.writeByte('_')
        sink.writeCrlf()
      case Frame.Bool(value)                   =>
        sink.writeByte('#')
        sink.writeByte(if (value) 't' else 'f')
        sink.writeCrlf()
      case Frame.Double(value)                 => writeLine(',', formatDouble(value), sink)
      case Frame.BigNumber(value)              => writeLine('(', value.toString, sink)
      case Frame.BulkError(value)              => writeBulk('!', value, sink)
      case Frame.VerbatimString(format, value) =>
        sink.writeByte('=')
        sink.writeLong(value.length.toLong + 4L)
        sink.writeCrlf()
        sink.writeUtf8(format)
        sink.writeByte(':')
        sink.writeBytes(value)
        sink.writeCrlf()
      case Frame.Map(entries)                  => writePairs('%', entries, sink)
      case Frame.Set(elements)                 => writeAggregate('~', elements, sink)
      case Frame.Attribute(entries)            => writePairs('|', entries, sink)
      case Frame.Push(elements)                => writeAggregate('>', elements, sink)
    }

  private def writeLine(kind: Int, content: String, sink: Sink): Unit = {
    sink.writeByte(kind)
    sink.writeUtf8(content)
    sink.writeCrlf()
  }

  private def writeBulk(kind: Int, value: Bytes, sink: Sink): Unit = {
    sink.writeByte(kind)
    sink.writeLong(value.length.toLong)
    sink.writeCrlf()
    sink.writeBytes(value)
    sink.writeCrlf()
  }

  private def writeAggregate(kind: Int, elements: Vector[Frame], sink: Sink): Unit = {
    sink.writeByte(kind)
    sink.writeLong(elements.length.toLong)
    sink.writeCrlf()
    val it = elements.iterator
    while (it.hasNext)
      writeFrame(it.next(), sink)
  }

  private def writePairs(kind: Int, entries: Vector[(Frame, Frame)], sink: Sink): Unit = {
    sink.writeByte(kind)
    sink.writeLong(entries.length.toLong)
    sink.writeCrlf()
    val it = entries.iterator
    while (it.hasNext) {
      val (key, value) = it.next()
      writeFrame(key, sink)
      writeFrame(value, sink)
    }
  }

  private def formatDouble(value: scala.Double): String =
    if (value == java.lang.Double.POSITIVE_INFINITY) "inf"
    else if (value == java.lang.Double.NEGATIVE_INFINITY) "-inf"
    else if (value.isNaN) "nan"
    else value.toString

  /**
    * An unsynchronized growable byte buffer.
    */
  final private class Sink(initialCapacity: Int) {

    private var buf: Array[Byte] = new Array[Byte](initialCapacity)
    private var len: Int         = 0

    def writeByte(value: Int): Unit = {
      ensure(1)
      buf(len) = value.toByte
      len += 1
    }

    def writeCrlf(): Unit = {
      ensure(2)
      buf(len) = '\r'
      buf(len + 1) = '\n'
      len += 2
    }

    def writeBytes(bytes: Bytes): Unit =
      writeArray(bytes.unsafeArray) // read-only view, never mutated

    def writeUtf8(text: String): Unit =
      writeArray(text.getBytes(StandardCharsets.UTF_8))

    def writeLong(value: Long): Unit =
      if (value == Long.MinValue) writeUtf8(value.toString) // -value would overflow; cold path
      else if (value < 0) {
        writeByte('-')
        writeDigits(-value)
      } else {
        writeDigits(value)
      }

    def result(): Bytes = Bytes.wrap(IArray.unsafeFromArray(java.util.Arrays.copyOf(buf, len)))

    private def writeDigits(value: Long): Unit = {
      var digits    = 1
      var ceiling   = 10L
      while (digits < 19 && value >= ceiling) {
        digits += 1
        ceiling *= 10
      }
      ensure(digits)
      var i         = len + digits - 1
      var remaining = value
      while (i >= len) {
        buf(i) = ('0' + (remaining % 10).toInt).toByte
        remaining /= 10
        i -= 1
      }
      len += digits
    }

    private def writeArray(array: Array[Byte]): Unit = {
      ensure(array.length)
      System.arraycopy(array, 0, buf, len, array.length)
      len += array.length
    }

    private def ensure(extra: Int): Unit =
      if (buf.length - len < extra) {
        var capacity = buf.length * 2
        while (capacity - len < extra) capacity *= 2
        buf = java.util.Arrays.copyOf(buf, capacity)
      }
  }
}

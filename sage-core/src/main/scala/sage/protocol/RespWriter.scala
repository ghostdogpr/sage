package sage.protocol

import java.nio.charset.StandardCharsets

import sage.Bytes

/**
  * Encodes commands to wire bytes. A client never writes arbitrary frames, so command encoding is the writer's whole surface.
  */
private[sage] object RespWriter {

  def writeCommand(name: String, args: Vector[Bytes]): Bytes = {
    val sink = new Sink(64)
    if (name.indexOf(' ') < 0) { // single-word fast path: no split allocation
      sink.writeByte('*')
      sink.writeLong(1L + args.length)
      sink.writeCrlf()
      writeBulk(Bytes.utf8(name), sink)
    } else {
      val words = name.split(' ').filter(_.nonEmpty)
      sink.writeByte('*')
      sink.writeLong(words.length.toLong + args.length)
      sink.writeCrlf()
      var w     = 0
      while (w < words.length) {
        writeBulk(Bytes.utf8(words(w)), sink)
        w += 1
      }
    }
    var i    = 0
    while (i < args.length) {
      writeBulk(args(i), sink)
      i += 1
    }
    sink.result()
  }

  private def writeBulk(value: Bytes, sink: Sink): Unit = {
    sink.writeByte('$')
    sink.writeLong(value.length.toLong)
    sink.writeCrlf()
    sink.writeBytes(value)
    sink.writeCrlf()
  }

  /**
    * An unsynchronized growable byte buffer (java.io.ByteArrayOutputStream locks on every call).
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
      writeArray(bytes.unsafeArray)

    def writeLong(value: Long): Unit =
      if (value == Long.MinValue) writeArray(value.toString.getBytes(StandardCharsets.UTF_8)) // -value would overflow
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

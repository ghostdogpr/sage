package sage.protocol

import sage.Bytes

/**
  * Encodes commands to wire bytes. A client never writes arbitrary frames, so command encoding is the writer's whole surface.
  */
private[sage] object RespWriter {

  // Sizes the buffer exactly, so a command encodes into one right-sized array: no oversized scratch and result() returns it without a copy.
  def writeCommand(name: String, args: Vector[Bytes]): Bytes =
    if (name.indexOf(' ') < 0) { // single-word fast path: no split allocation
      val nameBytes = Bytes.utf8(name)
      val count     = 1L + args.length
      val sink      = new Sink(headerSize(count) + bulkSize(nameBytes.length) + argsSize(args))
      sink.writeByte('*')
      sink.writeLong(count)
      sink.writeCrlf()
      writeBulk(nameBytes, sink)
      writeArgs(args, sink)
      sink.result()
    } else {
      val words     = name.split(' ').filter(_.nonEmpty)
      val wordBytes = words.map(Bytes.utf8)
      val count     = wordBytes.length.toLong + args.length
      var bodySize  = 0
      var s         = 0
      while (s < wordBytes.length) { bodySize += bulkSize(wordBytes(s).length); s += 1 }
      val sink      = new Sink(headerSize(count) + bodySize + argsSize(args))
      sink.writeByte('*')
      sink.writeLong(count)
      sink.writeCrlf()
      var w         = 0
      while (w < wordBytes.length) {
        writeBulk(wordBytes(w), sink)
        w += 1
      }
      writeArgs(args, sink)
      sink.result()
    }

  private def writeArgs(args: Vector[Bytes], sink: Sink): Unit = {
    var i = 0
    while (i < args.length) {
      writeBulk(args(i), sink)
      i += 1
    }
  }

  // mirror writeBulk/the array header in RespWriter so the precomputed size matches the bytes actually written
  private def headerSize(count: Long): Int = 1 + digitCount(count) + 2

  private def bulkSize(length: Int): Int = 1 + digitCount(length.toLong) + 2 + length + 2

  private def argsSize(args: Vector[Bytes]): Int = {
    var total = 0
    var i     = 0
    while (i < args.length) {
      total += bulkSize(args(i).length)
      i += 1
    }
    total
  }

  // shared with Sink.writeDigits so the precomputed size and the written digit count never disagree
  private def digitCount(value: Long): Int = {
    var digits  = 1
    var ceiling = 10L
    while (digits < 19 && value >= ceiling) {
      digits += 1
      ceiling *= 10
    }
    digits
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

    // only ever called with non-negative lengths and element counts
    def writeLong(value: Long): Unit = writeDigits(value)

    // zero-copy on the exact-size common path; the copy only covers a growth over-allocation
    def result(): Bytes =
      if (len == buf.length) Bytes.wrap(IArray.unsafeFromArray(buf))
      else Bytes.wrap(IArray.unsafeFromArray(java.util.Arrays.copyOf(buf, len)))

    private def writeDigits(value: Long): Unit = {
      val digits    = digitCount(value)
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

    // Long arithmetic so the doubling cannot overflow to a negative capacity on a multi-GB command; capped at the max array size
    private def ensure(extra: Int): Unit =
      if (buf.length - len < extra) {
        val needed   = len.toLong + extra
        var capacity = buf.length.toLong * 2
        while (capacity < needed) capacity *= 2
        buf = java.util.Arrays.copyOf(buf, math.min(capacity, Int.MaxValue - 8).toInt)
      }
  }
}

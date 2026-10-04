package sage.protocol

import sage.Bytes
import sage.codec.Primitives.{digitCount, writeDigits}

/**
  * Encodes commands as RESP bytes.
  */
private[sage] object RespWriter {

  def writeCommand(name: String, args: Vector[Bytes]): Bytes =
    if (name.indexOf(' ') < 0) write(Bytes.utf8(name), args)
    else
      // a multi-word name such as "XGROUP CREATE" is sent as one bulk string per word
      name.split(' ').iterator.filter(_.nonEmpty).map(Bytes.utf8).toVector match {
        case first +: rest => write(first, rest ++ args)
        case _             => write(Bytes.empty, args)
      }

  // precomputes the buffer size to encode into one array and return it without copying.
  private def write(name: Bytes, args: Vector[Bytes]): Bytes = {
    val count = 1L + args.length
    val sink  = new Sink(headerSize(count) + bulkSize(name.length) + argsSize(args))
    sink.writeByte('*')
    sink.writeLong(count)
    sink.writeCrlf()
    writeBulk(name, sink)
    var i     = 0
    while (i < args.length) {
      writeBulk(args(i), sink)
      i += 1
    }
    sink.result()
  }

  // keep this calculation aligned with writeBulk and the array header written above.
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

  private def writeBulk(value: Bytes, sink: Sink): Unit = {
    sink.writeByte('$')
    sink.writeLong(value.length.toLong)
    sink.writeCrlf()
    sink.writeBytes(value)
    sink.writeCrlf()
  }

  final private class Sink(size: Int) {

    private val buf: Array[Byte] = new Array[Byte](size)
    private var len: Int         = 0

    def writeByte(value: Int): Unit = {
      buf(len) = value.toByte
      len += 1
    }

    def writeCrlf(): Unit = {
      buf(len) = '\r'
      buf(len + 1) = '\n'
      len += 2
    }

    def writeBytes(bytes: Bytes): Unit = {
      val array = bytes.unsafeArray
      System.arraycopy(array, 0, buf, len, array.length)
      len += array.length
    }

    def writeLong(value: Long): Unit = {
      val digits = digitCount(value)
      writeDigits(buf, len, digits, value)
      len += digits
    }

    def result(): Bytes = Bytes.wrap(IArray.unsafeFromArray(buf))
  }
}

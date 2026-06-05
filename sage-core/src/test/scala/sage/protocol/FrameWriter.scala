package sage.protocol

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import sage.Bytes

/**
  * Test oracle: the parser's dual, for round-trip properties. Production code never writes frames.
  */
object FrameWriter {

  def write(frame: Frame): Bytes = {
    val out = new ByteArrayOutputStream(64)
    writeFrame(frame, out)
    Bytes.fromArray(out.toByteArray)
  }

  private def writeFrame(frame: Frame, out: ByteArrayOutputStream): Unit =
    frame match {
      case Frame.SimpleString(value)           => writeLine('+', value, out)
      case Frame.SimpleError(value)            => writeLine('-', value, out)
      case Frame.Integer(value)                => writeLine(':', value.toString, out)
      case Frame.BulkString(value)             => writeBulk('$', value, out)
      case Frame.Array(elements)               => writeAggregate('*', elements, out)
      case Frame.Null                          => writeLine('_', "", out)
      case Frame.Bool(value)                   => writeLine('#', if (value) "t" else "f", out)
      case Frame.Double(value)                 => writeLine(',', formatDouble(value), out)
      case Frame.BigNumber(value)              => writeLine('(', value.toString, out)
      case Frame.BulkError(value)              => writeBulk('!', value, out)
      case Frame.VerbatimString(format, value) =>
        out.write('=')
        writeText((value.length + 4).toString, out)
        writeCrlf(out)
        writeText(format, out)
        out.write(':')
        out.write(value.toArray)
        writeCrlf(out)
      case Frame.Map(entries)                  => writePairs('%', entries, out)
      case Frame.Set(elements)                 => writeAggregate('~', elements, out)
      case Frame.Attribute(entries)            => writePairs('|', entries, out)
      case Frame.Push(elements)                => writeAggregate('>', elements, out)
    }

  private def writeLine(kind: Char, content: String, out: ByteArrayOutputStream): Unit = {
    out.write(kind)
    writeText(content, out)
    writeCrlf(out)
  }

  private def writeBulk(kind: Char, value: Bytes, out: ByteArrayOutputStream): Unit = {
    out.write(kind)
    writeText(value.length.toString, out)
    writeCrlf(out)
    out.write(value.toArray)
    writeCrlf(out)
  }

  private def writeAggregate(kind: Char, elements: Vector[Frame], out: ByteArrayOutputStream): Unit = {
    out.write(kind)
    writeText(elements.length.toString, out)
    writeCrlf(out)
    elements.foreach(writeFrame(_, out))
  }

  private def writePairs(kind: Char, entries: Vector[(Frame, Frame)], out: ByteArrayOutputStream): Unit = {
    out.write(kind)
    writeText(entries.length.toString, out)
    writeCrlf(out)
    entries.foreach { case (key, value) =>
      writeFrame(key, out)
      writeFrame(value, out)
    }
  }

  private def writeText(content: String, out: ByteArrayOutputStream): Unit =
    out.write(content.getBytes(StandardCharsets.UTF_8))

  private def writeCrlf(out: ByteArrayOutputStream): Unit = {
    out.write('\r')
    out.write('\n')
  }

  private def formatDouble(value: scala.Double): String =
    if (value == java.lang.Double.POSITIVE_INFINITY) "inf"
    else if (value == java.lang.Double.NEGATIVE_INFINITY) "-inf"
    else if (value.isNaN) "nan"
    else value.toString
}

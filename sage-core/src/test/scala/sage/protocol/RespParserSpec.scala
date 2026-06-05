package sage.protocol

import sage.Bytes
import sage.SageException.ProtocolError

class RespParserSpec extends munit.FunSuite {

  private def parse(input: String): Vector[Frame] = {
    val parser = new RespParser
    parser.feed(Bytes.utf8(input)).fold(error => fail(s"unexpected protocol error: $error"), identity)
  }

  private def parseOne(input: String): Frame =
    parse(input) match {
      case Vector(frame) => frame
      case other         => fail(s"expected exactly one frame, got $other")
    }

  private def parseError(input: String): ProtocolError = {
    val parser = new RespParser
    parser.feed(Bytes.utf8(input)).fold(identity, frames => fail(s"expected a protocol error, got $frames"))
  }

  // golden frames, one per RESP3 type, reused by the chunk-split tests
  private val goldens: Vector[(String, Frame)] = Vector(
    "+OK\r\n"                                          -> Frame.SimpleString("OK"),
    "-ERR something went wrong\r\n"                    -> Frame.SimpleError("ERR something went wrong"),
    ":1234\r\n"                                        -> Frame.Integer(1234L),
    ":-42\r\n"                                         -> Frame.Integer(-42L),
    "$5\r\nhello\r\n"                                  -> Frame.BulkString(Bytes.utf8("hello")),
    "$0\r\n\r\n"                                       -> Frame.BulkString(Bytes.empty),
    "$4\r\na\r\nb\r\n"                                 -> Frame.BulkString(Bytes.utf8("a\r\nb")), // binary-safe: CRLF inside the payload
    "_\r\n"                                            -> Frame.Null,
    "#t\r\n"                                           -> Frame.Bool(true),
    "#f\r\n"                                           -> Frame.Bool(false),
    ",3.14\r\n"                                        -> Frame.Double(3.14),
    ",10\r\n"                                          -> Frame.Double(10.0),
    ",inf\r\n"                                         -> Frame.Double(Double.PositiveInfinity),
    ",-inf\r\n"                                        -> Frame.Double(Double.NegativeInfinity),
    "(3492890328409238509324850943850943825024385\r\n" -> Frame.BigNumber(BigInt("3492890328409238509324850943850943825024385")),
    "!21\r\nSYNTAX invalid syntax\r\n"                 -> Frame.BulkError(Bytes.utf8("SYNTAX invalid syntax")),
    "=15\r\ntxt:Some string\r\n"                       -> Frame.VerbatimString("txt", Bytes.utf8("Some string")),
    "*2\r\n:1\r\n:2\r\n"                               -> Frame.Array(Vector(Frame.Integer(1), Frame.Integer(2))),
    "*0\r\n"                                           -> Frame.Array(Vector.empty),
    "*2\r\n*1\r\n+a\r\n$1\r\nb\r\n"                    -> Frame.Array(Vector(Frame.Array(Vector(Frame.SimpleString("a"))), Frame.BulkString(Bytes.utf8("b")))),
    "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"          -> Frame.Map(
      Vector(Frame.SimpleString("first") -> Frame.Integer(1), Frame.SimpleString("second") -> Frame.Integer(2))
    ),
    "~3\r\n:1\r\n:2\r\n:3\r\n"                         -> Frame.Set(Vector(Frame.Integer(1), Frame.Integer(2), Frame.Integer(3))),
    "|1\r\n+ttl\r\n:3600\r\n"                          -> Frame.Attribute(Vector(Frame.SimpleString("ttl") -> Frame.Integer(3600))),
    ">2\r\n+message\r\n$5\r\nhello\r\n"                -> Frame.Push(Vector(Frame.SimpleString("message"), Frame.BulkString(Bytes.utf8("hello"))))
  )

  goldens.foreach { case (wire, expected) =>
    test(s"parses ${expected.getClass.getSimpleName}: ${wire.replace("\r\n", "\\r\\n")}") {
      assertEquals(parseOne(wire), expected)
    }
  }

  test("parses nan as a NaN double") {
    parseOne(",nan\r\n") match {
      case Frame.Double(value) => assert(value.isNaN)
      case other               => fail(s"expected a double, got $other")
    }
  }

  test("RESP2-compat null forms parse as Null") {
    assertEquals(parseOne("$-1\r\n"), Frame.Null)
    assertEquals(parseOne("*-1\r\n"), Frame.Null)
  }

  test("parses multiple frames from one feed") {
    assertEquals(parse("+a\r\n+b\r\n:3\r\n"), Vector(Frame.SimpleString("a"), Frame.SimpleString("b"), Frame.Integer(3)))
  }

  test("retains a trailing partial frame and completes it on the next feed") {
    val parser = new RespParser
    assertEquals(parser.feed(Bytes.utf8("+a\r\n+b")), Right(Vector(Frame.SimpleString("a"))))
    assertEquals(parser.feed(Bytes.utf8("\r\n")), Right(Vector(Frame.SimpleString("b"))))
  }

  test("incomplete bulk payload stays incomplete") {
    val parser = new RespParser
    assertEquals(parser.feed(Bytes.utf8("$11\r\nhel")), Right(Vector.empty))
    assertEquals(parser.feed(Bytes.utf8("lo worl")), Right(Vector.empty))
    assertEquals(parser.feed(Bytes.utf8("d\r\n")), Right(Vector(Frame.BulkString(Bytes.utf8("hello world")))))
  }

  test("handles frames split at every two-way boundary") {
    val (wires, expected) = goldens.unzip
    val stream            = wires.mkString.getBytes
    for (splitAt <- 0 to stream.length) {
      val parser = new RespParser
      val first  = parser.feed(Bytes.fromArray(stream.take(splitAt))).fold(error => fail(s"split $splitAt: $error"), identity)
      val second = parser.feed(Bytes.fromArray(stream.drop(splitAt))).fold(error => fail(s"split $splitAt: $error"), identity)
      assertEquals(first ++ second, expected, s"split at byte $splitAt")
    }
  }

  test("handles the frame stream fed in chunks of every size, including byte-by-byte") {
    val (wires, expected) = goldens.unzip
    val stream            = wires.mkString.getBytes
    for (chunkSize <- 1 to 7) {
      val parser = new RespParser
      val frames = stream.grouped(chunkSize).foldLeft(Vector.empty[Frame]) { (acc, chunk) =>
        acc ++ parser.feed(Bytes.fromArray(chunk)).fold(error => fail(s"chunk size $chunkSize: $error"), identity)
      }
      assertEquals(frames, expected, s"chunk size $chunkSize")
    }
  }

  test("rejects an unknown frame type byte") {
    assert(parseError("?\r\n").message.contains("unknown frame type"))
  }

  test("rejects an invalid integer") {
    assert(parseError(":abc\r\n").message.contains("invalid integer"))
  }

  test("rejects an invalid boolean") {
    assert(parseError("#x\r\n").message.contains("invalid boolean"))
  }

  test("rejects a bulk payload not followed by CRLF") {
    assert(parseError("$3\r\nabcXY").message.contains("missing CRLF"))
  }

  test("rejects a negative aggregate length") {
    assert(parseError("%-1\r\n").message.contains("invalid aggregate length"))
  }

  test("is poisoned after a protocol error") {
    val parser = new RespParser
    val error  = parser.feed(Bytes.utf8("?oops\r\n")).fold(identity, frames => fail(s"expected a protocol error, got $frames"))
    assertEquals(parser.feed(Bytes.utf8("+OK\r\n")), Left(error))
  }
}

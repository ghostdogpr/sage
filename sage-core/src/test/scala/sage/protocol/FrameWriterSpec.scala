package sage.protocol

import sage.Bytes

class FrameWriterSpec extends munit.FunSuite {

  private def written(frame: Frame): String = FrameWriter.write(frame).asUtf8String

  test("writes simple frames") {
    assertEquals(written(Frame.SimpleString("OK")), "+OK\r\n")
    assertEquals(written(Frame.SimpleError("ERR oops")), "-ERR oops\r\n")
    assertEquals(written(Frame.Integer(1234)), ":1234\r\n")
    assertEquals(written(Frame.Null), "_\r\n")
    assertEquals(written(Frame.Bool(true)), "#t\r\n")
    assertEquals(written(Frame.Bool(false)), "#f\r\n")
    assertEquals(written(Frame.BigNumber(BigInt("123456789012345678901234567890"))), "(123456789012345678901234567890\r\n")
  }

  test("writes doubles, including the special values") {
    assertEquals(written(Frame.Double(3.14)), ",3.14\r\n")
    assertEquals(written(Frame.Double(Double.PositiveInfinity)), ",inf\r\n")
    assertEquals(written(Frame.Double(Double.NegativeInfinity)), ",-inf\r\n")
    assertEquals(written(Frame.Double(Double.NaN)), ",nan\r\n")
  }

  test("writes bulk frames") {
    assertEquals(written(Frame.BulkString(Bytes.utf8("hello"))), "$5\r\nhello\r\n")
    assertEquals(written(Frame.BulkString(Bytes.empty)), "$0\r\n\r\n")
    assertEquals(written(Frame.BulkError(Bytes.utf8("SYNTAX invalid syntax"))), "!21\r\nSYNTAX invalid syntax\r\n")
    assertEquals(written(Frame.VerbatimString("txt", Bytes.utf8("Some string"))), "=15\r\ntxt:Some string\r\n")
  }

  test("writes aggregate frames") {
    assertEquals(written(Frame.Array(Vector(Frame.Integer(1), Frame.Integer(2)))), "*2\r\n:1\r\n:2\r\n")
    assertEquals(written(Frame.Map(Vector(Frame.SimpleString("first") -> Frame.Integer(1)))), "%1\r\n+first\r\n:1\r\n")
    assertEquals(written(Frame.Set(Vector(Frame.Integer(1)))), "~1\r\n:1\r\n")
    assertEquals(written(Frame.Attribute(Vector(Frame.SimpleString("ttl") -> Frame.Integer(3600)))), "|1\r\n+ttl\r\n:3600\r\n")
    assertEquals(written(Frame.Push(Vector(Frame.SimpleString("message")))), ">1\r\n+message\r\n")
  }

  test("parse(write(frame)) round-trips every frame type") {
    val frames = Vector[Frame](
      Frame.SimpleString("OK"),
      Frame.SimpleError("ERR oops"),
      Frame.Integer(-42),
      Frame.Integer(Long.MaxValue),
      Frame.Integer(Long.MinValue), // 19 digits + sign: exercises both slow paths (parse fallback and MinValue write)
      Frame.BulkString(Bytes.utf8("a\r\nb")),
      Frame.Null,
      Frame.Bool(true),
      Frame.Double(2.5),
      Frame.Double(Double.PositiveInfinity),
      Frame.BigNumber(BigInt("-9999999999999999999999")),
      Frame.BulkError(Bytes.utf8("WRONGTYPE")),
      Frame.VerbatimString("mkd", Bytes.utf8("# hi")),
      Frame.Set(Vector(Frame.Integer(1), Frame.Integer(2))),
      Frame.Attribute(Vector(Frame.SimpleString("ttl") -> Frame.Integer(3600))),
      Frame.Push(Vector(Frame.SimpleString("message"), Frame.BulkString(Bytes.utf8("hello")))),
      // deep nesting with duplicate map keys, which a native Map would collapse
      Frame.Map(
        Vector(
          Frame.SimpleString("k") -> Frame.Array(Vector(Frame.Map(Vector(Frame.Integer(1) -> Frame.Null)))),
          Frame.SimpleString("k") -> Frame.Integer(2)
        )
      )
    )
    frames.foreach { frame =>
      val parser = new RespParser
      assertEquals(parser.feed(FrameWriter.write(frame)), Right(Vector(frame)), s"round-tripping $frame")
    }
  }
}

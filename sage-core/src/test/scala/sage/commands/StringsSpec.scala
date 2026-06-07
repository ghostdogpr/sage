package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame

class StringsSpec extends munit.FunSuite {

  test("SET decodes +OK as true and null as false") {
    assertEquals(Reply.run(Strings.set("k", "v"), Frame.SimpleString("OK")), Right(true))
    assertEquals(Reply.run(Strings.set("k", "v", condition = SetCondition.IfNotExists), Frame.Null), Right(false))
  }

  test("SET rejects an unexpected frame naming expected and actual") {
    Reply.run(Strings.set("k", "v"), Frame.Integer(1)) match {
      case Left(error: DecodeError) =>
        assertEquals(error.expected, "simple string 'OK' or null")
        assertEquals(error.actual, "integer 1")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }

  test("setGet decodes the previous value and null when the key was absent") {
    assertEquals(Reply.run(Strings.setGet("k", "v"), Frame.BulkString(Bytes.utf8("old"))), Right(Some("old")))
    assertEquals(Reply.run(Strings.setGet[String, String]("k", "v"), Frame.Null), Right(None))
  }

  test("MGET decodes positionally with None for missing keys") {
    val reply = Frame.Array(Vector(Frame.BulkString(Bytes.utf8("1")), Frame.Null, Frame.BulkString(Bytes.utf8("3"))))
    assertEquals(Reply.run(Strings.mGet[String, String]("a", "b", "c"), reply), Right(Vector(Some("1"), None, Some("3"))))
  }

  test("MGET propagates an element decode failure") {
    val reply = Frame.Array(Vector(Frame.BulkString(Bytes.utf8("1")), Frame.Integer(2)))
    assert(Reply.run(Strings.mGet[String, String]("a", "b"), reply).isLeft)
  }

  test("MGET and MSET mark key positions for the slot engine") {
    assertEquals(Strings.mGet[String, String]("a", "b", "c").keyIndices, Vector(0, 1, 2))
    assertEquals(Strings.mSet(("a", "1"), ("b", "2"), ("c", "3")).keyIndices, Vector(0, 2, 4))
    assertEquals(Strings.mSetNx(("a", "1"), ("b", "2")).keyIndices, Vector(0, 2))
  }

  test("INCRBYFLOAT decodes the float bulk string reply") {
    assertEquals(Reply.run(Strings.incrByFloat("k", 0.1), Frame.BulkString(Bytes.utf8("3.0e3"))), Right(3000.0))
    Reply.run(Strings.incrByFloat("k", 0.1), Frame.BulkString(Bytes.utf8("abc"))) match {
      case Left(error: DecodeError) => assertEquals(error.actual, "bulk string 'abc'")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }

  test("GETRANGE decodes an empty bulk string for a missing key") {
    assertEquals(Reply.run(Strings.getRange[String, String]("k", 0L, 4L), Frame.BulkString(Bytes.empty)), Right(""))
  }

  test("MSETNX decodes the flag and rejects other integers") {
    assertEquals(Reply.run(Strings.mSetNx(("a", "1")), Frame.Integer(1)), Right(true))
    assertEquals(Reply.run(Strings.mSetNx(("a", "1")), Frame.Integer(0)), Right(false))
    assert(Reply.run(Strings.mSetNx(("a", "1")), Frame.Integer(2)).isLeft)
  }
}

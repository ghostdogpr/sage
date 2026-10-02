package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame
import sage.protocol.Frames.bulk

class StringsSpec extends munit.FunSuite {

  test("GET decodes a present value as Some and a missing key as None") {
    assertEquals(Reply.decode(Strings.get[String, String]("k"), bulk("v")).toEither, Right(Some("v")))
    assertEquals(Reply.decode(Strings.get[String, String]("k"), Frame.Null).toEither, Right(None))
  }

  test("SET decodes +OK as true and null as false") {
    assertEquals(Reply.decode(Strings.set("k", "v"), Frame.SimpleString("OK")).toEither, Right(true))
    assertEquals(Reply.decode(Strings.set("k", "v", condition = SetCondition.IfNotExists), Frame.Null).toEither, Right(false))
  }

  test("SET rejects an unexpected frame naming expected and actual") {
    Reply.decode(Strings.set("k", "v"), Frame.Integer(1)).toEither match {
      case Left(error: DecodeError) =>
        assertEquals(error.expected, "simple string 'OK' or null")
        assertEquals(error.actual, "integer 1")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }

  test("setGet decodes the previous value and null when the key was absent") {
    assertEquals(Reply.decode(Strings.setGet("k", "v"), bulk("old")).toEither, Right(Some("old")))
    assertEquals(Reply.decode(Strings.setGet[String, String]("k", "v"), Frame.Null).toEither, Right(None))
  }

  test("MGET decodes positionally with None for missing keys") {
    val reply = Frame.Array(Vector(bulk("1"), Frame.Null, bulk("3")))
    assertEquals(Reply.decode(Strings.mGet[String, String]("a", "b", "c"), reply).toEither, Right(Vector(Some("1"), None, Some("3"))))
  }

  test("MGET propagates an element decode failure") {
    val reply = Frame.Array(Vector(bulk("1"), Frame.Integer(2)))
    assert(Reply.decode(Strings.mGet[String, String]("a", "b"), reply).toEither.isLeft)
  }

  test("MGET and MSET mark key positions for the slot engine") {
    assertEquals(Strings.mGet[String, String]("a", "b", "c").keyIndices, Vector(0, 1, 2))
    assertEquals(Strings.mSet(("a", "1"), ("b", "2"), ("c", "3")).keyIndices, Vector(0, 2, 4))
    assertEquals(Strings.mSetNx(("a", "1"), ("b", "2")).keyIndices, Vector(0, 2))
  }

  test("INCRBYFLOAT decodes the float bulk string reply") {
    assertEquals(Reply.decode(Strings.incrByFloat("k", 0.1), bulk("3.0e3")).toEither, Right(3000.0))
    Reply.decode(Strings.incrByFloat("k", 0.1), bulk("abc")).toEither match {
      case Left(error: DecodeError) => assertEquals(error.actual, "bulk string 'abc'")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }

  test("GETRANGE decodes an empty bulk string for a missing key") {
    assertEquals(Reply.decode(Strings.getRange[String, String]("k", 0L, 4L), Frame.BulkString(Bytes.empty)).toEither, Right(""))
  }

  test("MSETNX decodes the flag and rejects other integers") {
    assertEquals(Reply.decode(Strings.mSetNx(("a", "1")), Frame.Integer(1)).toEither, Right(true))
    assertEquals(Reply.decode(Strings.mSetNx(("a", "1")), Frame.Integer(0)).toEither, Right(false))
    assert(Reply.decode(Strings.mSetNx(("a", "1")), Frame.Integer(2)).toEither.isLeft)
  }
}

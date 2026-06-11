package sage.commands

import sage.Bytes
import sage.SageException.{DecodeError, ServerError}
import sage.protocol.Frame

class FrameDecodeSpec extends munit.FunSuite {

  test("as decodes a bulk-string frame through a ValueCodec") {
    assertEquals(Frame.BulkString(Bytes.utf8("42")).as[Int], Right(42))
    assertEquals(Frame.BulkString(Bytes.utf8("hi")).as[String], Right("hi"))
    assertEquals(Frame.Integer(1).as[Int], Left(DecodeError("bulk string", "integer 1")))
  }

  test("asLong and asString read the matching scalar shapes") {
    assertEquals(Frame.Integer(7).asLong, Right(7L))
    assertEquals(Frame.SimpleString("OK").asString, Right("OK"))
    assertEquals(Frame.BulkString(Bytes.utf8("v")).asString, Right("v"))
    assert(Frame.SimpleString("x").asLong.isLeft)
  }

  test("asArray exposes array/set/push elements; asArrayOf decodes them") {
    val array = Frame.Array(Vector(Frame.BulkString(Bytes.utf8("1")), Frame.BulkString(Bytes.utf8("2"))))
    assertEquals(array.asArray.map(_.length), Right(2))
    assertEquals(array.asArrayOf[Int], Right(Vector(1, 2)))
    assertEquals(Frame.Integer(1).asArray, Left(DecodeError("array", "integer 1")))
  }
}

class ServerErrorSpec extends munit.FunSuite {

  test("of splits the leading code from the detail") {
    assertEquals(ServerError.of("WRONGTYPE Operation against a key"), ServerError("WRONGTYPE", "Operation against a key"))
    assertEquals(ServerError.of("MOVED 3999 127.0.0.1:6381"), ServerError("MOVED", "3999 127.0.0.1:6381"))
  }

  test("a single-token error has an empty detail and round-trips through the message") {
    val e = ServerError.of("NOSCRIPT")
    assertEquals(e, ServerError("NOSCRIPT", ""))
    assertEquals(e.getMessage, "NOSCRIPT")
    assertEquals(ServerError.of("WRONGTYPE bad").getMessage, "WRONGTYPE bad")
  }
}

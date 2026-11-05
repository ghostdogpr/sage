package sage.commands

import sage.SageException.DecodeError
import sage.protocol.Frame
import sage.protocol.Frames.{bulk, map}

class ConnectionSpec extends munit.FunSuite {

  test("PING decodes PONG and an echoed message") {
    assertEquals(Reply.decode(Connection.ping(), Frame.SimpleString("PONG")).toEither, Right("PONG"))
    assertEquals(Reply.decode(Connection.ping(Some("hi")), bulk("hi")).toEither, Right("hi"))
  }

  test("HELLO decodes the proto of the reply and ignores other entries") {
    val reply = map(
      "server"  -> bulk("redis"),
      "version" -> bulk("7.4.0"),
      "proto"   -> Frame.Integer(3),
      "id"      -> Frame.Integer(42),
      "mode"    -> bulk("standalone"),
      "role"    -> bulk("master"),
      "modules" -> Frame.Array(Vector.empty)
    )
    assertEquals(Reply.decode(Connection.hello(), reply).toEither, Right(3L))
  }

  test("HELLO decodes a proto other than 3 from a flat RESP2 reply") {
    val reply = Frame.Array(Vector(bulk("server"), bulk("redis"), bulk("proto"), Frame.Integer(2)))
    assertEquals(Reply.decode(Connection.hello(), reply).toEither, Right(2L))
  }

  test("HELLO reports a missing proto field") {
    Reply.decode(Connection.hello(), map("server" -> bulk("redis"))).toEither match {
      case Left(error: DecodeError) => assertEquals(error.expected, "field 'proto'")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }
}

package sage.commands

import sage.SageException.DecodeError
import sage.protocol.Frame
import sage.protocol.Frames.{bulk, map}

class ConnectionSpec extends munit.FunSuite {

  test("PING decodes PONG and an echoed message") {
    assertEquals(Reply.decode(Connection.ping(), Frame.SimpleString("PONG")).toEither, Right("PONG"))
    assertEquals(Reply.decode(Connection.ping(Some("hi")), bulk("hi")).toEither, Right("hi"))
  }

  test("HELLO accepts a proto 3 reply and ignores other entries") {
    val reply = map(
      "server"  -> bulk("redis"),
      "version" -> bulk("7.4.0"),
      "proto"   -> Frame.Integer(3),
      "id"      -> Frame.Integer(42),
      "mode"    -> bulk("standalone"),
      "role"    -> bulk("master"),
      "modules" -> Frame.Array(Vector.empty)
    )
    assertEquals(Reply.decode(Connection.hello(), reply).toEither, Right(()))
  }

  test("HELLO rejects a proto other than 3, including values beyond Int range") {
    def reply(proto: Long) =
      map("server" -> bulk("redis"), "version" -> bulk("7.4.0"), "proto" -> Frame.Integer(proto), "role" -> bulk("master"))
    Reply.decode(Connection.hello(), reply(2)).toEither match {
      case Left(error: DecodeError) =>
        assertEquals(error.expected, "proto 3")
        assertEquals(error.actual, "proto 2")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
    Reply.decode(Connection.hello(), reply(2147483648L)).toEither match {
      case Left(error: DecodeError) => assertEquals(error.actual, "proto 2147483648")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }

  test("HELLO reports a missing proto field") {
    Reply.decode(Connection.hello(), map("server" -> bulk("redis"))).toEither match {
      case Left(error: DecodeError) => assertEquals(error.expected, "field 'proto'")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }
}

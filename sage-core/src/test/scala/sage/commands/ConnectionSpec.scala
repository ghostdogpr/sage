package sage.commands

import sage.SageException.DecodeError
import sage.protocol.Frame
import sage.protocol.Frames.{bulk, map}

class ConnectionSpec extends munit.FunSuite {

  test("PING decodes PONG and an echoed message") {
    assertEquals(Reply.run(Connection.ping(), Frame.SimpleString("PONG")), Right("PONG"))
    assertEquals(Reply.run(Connection.ping(Some("hi")), bulk("hi")), Right("hi"))
  }

  test("HELLO decodes the fields it needs and ignores unknown entries") {
    val reply = map(
      "server"  -> bulk("redis"),
      "version" -> bulk("7.4.0"),
      "proto"   -> Frame.Integer(3),
      "id"      -> Frame.Integer(42),
      "mode"    -> bulk("standalone"),
      "role"    -> bulk("master"),
      "modules" -> Frame.Array(Vector.empty)
    )
    assertEquals(Reply.run(Connection.hello(), reply), Right(HelloReply("redis", "7.4.0", 3, "master")))
  }

  test("HELLO rejects a proto other than 3, including values beyond Int range") {
    def reply(proto: Long) =
      map("server" -> bulk("redis"), "version" -> bulk("7.4.0"), "proto" -> Frame.Integer(proto), "role" -> bulk("master"))
    Reply.run(Connection.hello(), reply(2)) match {
      case Left(error: DecodeError) =>
        assertEquals(error.expected, "proto 3")
        assertEquals(error.actual, "proto 2")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
    Reply.run(Connection.hello(), reply(2147483648L)) match {
      case Left(error: DecodeError) => assertEquals(error.actual, "proto 2147483648")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }

  test("HELLO reports a missing required field") {
    Reply.run(Connection.hello(), map("server" -> bulk("redis"))) match {
      case Left(error: DecodeError) => assertEquals(error.expected, "map entry 'version'")
      case other                    => fail(s"expected a DecodeError, got $other")
    }
  }
}

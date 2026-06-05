package sage.commands

import sage.{Bytes, SageException}
import sage.protocol.{Frame, RespParser}

class CommandSpec extends munit.FunSuite {

  test("GET encodes against the golden wire frame") {
    assertEquals(Strings.get[String, String]("foo").encode.asUtf8String, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
  }

  test("SET encodes against the golden wire frame") {
    assertEquals(Strings.set("foo", "bar").encode.asUtf8String, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
  }

  test("PING encodes against the golden wire frame, with and without a message") {
    assertEquals(Connection.ping().encode.asUtf8String, "*1\r\n$4\r\nPING\r\n")
    assertEquals(Connection.ping(Some("hi")).encode.asUtf8String, "*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n")
  }

  test("HELLO encodes against the golden wire frame, with and without credentials") {
    assertEquals(Connection.hello().encode.asUtf8String, "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
    assertEquals(
      Connection.hello(Some(("user", "pass"))).encode.asUtf8String,
      "*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$4\r\npass\r\n"
    )
  }

  test("multi-word command names encode one bulk string per word") {
    val command = Command[Unit]("CONFIG GET", Vector.empty, Vector(Bytes.utf8("maxmemory")), _ => Right(()))
    assertEquals(command.encode.asUtf8String, "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n")
  }

  test("a command's encoded bytes parse back as an array of bulk strings") {
    val parser   = new RespParser
    val expected =
      Frame.Array(Vector(Frame.BulkString(Bytes.utf8("SET")), Frame.BulkString(Bytes.utf8("key")), Frame.BulkString(Bytes.utf8("value"))))
    assertEquals(parser.feed(Strings.set("key", "value").encode), Right(Vector(expected)))
  }

  test("keyIndices marks the key positions in args for the slot engine") {
    val command = Strings.get[String, String]("foo")
    assertEquals(command.keyIndices, Vector(0))
    assert(command.args(command.keyIndices.head).sameBytes(Bytes.utf8("foo")))
    assertEquals(Strings.set("foo", "bar").keyIndices, Vector(0))
    assertEquals(Connection.ping().keyIndices, Vector.empty[Int])
  }

  test("GET decodes a missing key as None") {
    assertEquals(Reply.run(Strings.get[String, String]("foo"), Frame.Null), Right(None))
  }

  test("GET decodes a present value as Some") {
    assertEquals(Reply.run(Strings.get[String, String]("foo"), Frame.BulkString(Bytes.utf8("bar"))), Right(Some("bar")))
  }

  test("SET decodes +OK as unit") {
    assertEquals(Reply.run(Strings.set("foo", "bar"), Frame.SimpleString("OK")), Right(()))
  }

  test("PING decodes PONG and an echoed message") {
    assertEquals(Reply.run(Connection.ping(), Frame.SimpleString("PONG")), Right("PONG"))
    assertEquals(Reply.run(Connection.ping(Some("hi")), Frame.BulkString(Bytes.utf8("hi"))), Right("hi"))
  }

  test("a top-level error frame becomes a ServerError for any command") {
    assertEquals(
      Reply.run(Strings.get[String, String]("foo"), Frame.SimpleError("ERR oops")),
      Left(SageException.ServerError("ERR oops"))
    )
    assertEquals(
      Reply.run(Strings.set("foo", "bar"), Frame.BulkError(Bytes.utf8("WRONGTYPE bad"))),
      Left(SageException.ServerError("WRONGTYPE bad"))
    )
  }

  test("an unexpected frame shape becomes a DecodeError naming expected and actual") {
    Reply.run(Strings.set("foo", "bar"), Frame.Integer(1)) match {
      case Left(error: SageException.DecodeError) =>
        assertEquals(error.expected, "simple string 'OK'")
        assertEquals(error.actual, "integer 1")
      case other                                  => fail(s"expected a DecodeError, got $other")
    }
  }

  test("map transforms the decoded result") {
    val exists = Strings.get[String, String]("foo").map(_.isDefined)
    assertEquals(Reply.run(exists, Frame.BulkString(Bytes.utf8("bar"))), Right(true))
    assertEquals(Reply.run(exists, Frame.Null), Right(false))
  }

  test("HELLO decodes the fields it needs and ignores unknown entries") {
    val reply = Frame.Map(
      Vector(
        Frame.BulkString(Bytes.utf8("server"))  -> Frame.BulkString(Bytes.utf8("redis")),
        Frame.BulkString(Bytes.utf8("version")) -> Frame.BulkString(Bytes.utf8("7.4.0")),
        Frame.BulkString(Bytes.utf8("proto"))   -> Frame.Integer(3),
        Frame.BulkString(Bytes.utf8("id"))      -> Frame.Integer(42),
        Frame.BulkString(Bytes.utf8("mode"))    -> Frame.BulkString(Bytes.utf8("standalone")),
        Frame.BulkString(Bytes.utf8("role"))    -> Frame.BulkString(Bytes.utf8("master")),
        Frame.BulkString(Bytes.utf8("modules")) -> Frame.Array(Vector.empty)
      )
    )
    assertEquals(Reply.run(Connection.hello(), reply), Right(HelloReply("redis", "7.4.0", 3, "master")))
  }

  test("HELLO reports a missing required field") {
    val reply = Frame.Map(Vector(Frame.BulkString(Bytes.utf8("server")) -> Frame.BulkString(Bytes.utf8("redis"))))
    Reply.run(Connection.hello(), reply) match {
      case Left(error: SageException.DecodeError) => assertEquals(error.expected, "map entry 'version'")
      case other                                  => fail(s"expected a DecodeError, got $other")
    }
  }
}

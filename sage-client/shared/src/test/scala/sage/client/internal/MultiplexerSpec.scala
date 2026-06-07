package sage.client.internal

import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.SageException.{ConnectionLost, DecodeError, NotConnected, ServerError}
import sage.commands.{Command, Connection, Strings}
import sage.protocol.Frame

class MultiplexerSpec extends munit.FunSuite {

  private def make(autoWrite: Boolean = true): (Multiplexer, FakeTransport) = {
    var transport: FakeTransport = null
    val multiplexer              = new Multiplexer((onFrame, onClosed) => {
      transport = new FakeTransport(onFrame, onClosed, autoWrite = autoWrite)
      transport
    })
    (multiplexer, transport)
  }

  test("matches replies to commands in FIFO order") {
    val (multiplexer, transport)            = make()
    var first: Option[Try[String]]          = None
    var second: Option[Try[Option[String]]] = None
    multiplexer.submit(Connection.ping(), r => first = Some(r))
    multiplexer.submit(Strings.get[String, String]("key"), r => second = Some(r))
    transport.emit(Frame.SimpleString("PONG"))
    transport.emit(Frame.BulkString(Bytes.utf8("value")))
    assertEquals(first, Some(Success("PONG")))
    assertEquals(second, Some(Success(Some("value"))))
  }

  test("replies interleaved with new submissions stay matched") {
    val (multiplexer, transport)    = make()
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    var third: Option[Try[String]]  = None
    multiplexer.submit(Connection.ping(Some("a")), r => first = Some(r))
    multiplexer.submit(Connection.ping(Some("b")), r => second = Some(r))
    transport.emit(Frame.SimpleString("a"))
    multiplexer.submit(Connection.ping(Some("c")), r => third = Some(r))
    transport.emit(Frame.SimpleString("b"))
    transport.emit(Frame.SimpleString("c"))
    assertEquals(first, Some(Success("a")))
    assertEquals(second, Some(Success("b")))
    assertEquals(third, Some(Success("c")))
  }

  test("a reply arriving while a later command is unwritten matches the written one") {
    val (multiplexer, transport)    = make(autoWrite = false)
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    multiplexer.submit(Connection.ping(Some("a")), r => first = Some(r))
    multiplexer.submit(Connection.ping(Some("b")), r => second = Some(r))
    transport.writeNext()
    transport.emit(Frame.SimpleString("a"))
    assertEquals(first, Some(Success("a")))
    assertEquals(second, None)
    transport.writeNext()
    transport.emit(Frame.SimpleString("b"))
    assertEquals(second, Some(Success("b")))
  }

  test("push frames between writes do not consume pending replies") {
    val (multiplexer, transport)    = make(autoWrite = false)
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    multiplexer.submit(Connection.ping(Some("a")), r => first = Some(r))
    multiplexer.submit(Connection.ping(Some("b")), r => second = Some(r))
    transport.writeNext()
    transport.emit(Frame.Push(Vector(Frame.SimpleString("message"))))
    transport.writeNext()
    transport.emit(Frame.SimpleString("a"))
    transport.emit(Frame.SimpleString("b"))
    assertEquals(first, Some(Success("a")))
    assertEquals(second, Some(Success("b")))
  }

  test("loss mid-interleave: replied commands keep their results, written and unwritten fail apart") {
    val (multiplexer, transport)    = make(autoWrite = false)
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    var third: Option[Try[String]]  = None
    multiplexer.submit(Connection.ping(Some("a")), r => first = Some(r))
    multiplexer.submit(Connection.ping(Some("b")), r => second = Some(r))
    multiplexer.submit(Connection.ping(Some("c")), r => third = Some(r))
    transport.writeNext()
    transport.emit(Frame.SimpleString("a"))
    transport.writeNext()
    multiplexer.close()
    assertEquals(first, Some(Success("a")))
    assertEquals(second, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    assertEquals(third, Some(Failure(ConnectionLost(mayHaveExecuted = false))))
  }

  test("a server error fails only that command") {
    val (multiplexer, transport)    = make()
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    multiplexer.submit(Connection.ping(), r => first = Some(r))
    multiplexer.submit(Connection.ping(), r => second = Some(r))
    transport.emit(Frame.SimpleError("ERR boom"))
    transport.emit(Frame.SimpleString("PONG"))
    assertEquals(first, Some(Failure(ServerError("ERR boom"))))
    assertEquals(second, Some(Success("PONG")))
  }

  test("a decode mismatch fails that command with a DecodeError") {
    val (multiplexer, transport)            = make()
    var result: Option[Try[Option[String]]] = None
    multiplexer.submit(Strings.get[String, String]("key"), r => result = Some(r))
    transport.emit(Frame.Integer(42))
    assertEquals(result, Some(Failure(DecodeError("bulk string or null", "integer 42"))))
  }

  test("close fails written in-flight commands as possibly executed") {
    val (multiplexer, _)            = make()
    var result: Option[Try[String]] = None
    multiplexer.submit(Connection.ping(), r => result = Some(r))
    multiplexer.close()
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
  }

  test("connection loss fails queued-but-unwritten commands as never sent") {
    val (multiplexer, transport)    = make(autoWrite = false)
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    multiplexer.submit(Connection.ping(), r => first = Some(r))
    multiplexer.submit(Connection.ping(), r => second = Some(r))
    transport.writeNext()
    multiplexer.close()
    assertEquals(first, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    assertEquals(second, Some(Failure(ConnectionLost(mayHaveExecuted = false))))
  }

  test("commands submitted after the connection died fail fast") {
    val (multiplexer, _)            = make()
    multiplexer.close()
    var result: Option[Try[String]] = None
    multiplexer.submit(Connection.ping(), r => result = Some(r))
    assertEquals(result, Some(Failure(NotConnected())))
  }

  test("push frames do not consume pending replies") {
    val (multiplexer, transport)    = make()
    var result: Option[Try[String]] = None
    multiplexer.submit(Connection.ping(), r => result = Some(r))
    transport.emit(Frame.Push(Vector(Frame.SimpleString("message"))))
    transport.emit(Frame.SimpleString("PONG"))
    assertEquals(result, Some(Success("PONG")))
  }

  test("attribute frames do not consume pending replies") {
    val (multiplexer, transport)    = make()
    var result: Option[Try[String]] = None
    multiplexer.submit(Connection.ping(), r => result = Some(r))
    transport.emit(Frame.Attribute(Vector(Frame.SimpleString("key") -> Frame.SimpleString("value"))))
    transport.emit(Frame.SimpleString("PONG"))
    assertEquals(result, Some(Success("PONG")))
  }

  test("a throwing decoder fails that command instead of losing its callback") {
    val (multiplexer, transport)    = make()
    val boom                        = new RuntimeException("boom")
    val throwing                    = Command[String]("PING", Command.NoKeys, Vector.empty, _ => throw boom)
    var result: Option[Try[String]] = None
    multiplexer.submit(throwing, r => result = Some(r))
    transport.emit(Frame.SimpleString("PONG"))
    assertEquals(result, Some(Failure(boom)))
  }

  test("a reply with nothing pending discards the connection") {
    val (_, transport) = make()
    transport.emit(Frame.SimpleString("PONG"))
    assertEquals(transport.closeCount, 1)
  }
}

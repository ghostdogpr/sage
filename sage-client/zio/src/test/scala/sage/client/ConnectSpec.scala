package sage.client

import scala.concurrent.ExecutionContext

import kyo.compat.*

import sage.Bytes
import sage.SageException.UnsupportedServer
import sage.client.internal.{Client, FakeTransport, MultiplexedConnection}
import sage.protocol.Frame

class ConnectSpec extends munit.FunSuite {

  private given ExecutionContext = munitExecutionContext

  private val helloReply: Frame =
    Frame.Map(
      Vector(
        Frame.BulkString(Bytes.utf8("server"))  -> Frame.BulkString(Bytes.utf8("redis")),
        Frame.BulkString(Bytes.utf8("version")) -> Frame.BulkString(Bytes.utf8("8.0.0")),
        Frame.BulkString(Bytes.utf8("proto"))   -> Frame.Integer(3),
        Frame.BulkString(Bytes.utf8("role"))    -> Frame.BulkString(Bytes.utf8("master"))
      )
    )

  private val helloThenPong: Bytes => Seq[Frame] = payload =>
    if (payload.asUtf8String.contains("HELLO")) Seq(helloReply) else Seq(Frame.SimpleString("PONG"))

  private def scripted(reply: Bytes => Seq[Frame]): (MultiplexedConnection.TransportFactory, () => FakeTransport) = {
    var transport: FakeTransport                        = null
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      transport = new FakeTransport(onFrame, onClosed, reply)
      transport
    }
    (factory, () => transport)
  }

  test("connect performs the HELLO 3 handshake and yields a working client") {
    val (factory, _) = scripted(helloThenPong)
    Client.connectWith(factory).flatMap(client => client.ping()).unsafeRun.map { result =>
      assertEquals(result, "PONG")
    }
  }

  test("a server without RESP3 is rejected with UnsupportedServer and the connection is released") {
    val (factory, transport) = scripted(_ => Seq(Frame.SimpleError("ERR unknown command 'HELLO'")))
    Client.connectWith(factory).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[UnsupportedServer], s"unexpected error: $error")
      assertEquals(transport().closeCount, 1)
    }
  }

  test("a NOPROTO rejection maps to UnsupportedServer") {
    val (factory, _) = scripted(_ => Seq(Frame.SimpleError("NOPROTO unsupported protocol version")))
    Client.connectWith(factory).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[UnsupportedServer], s"unexpected error: $error")
    }
  }

  test("the ZIO artifact lowers to a native ZIO") {
    val (factory, _)                            = scripted(helloThenPong)
    val native: zio.ZIO[Any, Throwable, String] = Client.connectWith(factory).flatMap(client => client.ping()).lower
    CIO.lift(native).unsafeRun.map(result => assertEquals(result, "PONG"))
  }
}

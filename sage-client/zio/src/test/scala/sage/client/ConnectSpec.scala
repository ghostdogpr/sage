package sage.client

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

import kyo.compat.*

import sage.{Bytes, Outcome, SageEvent, SageListener}
import sage.SageException.{NotConnected, UnsupportedServer}
import sage.client.internal.{Client, Events, FakeTransport, ManualScheduler, MultiplexedConnection}
import sage.commands.{Command, Execution, Pipeline}
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
    if (payload.asUtf8String.contains("HELLO")) Seq(helloReply)
    else if (payload.asUtf8String.contains("PING")) Seq(Frame.SimpleString("PONG"))
    else Seq(Frame.SimpleString("OK")) // CLIENT TRACKING/SETINFO and SELECT bootstrap commands all answer OK

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

  test("a pipeline carrying a blocking command fails fast without reaching the socket") {
    val (factory, transport) = scripted(helloThenPong)
    val blocking             = Pipeline.sequence(Vector(Command("BLPOP", Command.NoKeys, Vector.empty, _ => Right(()), Execution.Blocking)))
    Client.connectWith(factory).flatMap(_.pipeline(blocking)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
      assertEquals(transport().written.count(_.asUtf8String.contains("BLPOP")), 0)
    }
  }

  test("a pipeline that fails fast while disconnected reports a failed completion per position") {
    val (factory, transport) = scripted(helloThenPong)
    val scheduler            = new ManualScheduler // so the post-drop reconnect stays pending and the connection stays not-live
    val completions          = new ConcurrentLinkedQueue[SageEvent.CommandCompleted]()
    val latch                = new CountDownLatch(2)
    val listener             = new SageListener {
      def onEvent(event: SageEvent): Unit = event match {
        case c: SageEvent.CommandCompleted => completions.add(c); latch.countDown()
        case _                             => ()
      }
    }
    val get                  = Command("GET", Command.NoKeys, Vector.empty, (_: Frame) => Right(0L))
    val twoGets              = Pipeline.sequence(Vector(get, get))
    Client
      .connectWith(factory, scheduler, events = Events(Vector(listener)))
      .flatMap { client =>
        transport().close() // drop the live socket; reconnect is scheduled on the manual scheduler and never runs
        client.pipeline(twoGets)
      }
      .unsafeRun
      .failed
      .map { error =>
        assert(error.isInstanceOf[NotConnected], s"unexpected error: $error")
        assert(latch.await(2, TimeUnit.SECONDS), "expected a failed completion per pipeline position")
        val seen = completions.asScala.toVector
        assertEquals(seen.map(_.name), Vector("GET", "GET"))
        assert(seen.forall(_.outcome.isInstanceOf[Outcome.Failed]), s"expected all failed, got ${seen.map(_.outcome)}")
      }
  }

  test("an empty pipeline succeeds without a round-trip") {
    val (factory, transport) = scripted(helloThenPong)
    val empty                = Pipeline.sequence(Vector.empty[Command[Long]])
    Client.connectWith(factory).flatMap(_.pipeline(empty)).unsafeRun.map { result =>
      assertEquals(result, Vector.empty[Long])
      // exclude the bootstrap commands (HELLO, CLIENT SETINFO/TRACKING); the empty pipeline itself causes no write
      assertEquals(transport().written.count(p => !p.asUtf8String.contains("HELLO") && !p.asUtf8String.contains("CLIENT")), 0)
    }
  }
}

package sage.client

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import kyo.compat.*

import sage.{Bytes, Outcome, SageEvent, SageListener}
import sage.SageException.{ConnectionFailed, NotConnected, UnsupportedServer}
import sage.client.internal.{Client, Events, FakeTransport, ManualScheduler, MultiplexedConnection}
import sage.commands.{Command, Execution, Server}
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

  test("a refused connection surfaces as a recoverable ConnectionFailed carrying the cause") {
    val deadPort = { val s = new java.net.ServerSocket(0); val p = s.getLocalPort; s.close(); p } // a port nothing listens on
    val config   = SageConfig(topology = Topology.Standalone(Endpoint("127.0.0.1", deadPort)), connectTimeout = 1.second)
    Client.connect(config).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[ConnectionFailed], s"expected ConnectionFailed, got $error")
      assert(error.getCause != null, "the original network error should be preserved as the cause")
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

  test("readFrom = Replica on a Standalone topology is rejected before connecting") {
    Client.connect(SageConfig(readFrom = ReadFrom.Replica)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
    }
  }

  test("a MasterReplica topology with no seeds is rejected") {
    Client.connect(SageConfig(topology = Topology.MasterReplica(Vector.empty))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
    }
  }

  test("a non-positive finite dedicatedPool.idleTimeout is rejected by validation, not thrown after connecting") {
    Client.connect(SageConfig(dedicatedPool = DedicatedPoolConfig(idleTimeout = Duration.Zero))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
    }
  }

  test("a non-finite, non-Inf dedicatedPool.idleTimeout (MinusInf) is rejected; only Duration.Inf disables the sweep") {
    Client.connect(SageConfig(dedicatedPool = DedicatedPoolConfig(idleTimeout = Duration.MinusInf))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
    }
  }

  test("clientCache.maxBytes <= 0 with caching enabled is rejected by validation") {
    Client.connect(SageConfig(clientCache = CacheConfig(enabled = true, maxBytes = 0L))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
    }
  }

  test("readFrom = ReplicaPreferred on a Standalone topology passes validation (degrades to the one node)") {
    // it may connect (a local server) or fail to connect (none) — either way it must not be rejected by validation
    Client
      .connect(SageConfig(readFrom = ReadFrom.ReplicaPreferred, connectTimeout = 100.millis))
      .flatMap(_.close)
      .unsafeRun
      .map(_ => assert(true))
      .recover {
        case _: IllegalArgumentException => fail("ReplicaPreferred on Standalone should pass validation")
        case _                           => assert(true) // a connect failure (no server) is fine; only validation matters here
      }
  }

  test("the ZIO artifact lowers to a native ZIO") {
    val (factory, _)                            = scripted(helloThenPong)
    val native: zio.ZIO[Any, Throwable, String] = Client.connectWith(factory).flatMap(client => client.ping()).lower
    CIO.lift(native).unsafeRun.map(result => assertEquals(result, "PONG"))
  }

  private def assertBarrierRidesMux(barrier: Command[?], barrierReply: Frame, token: String): scala.concurrent.Future[Unit] = {
    val transports                                      = new ConcurrentLinkedQueue[FakeTransport]()
    val reply: Bytes => Seq[Frame]                      = payload =>
      if (payload.asUtf8String.contains("HELLO")) Seq(helloReply)
      else if (payload.asUtf8String.contains(token)) Seq(barrierReply)
      else Seq(Frame.SimpleString("OK"))
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, reply)
      transports.add(transport)
      transport
    }
    val write                                           =
      Command("SET", Command.NoKeys, Vector(Bytes.utf8("k"), Bytes.utf8("v")), (_: Frame) => Right(()), Execution.Ordinary)
    Client
      .connectWith(factory)
      .flatMap(client => client.run(write).flatMap(_ => client.run(barrier)))
      .unsafeRun
      .map { _ =>
        assertEquals(transports.size, 1)
        val payloads   = transports.peek().written.map(_.asUtf8String).toVector
        val setIdx     = payloads.indexWhere(_.contains("SET"))
        val barrierIdx = payloads.indexWhere(_.contains(token))
        assert(setIdx >= 0 && barrierIdx >= 0, s"SET and $token must both ride the multiplexed transport, got $payloads")
        assert(setIdx < barrierIdx, s"$token must follow the SET on the same transport, got $payloads")
      }
  }

  test("WAIT rides the multiplexed connection carrying the preceding writes, never a dedicated connection") {
    assertBarrierRidesMux(Server.waitReplicas(0L, 100.millis), Frame.Integer(0L), "WAIT")
  }

  test("WAITAOF rides the multiplexed connection carrying the preceding writes, never a dedicated connection") {
    assertBarrierRidesMux(Server.waitAof(0L, 0L, 100.millis), Frame.Array(Vector(Frame.Integer(0L), Frame.Integer(0L))), "WAITAOF")
  }

  test("a pipeline carrying a blocking command fails fast without reaching the socket") {
    val (factory, transport) = scripted(helloThenPong)
    val blocking             = Vector(Command("BLPOP", Command.NoKeys, Vector.empty, _ => Right(()), Execution.Blocking))
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
    val twoGets              = Vector(get, get)
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

  test("a sub-millisecond duration is rejected before connecting rather than truncating to 0ms") {
    Client.connect(SageConfig(connectTimeout = 500.micros)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
      assert(error.getMessage.contains("at least 1ms"), s"unexpected message: ${error.getMessage}")
    }
  }

  test("an empty pipeline succeeds without a round-trip") {
    val (factory, transport) = scripted(helloThenPong)
    val empty                = Vector.empty[Command[Long]]
    Client.connectWith(factory).flatMap(_.pipeline(empty)).unsafeRun.map { result =>
      assertEquals(result, Vector.empty[Long])
      // exclude the bootstrap commands (HELLO, CLIENT SETINFO/TRACKING); the empty pipeline itself causes no write
      assertEquals(transport().written.count(p => !p.asUtf8String.contains("HELLO") && !p.asUtf8String.contains("CLIENT")), 0)
    }
  }
}

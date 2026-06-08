package sage.client.internal

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.SageException.{ConnectionLost, DecodeError, NotConnected, ServerError}
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.commands.{Command, Connection, Strings}
import sage.protocol.Frame

class MultiplexedConnectionSpec extends munit.FunSuite {

  private val fixedBackoff = BackoffConfig(initialDelay = 1.milli, maxDelay = 1.milli, multiplier = 1.0)
  private val noWatchdog   = WatchdogConfig(enabled = false)

  private def make(
    autoWrite: Boolean = true,
    respond: Bytes => Seq[Frame] = _ => Nil,
    watchdog: WatchdogConfig = noWatchdog,
    closeTimeout: FiniteDuration = Duration.Zero,
    bootstrap: Vector[Command[?]] = Vector.empty
  ): (MultiplexedConnection, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, respond, autoWrite)
      transports += transport
      transport
    }
    val connection                                      =
      MultiplexedConnection.connect(factory, scheduler, bootstrap, fixedBackoff, watchdog, 1.second, closeTimeout)
    (connection, scheduler, transports)
  }

  test("matches replies to commands in FIFO order") {
    val (connection, _, transports)         = make()
    var first: Option[Try[String]]          = None
    var second: Option[Try[Option[String]]] = None
    connection.submit(Connection.ping(), r => first = Some(r))
    connection.submit(Strings.get[String, String]("key"), r => second = Some(r))
    transports.head.emit(Frame.SimpleString("PONG"))
    transports.head.emit(Frame.BulkString(Bytes.utf8("value")))
    assertEquals(first, Some(Success("PONG")))
    assertEquals(second, Some(Success(Some("value"))))
  }

  test("replies interleaved with new submissions stay matched") {
    val (connection, _, transports) = make()
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    var third: Option[Try[String]]  = None
    connection.submit(Connection.ping(Some("a")), r => first = Some(r))
    connection.submit(Connection.ping(Some("b")), r => second = Some(r))
    transports.head.emit(Frame.SimpleString("a"))
    connection.submit(Connection.ping(Some("c")), r => third = Some(r))
    transports.head.emit(Frame.SimpleString("b"))
    transports.head.emit(Frame.SimpleString("c"))
    assertEquals(first, Some(Success("a")))
    assertEquals(second, Some(Success("b")))
    assertEquals(third, Some(Success("c")))
  }

  test("a reply arriving while a later command is unwritten matches the written one") {
    val (connection, _, transports) = make(autoWrite = false)
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    connection.submit(Connection.ping(Some("a")), r => first = Some(r))
    connection.submit(Connection.ping(Some("b")), r => second = Some(r))
    transports.head.writeNext()
    transports.head.emit(Frame.SimpleString("a"))
    assertEquals(first, Some(Success("a")))
    assertEquals(second, None)
    transports.head.writeNext()
    transports.head.emit(Frame.SimpleString("b"))
    assertEquals(second, Some(Success("b")))
  }

  test("push frames between writes do not consume pending replies") {
    val (connection, _, transports) = make(autoWrite = false)
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    connection.submit(Connection.ping(Some("a")), r => first = Some(r))
    connection.submit(Connection.ping(Some("b")), r => second = Some(r))
    transports.head.writeNext()
    transports.head.emit(Frame.Push(Vector(Frame.SimpleString("message"))))
    transports.head.writeNext()
    transports.head.emit(Frame.SimpleString("a"))
    transports.head.emit(Frame.SimpleString("b"))
    assertEquals(first, Some(Success("a")))
    assertEquals(second, Some(Success("b")))
  }

  test("loss mid-interleave: replied commands keep their results, written and unwritten fail apart") {
    val (connection, _, transports) = make(autoWrite = false)
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    var third: Option[Try[String]]  = None
    connection.submit(Connection.ping(Some("a")), r => first = Some(r))
    connection.submit(Connection.ping(Some("b")), r => second = Some(r))
    connection.submit(Connection.ping(Some("c")), r => third = Some(r))
    transports.head.writeNext()
    transports.head.emit(Frame.SimpleString("a"))
    transports.head.writeNext()
    connection.close()
    assertEquals(first, Some(Success("a")))
    assertEquals(second, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    assertEquals(third, Some(Failure(ConnectionLost(mayHaveExecuted = false))))
  }

  test("a server error fails only that command") {
    val (connection, _, transports) = make()
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => first = Some(r))
    connection.submit(Connection.ping(), r => second = Some(r))
    transports.head.emit(Frame.SimpleError("ERR boom"))
    transports.head.emit(Frame.SimpleString("PONG"))
    assertEquals(first, Some(Failure(ServerError("ERR boom"))))
    assertEquals(second, Some(Success("PONG")))
  }

  test("a decode mismatch fails that command with a DecodeError") {
    val (connection, _, transports)         = make()
    var result: Option[Try[Option[String]]] = None
    connection.submit(Strings.get[String, String]("key"), r => result = Some(r))
    transports.head.emit(Frame.Integer(42))
    assertEquals(result, Some(Failure(DecodeError("bulk string or null", "integer 42"))))
  }

  test("close fails written in-flight commands as possibly executed") {
    val (connection, _, _)          = make()
    var result: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => result = Some(r))
    connection.close()
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
  }

  test("connection loss fails queued-but-unwritten commands as never sent") {
    val (connection, _, transports) = make(autoWrite = false)
    var first: Option[Try[String]]  = None
    var second: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => first = Some(r))
    connection.submit(Connection.ping(), r => second = Some(r))
    transports.head.writeNext()
    connection.close()
    assertEquals(first, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    assertEquals(second, Some(Failure(ConnectionLost(mayHaveExecuted = false))))
  }

  test("commands submitted after the connection is closed fail fast") {
    val (connection, _, _)          = make()
    connection.close()
    var result: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => result = Some(r))
    assertEquals(result, Some(Failure(NotConnected())))
    assertEquals(connection.currentState, MultiplexedConnection.State.Closed)
  }

  test("attribute frames do not consume pending replies") {
    val (connection, _, transports) = make()
    var result: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => result = Some(r))
    transports.head.emit(Frame.Attribute(Vector(Frame.SimpleString("key") -> Frame.SimpleString("value"))))
    transports.head.emit(Frame.SimpleString("PONG"))
    assertEquals(result, Some(Success("PONG")))
  }

  test("a throwing decoder fails that command instead of losing its callback") {
    val (connection, _, transports) = make()
    val boom                        = new RuntimeException("boom")
    val throwing                    = Command[String]("PING", Command.NoKeys, Vector.empty, _ => throw boom)
    var result: Option[Try[String]] = None
    connection.submit(throwing, r => result = Some(r))
    transports.head.emit(Frame.SimpleString("PONG"))
    assertEquals(result, Some(Failure(boom)))
  }

  test("a reply with nothing pending discards the connection") {
    val (_, _, transports) = make()
    transports.head.emit(Frame.SimpleString("PONG"))
    assertEquals(transports.head.closeCount, 1)
  }

  test("a lost connection reconnects after backoff and accepts commands again") {
    val (connection, scheduler, transports) = make()
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)

    transports.head.emit(Frame.SimpleString("PONG")) // stray frame -> connection discarded
    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)

    var duringOutage: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => duringOutage = Some(r))
    assertEquals(duringOutage, Some(Failure(NotConnected())))

    scheduler.advance(1.milli)
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
    assertEquals(transports.size, 2)

    var afterReconnect: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => afterReconnect = Some(r))
    transports.last.emit(Frame.SimpleString("PONG"))
    assertEquals(afterReconnect, Some(Success("PONG")))
  }

  test("every reconnect attempt re-resolves the endpoint, honoring a repoint between attempts") {
    val seen                                            = mutable.ArrayBuffer.empty[String]
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    var endpoint                                        = "master-1"
    var healthy                                         = true
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      seen += endpoint
      val reply: Bytes => Seq[Frame] = _ => if (healthy) Seq(Frame.SimpleString("PONG")) else Seq(Frame.SimpleError("LOADING"))
      val transport                  = new FakeTransport(onFrame, onClosed, reply)
      transports += transport
      transport
    }
    // bootstrap PING stands in for HELLO: it fails while the resolved server is unhealthy, so each retry must re-resolve afresh
    val connection                                      =
      MultiplexedConnection.connect(factory, scheduler, Vector(Connection.ping()), fixedBackoff, noWatchdog, 1.second, Duration.Zero)
    assertEquals(seen.toList, List("master-1"))
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)

    healthy = false
    transports.last.emit(Frame.SimpleString("stray")) // nothing pending -> connection discarded -> reconnect loop begins

    scheduler.advance(1.milli) // attempt 0: re-resolves, still master-1, still unhealthy -> fails
    scheduler.advance(1.milli) // attempt 1: re-resolves, still master-1 -> fails
    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)

    endpoint = "master-2"      // DNS repoint to the promoted master
    healthy = true
    scheduler.advance(1.milli) // attempt 2: re-resolves and lands on master-2 -> succeeds

    assertEquals(seen.toList, List("master-1", "master-1", "master-1", "master-2"))
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
  }

  test("a READONLY reply fails the command and poisons the connection, then reconnects") {
    val (connection, scheduler, transports) = make()
    var result: Option[Try[Boolean]]        = None
    connection.submit(Strings.set("k", "v"), r => result = Some(r))
    transports.head.emit(Frame.SimpleError("READONLY You can't write against a read only replica."))

    assertEquals(result, Some(Failure(ServerError("READONLY You can't write against a read only replica."))))
    assertEquals(transports.head.closeCount, 1)
    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)

    scheduler.advance(1.milli)
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
    assertEquals(transports.size, 2)
  }

  test("close during a reconnect attempt tears down the in-flight connection rather than orphaning it") {
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      // generation 0 answers the bootstrap PING; later generations never reply, so establish() blocks until close() aborts it
      val first                      = transports.isEmpty
      val reply: Bytes => Seq[Frame] = _ => if (first) Seq(Frame.SimpleString("PONG")) else Nil
      val transport                  = new FakeTransport(onFrame, onClosed, reply)
      transports += transport
      transport
    }
    val connection                                      =
      MultiplexedConnection.connect(factory, scheduler, Vector(Connection.ping()), fixedBackoff, noWatchdog, 5.seconds, Duration.Zero)

    transports.head.emit(Frame.SimpleString("stray"))
    val reconnect = new Thread(() => scheduler.advance(1.milli)) // advance blocks inside establish() awaiting the bootstrap reply
    reconnect.start()

    val deadline = System.currentTimeMillis() + 2000
    while (transports.size < 2 && System.currentTimeMillis() < deadline) Thread.sleep(1)
    assertEquals(transports.size, 2)

    connection.close()
    reconnect.join()

    assert(transports(1).closeCount >= 1)
    assertEquals(connection.currentState, MultiplexedConnection.State.Closed)
  }

  test("close aborts a reconnect still blocked in the connect (start) phase") {
    // a transport whose start() blocks like socket.connect until close() aborts it — the reconnect generation uses it
    final class ConnectingTransport(onClosed: () => Unit) extends Transport {
      private val gate                     = new java.util.concurrent.CountDownLatch(1)
      @volatile var wasClosed: Boolean     = false
      def start(): Unit                    = { gate.await(); throw new java.io.IOException("connect aborted") }
      def send(item: Transport.Item): Unit = item.dropped()
      def close(): Unit                    = { wasClosed = true; gate.countDown(); onClosed() }
    }

    val connecting                                      = new java.util.concurrent.atomic.AtomicReference[ConnectingTransport]()
    var head: FakeTransport                             = null
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) =>
      if (head == null) {
        head = new FakeTransport(onFrame, onClosed, _ => Seq(Frame.SimpleString("PONG")))
        head
      } else {
        val transport = new ConnectingTransport(onClosed)
        connecting.set(transport)
        transport
      }
    val connection                                      =
      MultiplexedConnection.connect(factory, scheduler, Vector(Connection.ping()), fixedBackoff, noWatchdog, 5.seconds, Duration.Zero)

    head.emit(Frame.SimpleString("stray"))
    val reconnect = new Thread(() => scheduler.advance(1.milli)) // blocks inside ConnectingTransport.start()
    reconnect.start()

    val deadline = System.currentTimeMillis() + 2000
    while (connecting.get() == null && System.currentTimeMillis() < deadline) Thread.sleep(1)
    assert(connecting.get() != null)

    connection.close()
    reconnect.join()

    assert(connecting.get().wasClosed)
    assertEquals(connection.currentState, MultiplexedConnection.State.Closed)
  }

  test("the watchdog reconnects a silently dead connection within the configured interval") {
    val watchdog                    = WatchdogConfig(pingInterval = 10.millis, pingTimeout = 5.millis, enabled = true)
    val (connection, scheduler, ts) = make(watchdog = watchdog) // respond = Nil: the injected PING is never answered
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)

    scheduler.advance(10.millis) // idle interval elapses -> PING injected
    assertEquals(ts.head.written.count(_.asUtf8String.contains("PING")), 1)
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)

    scheduler.advance(10.millis) // PING unanswered past pingTimeout -> dead -> reconnect
    assertEquals(ts.head.closeCount, 1)
    assert(connection.currentState != MultiplexedConnection.State.Live)
  }

  test("the watchdog leaves a connection that answers PING alive") {
    val watchdog                     = WatchdogConfig(pingInterval = 10.millis, pingTimeout = 5.millis, enabled = true)
    val respond: Bytes => Seq[Frame] = p => if (p.asUtf8String.contains("PING")) Seq(Frame.SimpleString("PONG")) else Nil
    val (connection, scheduler, ts)  = make(respond = respond, watchdog = watchdog)
    scheduler.advance(100.millis)
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
    assertEquals(ts.size, 1)
  }

  test("graceful drain lets an in-flight reply complete before close finishes") {
    val (connection, _, transports) = make(autoWrite = false, closeTimeout = 2.seconds)
    var result: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => result = Some(r))
    transports.head.writeNext() // written, awaiting a reply

    val emitter = new Thread(() => {
      Thread.sleep(20)
      transports.head.emit(Frame.SimpleString("PONG"))
    })
    emitter.start()
    connection.close() // blocks draining until the reply lands or closeTimeout
    emitter.join()

    assertEquals(result, Some(Success("PONG")))
    assertEquals(connection.currentState, MultiplexedConnection.State.Closed)
  }

  test("graceful drain force-closes a straggler at the timeout, failing it as possibly executed") {
    val (connection, _, transports) = make(autoWrite = false, closeTimeout = 20.millis)
    var result: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => result = Some(r))
    transports.head.writeNext() // written, no reply will arrive
    connection.close()
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    assertEquals(connection.currentState, MultiplexedConnection.State.Closed)
  }
}

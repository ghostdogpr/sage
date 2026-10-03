package sage.client.internal

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

import Replies.bulk

import sage.Bytes
import sage.SageException.{ConnectionLost, DecodeError, NotConnected, ServerError}
import sage.client.{BackoffConfig, CacheConfig, SageConfig, WatchdogConfig}
import sage.commands.{Command, Connection, Strings}
import sage.protocol.Frame

class MultiplexedConnectionSpec extends munit.FunSuite {

  private val fixedBackoff = BackoffConfig(initialDelay = 1.milli, maxDelay = 1.milli, multiplier = 1.0)
  private val noWatchdog   = WatchdogConfig(enabled = false)

  private val noCache = CacheConfig(enabled = false)

  private val cachingConfig = SageConfig(
    reconnect = fixedBackoff,
    watchdog = noWatchdog,
    connectTimeout = 1.second,
    closeTimeout = Duration.Zero,
    clientCache = CacheConfig(enabled = true, maxBytes = 1L << 20)
  )

  private def make(
    autoWrite: Boolean = true,
    respond: Bytes => Seq[Frame] = _ => Nil,
    watchdog: WatchdogConfig = noWatchdog,
    closeTimeout: FiniteDuration = Duration.Zero
  ): (MultiplexedConnection, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, Replies.withSetup(respond))
      transports += transport
      transport
    }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        SageConfig(clientCache = noCache, reconnect = fixedBackoff, watchdog = watchdog, connectTimeout = 1.second, closeTimeout = closeTimeout),
        MultiplexedConnection.NodeRole.Master
      ).start()
    // the setup is written before the test takes control of writes
    transports.foreach(_.autoWrite = autoWrite)
    (connection, scheduler, transports)
  }

  test("an interrupted handshake closes the connection it opened") {
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    // the reply to HELLO never comes, and the waiting thread is interrupted instead
    val respond: Bytes => Seq[Frame]                    = payload => {
      if (payload.asUtf8String.contains("HELLO")) Thread.currentThread().interrupt()
      Nil
    }
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, respond)
      transports += transport
      transport
    }
    val connection                                      = new MultiplexedConnection(
      factory,
      new ManualScheduler,
      SageConfig(clientCache = noCache, reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1.second, closeTimeout = Duration.Zero),
      MultiplexedConnection.NodeRole.Master
    )
    @volatile var failure: Throwable                    = null
    Thread
      .ofVirtual()
      .start { () =>
        try connection.start(): Unit
        catch { case e: Throwable => failure = e }
      }
      .join()
    assert(failure.isInstanceOf[InterruptedException], String.valueOf(failure))
    assertEquals(transports.map(_.closeCount).toVector, Vector(1))
  }

  test("matches replies to commands in FIFO order") {
    val (connection, _, transports)         = make()
    var first: Option[Try[String]]          = None
    var second: Option[Try[Option[String]]] = None
    connection.submit(Connection.ping(), r => first = Some(r))
    connection.submit(Strings.get[String, String]("key"), r => second = Some(r))
    transports.head.emit(Frame.SimpleString("PONG"))
    transports.head.emit(bulk("value"))
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
    assertEquals(first, Some(Failure(ServerError("ERR", "boom"))))
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

  test("a batch reaches the socket as a single write and matches replies per position") {
    val (connection, _, transports) = make()
    val commands                    = Vector(Connection.ping(Some("a")), Connection.ping(Some("b")), Connection.ping(Some("c")))
    val results                     = new Array[Try[Any]](3)
    val callbacks                   = Vector.tabulate(3)(i => (r: Try[Any]) => results(i) = r)
    val submitted                   = connection.submitAll(commands, callbacks)
    assert(submitted)
    assertEquals(transports.head.sent.length, 1) // one round-trip: the whole pipeline is one write, not three
    transports.head.emit(Frame.SimpleString("a"))
    transports.head.emit(Frame.SimpleString("b"))
    transports.head.emit(Frame.SimpleString("c"))
    assertEquals(results.toVector, Vector[Try[Any]](Success("a"), Success("b"), Success("c")))
  }

  test("a batch's single write concatenates every command's encoding") {
    val (connection, _, transports) = make(autoWrite = false)
    val commands                    = Vector(Connection.ping(Some("a")), Connection.ping(Some("b")))
    connection.submitAll(commands, Vector.fill(2)((_: Try[Any]) => ()))
    transports.head.writeNext()
    val expected                    = Bytes.concat(commands.map(_.encode))
    assertEquals(transports.head.sent.length, 1)
    assert(transports.head.sent.head.sameBytes(expected))
  }

  test("a batch returns false when not connected, submitting nothing") {
    val (connection, _, _) = make()
    connection.close()
    val callbacks          = Vector((_: Try[Any]) => ())
    assertEquals(connection.submitAll(Vector(Connection.ping()), callbacks), false)
  }

  test("a written batch losing the connection fails every in-flight position as possibly executed") {
    val (connection, _, transports) = make(autoWrite = false)
    val commands                    = Vector(Connection.ping(Some("a")), Connection.ping(Some("b")), Connection.ping(Some("c")))
    val results                     = new Array[Try[Any]](3)
    val callbacks                   = Vector.tabulate(3)(i => (r: Try[Any]) => results(i) = r)
    connection.submitAll(commands, callbacks)
    transports.head.writeNext() // the whole batch is one write
    transports.head.emit(Frame.SimpleString("a"))
    connection.close()
    assertEquals(results(0), Success("a"))
    assertEquals(results(1), Failure(ConnectionLost(mayHaveExecuted = true)))
    assertEquals(results(2), Failure(ConnectionLost(mayHaveExecuted = true)))
  }

  test("a batch dropped before any write fails every position as never sent") {
    val (connection, _, _) = make(autoWrite = false)
    val results            = new Array[Try[Any]](3)
    val callbacks          = Vector.tabulate(3)(i => (r: Try[Any]) => results(i) = r)
    connection.submitAll(Vector.fill(3)(Connection.ping()), callbacks)
    connection.close()
    assertEquals(results.toVector, Vector.fill[Try[Any]](3)(Failure(ConnectionLost(mayHaveExecuted = false))))
  }

  test("out-of-band push frames do not consume pending replies") {
    val (connection, _, transports) = make()
    var result: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => result = Some(r))
    transports.head.emit(Frame.Push(Vector(Frame.SimpleString("message"), Frame.SimpleString("hi"))))
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
    result match {
      case Some(Failure(e: DecodeError)) => assertEquals(e.getCause, boom)
      case other                         => fail(s"expected a DecodeError wrapping the thrown exception, got $other")
    }
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

  test("the initial connection at clock 0 counts as stable: a loss after the ceiling resets to the initial delay") {
    val backoff                                         = BackoffConfig(initialDelay = 10.millis, maxDelay = 10.seconds, multiplier = 2.0)
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, Replies.withSetup(_ => Nil))
      transports += transport
      transport
    }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        SageConfig(clientCache = noCache, reconnect = backoff, watchdog = noWatchdog, connectTimeout = 1.second, closeTimeout = Duration.Zero),
        MultiplexedConnection.NodeRole.Master
      ).start()
    import MultiplexedConnection.State

    scheduler.advance(11.seconds)
    transports.last.emit(Frame.SimpleString("stray"))
    scheduler.advance(9.millis)
    assertEquals(connection.currentState, State.Reconnecting, "still backing off before the reset 10ms initial delay")
    scheduler.advance(1.milli)
    assertEquals(connection.currentState, State.Live, "a stable loss must retry at attempt 0, not advance to attempt 1")
  }

  test("a flapping connection backs off increasingly, and a stable connection resets the backoff") {
    val backoff                                         = BackoffConfig(initialDelay = 10.millis, maxDelay = 10.seconds, multiplier = 2.0)
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, Replies.withSetup(_ => Nil))
      transports += transport
      transport
    }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        SageConfig(clientCache = noCache, reconnect = backoff, watchdog = noWatchdog, connectTimeout = 1.second, closeTimeout = Duration.Zero),
        MultiplexedConnection.NodeRole.Master
      ).start()
    import MultiplexedConnection.State

    // when a Live connection drops before the stable interval, keep the attempt count and increase the next backoff delay
    assertEquals(connection.currentState, State.Live)
    transports.last.emit(Frame.SimpleString("stray")) // flap 1 -> attempt 0 -> 10ms
    assertEquals(connection.currentState, State.Reconnecting)
    scheduler.advance(9.millis)
    assertEquals(connection.currentState, State.Reconnecting, "still backing off before the 10ms attempt-0 delay")
    scheduler.advance(1.milli)
    assertEquals(connection.currentState, State.Live)

    transports.last.emit(Frame.SimpleString("stray")) // the second short-lived connection uses attempt 1 and a 20 ms delay
    scheduler.advance(19.millis)
    assertEquals(connection.currentState, State.Reconnecting, "the backoff doubled to 20ms rather than resetting to 10ms")
    scheduler.advance(1.milli)
    assertEquals(connection.currentState, State.Live)

    // after the connection remains Live longer than the maximum backoff, the next loss uses the initial delay again
    scheduler.advance(11.seconds)
    assertEquals(connection.currentState, State.Live)
    transports.last.emit(Frame.SimpleString("stray")) // stable -> attempt 0 -> 10ms
    scheduler.advance(9.millis)
    assertEquals(connection.currentState, State.Reconnecting, "still backing off before the reset 10ms initial delay")
    scheduler.advance(1.milli)
    assertEquals(connection.currentState, State.Live)
  }

  test("a socket dying in the establish->Live window is not published Live, and the reconnect loop recovers") {
    final class DyingAfterBootstrap(onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {
      def start(): Unit                    = ()
      // answers the setup, then drops the connection right after the last setup command
      def send(item: Transport.Item): Unit = {
        item.writeAttempted()
        Replies.withSetup(_ => Nil)(item.payload).foreach(onFrame)
        if (item.payload.asUtf8String.contains("LIB-VER")) onClosed()
      }
      def close(): Unit                    = ()
    }
    val healthy = mutable.ArrayBuffer.empty[FakeTransport]
    var first                                           = true
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) =>
      if (first) {
        first = false
        new DyingAfterBootstrap(onFrame, onClosed)
      } else {
        val t = new FakeTransport(onFrame, onClosed, Replies.withSetup(_ => Seq(Frame.SimpleString("PONG"))))
        healthy += t
        t
      }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        SageConfig(clientCache = noCache, reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1.second, closeTimeout = Duration.Zero),
        MultiplexedConnection.NodeRole.Master
      ).start()

    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)

    scheduler.advance(1.milli)
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
    assertEquals(healthy.size, 1)

    var afterRecovery: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => afterRecovery = Some(r))
    healthy.head.emit(Frame.SimpleString("PONG"))
    assertEquals(afterRecovery, Some(Success("PONG")))
  }

  test("losing liveness retires the pool's dedicated connections, so one opened before a reconnect is not reused") {
    val (connection, scheduler, transports) = make()
    val dedicated                           = connection.pool.acquireForTransaction()
    connection.pool.releaseTransaction(dedicated, reusable = true)
    assertEquals(transports.size, 2)

    transports.head.emit(Frame.SimpleString("PONG")) // stray frame -> discarded -> Reconnecting
    assert(!connection.isLive)
    scheduler.advance(1.milli)                       // reconnects -> Live
    assert(connection.isLive)

    val fresh = connection.pool.acquireForTransaction()
    assert(fresh ne dedicated)
    assertEquals(transports.size, 4)
    connection.pool.releaseTransaction(fresh, reusable = true)
  }

  test("every reconnect attempt re-resolves the endpoint, honoring a repoint between attempts") {
    val seen                                            = mutable.ArrayBuffer.empty[String]
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    var endpoint                                        = "master-1"
    var healthy                                         = true
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      seen += endpoint
      val reply: Bytes => Seq[Frame] = payload => if (healthy) Replies.withSetup(_ => Nil)(payload) else Seq(Frame.SimpleError("LOADING"))
      val transport                  = new FakeTransport(onFrame, onClosed, reply)
      transports += transport
      transport
    }
    // HELLO fails while the current endpoint is unhealthy, which makes every retry resolve the endpoint again.
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        SageConfig(clientCache = noCache, reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1.second, closeTimeout = Duration.Zero),
        MultiplexedConnection.NodeRole.Master
      ).start()
    assertEquals(seen.toList, List("master-1"))
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)

    healthy = false
    transports.last.emit(Frame.SimpleString("stray")) // nothing pending -> connection discarded -> reconnect loop begins

    scheduler.advance(1.milli) // attempt 0: re-resolves, still master-1, still unhealthy -> fails
    scheduler.advance(1.milli) // attempt 1: re-resolves, still master-1 -> fails
    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)

    endpoint = "master-2"      // DNS repoint to the promoted master
    healthy = true
    scheduler.advance(1.milli) // attempt 2: re-resolves and connects to master-2 -> succeeds

    assertEquals(seen.toList, List("master-1", "master-1", "master-1", "master-2"))
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
  }

  test("a READONLY reply fails the command and poisons the connection, then reconnects") {
    val (connection, scheduler, transports) = make()
    var result: Option[Try[Boolean]]        = None
    connection.submit(Strings.set("k", "v"), r => result = Some(r))
    transports.head.emit(Frame.SimpleError("READONLY You can't write against a read only replica."))

    assertEquals(result, Some(Failure(ServerError("READONLY", "You can't write against a read only replica."))))
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
      // generation 0 answers the setup; later generations never reply, so establish() blocks until close() aborts it
      val first                      = transports.isEmpty
      val reply: Bytes => Seq[Frame] = if (first) Replies.withSetup(_ => Nil) else _ => Nil
      val transport                  = new FakeTransport(onFrame, onClosed, reply)
      transports += transport
      transport
    }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        SageConfig(clientCache = noCache, reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 5.seconds, closeTimeout = Duration.Zero),
        MultiplexedConnection.NodeRole.Master
      ).start()

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
    val connecting                                      = new java.util.concurrent.atomic.AtomicReference[ConnectingTransport]()
    var head: FakeTransport                             = null
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) =>
      if (head == null) {
        head = new FakeTransport(onFrame, onClosed, Replies.withSetup(_ => Seq(Frame.SimpleString("PONG"))))
        head
      } else {
        val transport = new ConnectingTransport(onClosed)
        connecting.set(transport)
        transport
      }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        SageConfig(clientCache = noCache, reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 5.seconds, closeTimeout = Duration.Zero),
        MultiplexedConnection.NodeRole.Master
      ).start()

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

  test("the watchdog starts even when the initial connection dies in the establish window") {
    val watchdog                                        = WatchdogConfig(pingInterval = 10.millis, pingTimeout = 5.millis, enabled = true)
    val scheduler                                       = new ManualScheduler
    val live                                            = mutable.ArrayBuffer.empty[FakeTransport]
    var first                                           = true
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) =>
      if (first) {
        first = false
        // answers the setup, then drops the connection right after the last setup command
        new Transport {
          def start(): Unit                    = ()
          def send(item: Transport.Item): Unit = {
            item.writeAttempted()
            Replies.withSetup(_ => Nil)(item.payload).foreach(onFrame)
            if (item.payload.asUtf8String.contains("LIB-VER")) onClosed()
          }
          def close(): Unit                    = ()
        }
      } else {
        val t = new FakeTransport(onFrame, onClosed, Replies.withSetup(_ => Nil))
        live += t
        t
      }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        SageConfig(clientCache = noCache, reconnect = fixedBackoff, watchdog = watchdog, connectTimeout = 1.second, closeTimeout = Duration.Zero),
        MultiplexedConnection.NodeRole.Master
      ).start()

    scheduler.advance(1.milli)
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
    assertEquals(live.size, 1)

    scheduler.advance(10.millis)
    assertEquals(live.head.written.count(_.asUtf8String.contains("PING")), 1)
  }

  test("a close interrupted while draining still closes the socket, returns normally and keeps the interrupt") {
    val (connection, _, transports) = make(autoWrite = false, closeTimeout = 2.seconds)
    connection.submit(Connection.ping(), _ => ())
    @volatile var interruptedAfter  = false
    Thread
      .ofVirtual()
      .start { () =>
        Thread.currentThread().interrupt()
        connection.close()
        interruptedAfter = Thread.currentThread().isInterrupted
      }
      .join()
    assertEquals(transports.head.closeCount, 1)
    assert(interruptedAfter)
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
    connection.close() // waits for the pending reply until it arrives or closeTimeout expires
    emitter.join()

    assertEquals(result, Some(Success("PONG")))
    assertEquals(connection.currentState, MultiplexedConnection.State.Closed)
  }

  test("graceful drain waits for a command accepted but still queued unwritten, instead of dropping it") {
    val (connection, _, transports) = make(autoWrite = false, closeTimeout = 2.seconds)
    var result: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => result = Some(r))

    val drainer = new Thread(() => {
      Thread.sleep(20)
      try {
        transports.head.writeNext()
        transports.head.emit(Frame.SimpleString("PONG"))
      } catch { case _: Throwable => () }
    })
    drainer.start()
    connection.close()
    drainer.join()

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

  private def cachedConnection(
    events: Events = Events.disabled
  ): (MultiplexedConnection, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      // answer the CLIENT TRACKING setup write, so every cached connection's first write is that command
      val transport =
        new FakeTransport(onFrame, onClosed, Replies.withSetup(p => if (p.asUtf8String.contains("TRACKING")) Seq(Frame.SimpleString("OK")) else Nil))
      transports += transport
      transport
    }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        scheduler,
        cachingConfig,
        MultiplexedConnection.NodeRole.Master,
        events = events
      ).start()
    (connection, scheduler, transports)
  }

  private def invalidationOf(redisKey: String): Frame =
    Frame.Push(Vector(bulk("invalidate"), Frame.Array(Vector(bulk(redisKey)))))

  test("a cached read fetches once, then serves repeats locally until an invalidation push evicts it") {
    val (connection, _, transports) = cachedConnection()
    val get                         = Strings.get[String, String]("foo")

    var first: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => first = Some(r), Events.untraced)
    assertEquals(transports.head.sent.length, 2)   // CLIENT TRACKING, then one batch: [CLIENT CACHING YES, GET foo]
    transports.head.emit(Frame.SimpleString("OK")) // CLIENT CACHING YES reply, discarded
    transports.head.emit(bulk("bar"))
    assertEquals(first, Some(Success(Some("bar"))))

    var second: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => second = Some(r), Events.untraced)
    assertEquals(second, Some(Success(Some("bar")))) // served locally
    assertEquals(transports.head.sent.length, 2)     // no new round-trip

    transports.head.emit(invalidationOf("foo"))
    var third: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => third = Some(r), Events.untraced)
    assertEquals(transports.head.sent.length, 3) // evicted -> refetch
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(bulk("baz"))
    assertEquals(third, Some(Success(Some("baz"))))
  }

  test("a null-payload invalidation push flushes the whole cache, and reads refetch on the same connection") {
    val (connection, _, transports) = cachedConnection()
    val get                         = Strings.get[String, String]("foo")

    connection.cachedSubmit(get, 60000L, _ => (), Events.untraced)
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(bulk("bar"))
    assertEquals(transports.head.sent.length, 2)

    transports.head.emit(Frame.Push(Vector(bulk("invalidate"), Frame.Null))) // FLUSHALL/tracking-drop form

    var afterFlush: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => afterFlush = Some(r), Events.untraced)
    assertEquals(transports.head.sent.length, 3) // flushed -> refetch
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(bulk("baz"))
    assertEquals(afterFlush, Some(Success(Some("baz"))))
  }

  test("TTL expiry evicts a cached read independently of invalidations") {
    val (connection, scheduler, transports) = cachedConnection()
    val get                                 = Strings.get[String, String]("foo")

    connection.cachedSubmit(get, 1000L, _ => (), Events.untraced)
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(bulk("bar"))
    assertEquals(transports.head.sent.length, 2)

    scheduler.advance(1001.millis)               // past the TTL
    connection.cachedSubmit(get, 1000L, _ => (), Events.untraced)
    assertEquals(transports.head.sent.length, 3) // expired -> refetch
  }

  test("a reconnect flushes the cache: tracking state is connection-bound") {
    val (connection, scheduler, transports) = cachedConnection()
    val get                                 = Strings.get[String, String]("foo")

    connection.cachedSubmit(get, 60000L, _ => (), Events.untraced)
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(bulk("bar"))

    transports.head.emit(Frame.SimpleString("stray")) // nothing pending -> discard -> reconnect
    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)
    scheduler.advance(1.milli)
    assertEquals(transports.size, 2)

    var afterReconnect: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => afterReconnect = Some(r), Events.untraced)
    assertEquals(transports(1).sent.length, 2) // fresh generation, empty cache -> refetch
    transports(1).emit(Frame.SimpleString("OK"))
    transports(1).emit(bulk("baz"))
    assertEquals(afterReconnect, Some(Success(Some("baz"))))
  }

  test("a cache miss is traced like an ordinary command; a local hit is not") {
    val tracer                      = new RecordingTracer
    val events                      = Events(Vector.empty, Some(tracer))
    val (connection, _, transports) = cachedConnection(events)
    val get                         = Strings.get[String, String]("foo")

    tracedRead(connection, events, get)(_ => ()) // miss -> fetch
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(bulk("bar"))
    assertEquals(tracer.log.toVector, Vector("start:GET", "settled:Succeeded"))

    tracedRead(connection, events, get)(_ => ())                                // served locally
    assertEquals(tracer.log.toVector, Vector("start:GET", "settled:Succeeded")) // unchanged: no span for a hit
  }

  test("a cached read that fails fast (not connected) settles a Failed span, like an ordinary command") {
    val tracer                              = new RecordingTracer
    val events                              = Events(Vector.empty, Some(tracer))
    val (connection, _, _)                  = cachedConnection(events)
    connection.close()
    var result: Option[Try[Option[String]]] = None
    tracedRead(connection, events, Strings.get[String, String]("foo"))(r => result = Some(r))
    assert(result.exists(_.isFailure))
    assertEquals(tracer.log.head, "start:GET")
    assert(tracer.log.last.startsWith("settled:Failed"))
  }

  test("a server that rejects CLIENT TRACKING degrades cached reads to uncached rather than failing (ADR-0045)") {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val respond: Bytes => Seq[Frame]                    = payload => {
      val text = payload.asUtf8String
      if (text.contains("TRACKING")) Seq(Frame.SimpleError("ERR unknown subcommand TRACKING"))
      else if (text.contains("GET")) Seq(bulk("bar"))
      else Nil
    }
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, Replies.withSetup(respond))
      transports += transport
      transport
    }
    val connection                                      = new MultiplexedConnection(
      factory,
      scheduler,
      cachingConfig,
      MultiplexedConnection.NodeRole.Master
    ).start()
    val get                                             = Strings.get[String, String]("foo")

    var first: Option[Try[Option[String]]]  = None
    connection.cachedSubmit(get, 60000L, r => first = Some(r), Events.untraced)
    var second: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => second = Some(r), Events.untraced)

    assertEquals(first, Some(Success(Some("bar"))))
    assertEquals(second, Some(Success(Some("bar"))))
    val writes = transports.head.written.map(_.asUtf8String)
    assert(!writes.exists(_.contains("CACHING")), "no CLIENT CACHING YES when tracking is unavailable")
    assertEquals(writes.count(_.contains("GET")), 2, "each cached read re-contacts the server: nothing is cached without tracking")
  }

  // traces the read once per call, as the client runtimes do
  private def tracedRead[A](connection: MultiplexedConnection, events: Events, command: Command[A])(callback: Try[A] => Unit): Unit =
    connection.cachedSubmit(command, 60000L, callback, Events.fetchTracking(events))
}

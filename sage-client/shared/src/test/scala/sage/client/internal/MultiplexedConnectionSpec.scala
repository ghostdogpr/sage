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
    assertEquals(transports.head.written.length, 1) // one round-trip: the whole pipeline is one write, not three
    transports.head.emit(Frame.SimpleString("a"))
    transports.head.emit(Frame.SimpleString("b"))
    transports.head.emit(Frame.SimpleString("c"))
    assertEquals(results.toVector, Vector[Try[Any]](Success("a"), Success("b"), Success("c")))
  }

  test("a batch's single write concatenates every command's encoding") {
    val (connection, _, transports) = make(autoWrite = false)
    val commands                    = Vector(Connection.ping(Some("a")), Connection.ping(Some("b")))
    val _                           = connection.submitAll(commands, Vector.fill(2)((_: Try[Any]) => ()))
    transports.head.writeNext()
    val expected                    = Bytes.concat(commands.map(_.encode))
    assertEquals(transports.head.written.length, 1)
    assert(transports.head.written.head.sameBytes(expected))
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
    val _                           = connection.submitAll(commands, callbacks)
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
    val _                  = connection.submitAll(Vector.fill(3)(Connection.ping()), callbacks)
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

  test("a socket dying in the establish->Live window is not published Live, and the reconnect loop recovers") {
    final class DyingAfterBootstrap(onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {
      def start(): Unit                    = ()
      def send(item: Transport.Item): Unit = {
        item.writeAttempted()
        onFrame(Frame.SimpleString("PONG"))
        onClosed()
      }
      def close(): Unit                    = ()
    }
    val healthy = mutable.ArrayBuffer.empty[FakeTransport]
    var first                                           = true
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) =>
      if (first) { first = false; new DyingAfterBootstrap(onFrame, onClosed) }
      else {
        val t = new FakeTransport(onFrame, onClosed, _ => Seq(Frame.SimpleString("PONG")))
        healthy += t
        t
      }
    val connection                                      =
      MultiplexedConnection.connect(factory, scheduler, Vector(Connection.ping()), fixedBackoff, noWatchdog, 1.second, Duration.Zero)

    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)

    scheduler.advance(1.milli)
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
    assertEquals(healthy.size, 1)

    var afterRecovery: Option[Try[String]] = None
    connection.submit(Connection.ping(), r => afterRecovery = Some(r))
    healthy.head.emit(Frame.SimpleString("PONG"))
    assertEquals(afterRecovery, Some(Success("PONG")))
  }

  test("isCurrent gates on liveness: a stamp is current only while Live, never during a reconnect window at the same generation") {
    val (connection, scheduler, transports) = make()
    val g                                   = connection.liveGeneration().getOrElse(fail("expected a live generation"))
    assert(connection.isCurrent(g))

    transports.head.emit(Frame.SimpleString("PONG")) // stray frame -> discarded -> Reconnecting, generation not yet bumped
    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)
    assertEquals(connection.liveGeneration(), None)
    assert(!connection.isCurrent(g), "the stamp must not read as current during a reconnect window, even at the same generation")

    scheduler.advance(1.milli) // reconnects -> Live, generation bumps
    assertEquals(connection.currentState, MultiplexedConnection.State.Live)
    assert(!connection.isCurrent(g), "the old stamp stays stale once the generation bumps")
    assert(connection.isCurrent(connection.liveGeneration().getOrElse(fail("expected a live generation"))))
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

  private def cachedConnection(): (MultiplexedConnection, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed)
      transports += transport
      transport
    }
    val connection                                      =
      MultiplexedConnection.connect(factory, scheduler, Vector.empty, fixedBackoff, noWatchdog, 1.second, Duration.Zero, cacheMaxBytes = 1L << 20)
    (connection, scheduler, transports)
  }

  private def invalidationOf(redisKey: String): Frame =
    Frame.Push(Vector(Frame.BulkString(Bytes.utf8("invalidate")), Frame.Array(Vector(Frame.BulkString(Bytes.utf8(redisKey))))))

  test("a cached read fetches once, then serves repeats locally until an invalidation push evicts it") {
    val (connection, _, transports) = cachedConnection()
    val get                         = Strings.get[String, String]("foo")

    var first: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => first = Some(r))
    assertEquals(transports.head.written.length, 1) // one batch: [CLIENT CACHING YES, GET foo]
    transports.head.emit(Frame.SimpleString("OK"))  // CLIENT CACHING YES reply, discarded
    transports.head.emit(Frame.BulkString(Bytes.utf8("bar")))
    assertEquals(first, Some(Success(Some("bar"))))

    var second: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => second = Some(r))
    assertEquals(second, Some(Success(Some("bar")))) // served locally
    assertEquals(transports.head.written.length, 1)  // no new round-trip

    transports.head.emit(invalidationOf("foo"))
    var third: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => third = Some(r))
    assertEquals(transports.head.written.length, 2) // evicted -> refetch
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(Frame.BulkString(Bytes.utf8("baz")))
    assertEquals(third, Some(Success(Some("baz"))))
  }

  test("TTL expiry evicts a cached read independently of invalidations") {
    val (connection, scheduler, transports) = cachedConnection()
    val get                                 = Strings.get[String, String]("foo")

    connection.cachedSubmit(get, 1000L, _ => ())
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(Frame.BulkString(Bytes.utf8("bar")))
    assertEquals(transports.head.written.length, 1)

    scheduler.advance(1001.millis)                  // past the TTL
    connection.cachedSubmit(get, 1000L, _ => ())
    assertEquals(transports.head.written.length, 2) // expired -> refetch
  }

  test("a reconnect flushes the cache: tracking state is connection-bound") {
    val (connection, scheduler, transports) = cachedConnection()
    val get                                 = Strings.get[String, String]("foo")

    connection.cachedSubmit(get, 60000L, _ => ())
    transports.head.emit(Frame.SimpleString("OK"))
    transports.head.emit(Frame.BulkString(Bytes.utf8("bar")))

    transports.head.emit(Frame.SimpleString("stray")) // nothing pending -> discard -> reconnect
    assertEquals(connection.currentState, MultiplexedConnection.State.Reconnecting)
    scheduler.advance(1.milli)
    assertEquals(transports.size, 2)

    var afterReconnect: Option[Try[Option[String]]] = None
    connection.cachedSubmit(get, 60000L, r => afterReconnect = Some(r))
    assertEquals(transports(1).written.length, 1) // fresh generation, empty cache -> refetch
    transports(1).emit(Frame.SimpleString("OK"))
    transports(1).emit(Frame.BulkString(Bytes.utf8("baz")))
    assertEquals(afterReconnect, Some(Success(Some("baz"))))
  }
}

package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.concurrent.duration.*

import sage.Bytes
import sage.SageException.NotConnected
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.commands.{Command, Connection}
import sage.protocol.Frame

class SubscriptionConnectionSpec extends munit.FunSuite {

  private val fixedBackoff = BackoffConfig(initialDelay = 1.milli, maxDelay = 1.milli, multiplier = 1.0)
  private val noWatchdog   = WatchdogConfig(enabled = false)

  // models the server's replies: PONG to the bootstrap/watchdog PING, and one subscribe/psubscribe confirmation push per subscribed name
  private val serverResponder: Bytes => Seq[Frame] = { payload =>
    val s = payload.asUtf8String
    if (s.contains("\r\nPING\r\n")) Seq(Frame.SimpleString("PONG"))
    else if (s.contains("\r\nSUBSCRIBE\r\n")) confirmations("subscribe", s)
    else if (s.contains("\r\nPSUBSCRIBE\r\n")) confirmations("psubscribe", s)
    else if (s.contains("\r\nSSUBSCRIBE\r\n")) confirmations("ssubscribe", s)
    else Nil
  }

  // a SUBSCRIBE/PSUBSCRIBE is a RESP array `*K`; K-1 of those elements are channel names, each acknowledged by one confirmation push
  private def confirmations(kind: String, payload: String): Seq[Frame] = {
    val names = payload.drop(1).takeWhile(_ != '\r').toInt - 1
    (1 to names).map(i => Frame.Push(Vector(Frame.BulkString(Bytes.utf8(kind)), Frame.BulkString(Bytes.utf8("?")), Frame.Integer(i.toLong))))
  }

  private def make(
    isLive: () => Boolean = () => true,
    watchdog: WatchdogConfig = noWatchdog,
    bufferSize: Int = 16,
    respond: Bytes => Seq[Frame] = serverResponder,
    bootstrap: Vector[Command[?]] = Vector(Connection.ping())
  ): (SubscriptionConnection, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, respond, autoWrite = true)
      transports += transport
      transport
    }
    val connection                                      = new SubscriptionConnection(factory, bootstrap, scheduler, fixedBackoff, watchdog, 1000L, bufferSize, isLive)
    (connection, scheduler, transports)
  }

  // a SUBSCRIBE token is "\r\nSUBSCRIBE\r\n" on the wire — never a substring of UN/PSUBSCRIBE, which are preceded by UN/P
  private def wrote(transport: FakeTransport, command: String): Boolean =
    transport.written.exists(_.asUtf8String.contains(s"\r\n$command\r\n"))

  private def message(channel: String, payload: String): Frame =
    Frame.Push(Vector(Frame.BulkString(Bytes.utf8("message")), Frame.BulkString(Bytes.utf8(channel)), Frame.BulkString(Bytes.utf8(payload))))

  private def shardMessage(channel: String, payload: String): Frame =
    Frame.Push(Vector(Frame.BulkString(Bytes.utf8("smessage")), Frame.BulkString(Bytes.utf8(channel)), Frame.BulkString(Bytes.utf8(payload))))

  private def patternMessage(pattern: String, channel: String, payload: String): Frame =
    Frame.Push(
      Vector(
        Frame.BulkString(Bytes.utf8("pmessage")),
        Frame.BulkString(Bytes.utf8(pattern)),
        Frame.BulkString(Bytes.utf8(channel)),
        Frame.BulkString(Bytes.utf8(payload))
      )
    )

  // next is async (callback); block on it for assertions
  private def nextBlocking(sub: SubscriptionConnection.RawSubscription): Option[SubscriptionConnection.Delivery] = {
    val box   = new AtomicReference[Option[SubscriptionConnection.Delivery]]()
    val latch = new CountDownLatch(1)
    sub.next { delivery => box.set(delivery); latch.countDown() }
    latch.await()
    box.get()
  }

  private def nextBlocking(sink: SubscriptionConnection.Sink): Option[SubscriptionConnection.Delivery] = {
    val box   = new AtomicReference[Option[SubscriptionConnection.Delivery]]()
    val latch = new CountDownLatch(1)
    sink.next { delivery => box.set(delivery); latch.countDown() }
    latch.await()
    box.get()
  }

  // Bytes has no structural `==` (CONTEXT: compare with sameBytes), so destructure and compare the payload as text
  private def assertChannel(delivery: Option[SubscriptionConnection.Delivery], channel: String, payload: String)(using munit.Location): Unit =
    delivery match {
      case Some(SubscriptionConnection.Delivery.Channel(ch, p)) => assertEquals(ch, channel); assertEquals(p.asUtf8String, payload)
      case other                                                => fail(s"expected a channel delivery, got $other")
    }

  private def assertPattern(delivery: Option[SubscriptionConnection.Delivery], pattern: String, channel: String, payload: String)(
    using munit.Location
  ): Unit =
    delivery match {
      case Some(SubscriptionConnection.Delivery.Pattern(pat, ch, p)) =>
        assertEquals(pat, pattern); assertEquals(ch, channel); assertEquals(p.asUtf8String, payload)
      case other                                                     => fail(s"expected a pattern delivery, got $other")
    }

  test("first subscribe establishes the connection and sends SUBSCRIBE, then delivers a message") {
    val (connection, _, transports) = make()
    val sub                         = connection.subscribeChannels(Vector("news"))
    assertEquals(transports.size, 1)
    assert(wrote(transports.head, "SUBSCRIBE"))

    transports.head.emit(message("news", "hello"))
    assertChannel(nextBlocking(sub), "news", "hello")
  }

  test("subscribe is gated on the client being live") {
    val (connection, _, transports) = make(isLive = () => false)
    intercept[NotConnected](connection.subscribeChannels(Vector("news")))
    assertEquals(transports.size, 0)
  }

  test("two subscribers to one channel both receive the message; SUBSCRIBE is sent once") {
    val (connection, _, transports) = make()
    val a                           = connection.subscribeChannels(Vector("news"))
    val b                           = connection.subscribeChannels(Vector("news"))
    assertEquals(transports.size, 1)
    assertEquals(transports.head.written.count(_.asUtf8String.contains("\r\nSUBSCRIBE\r\n")), 1)

    transports.head.emit(message("news", "x"))
    assertChannel(nextBlocking(a), "news", "x")
    assertChannel(nextBlocking(b), "news", "x")
  }

  test("UNSUBSCRIBE is sent only when the last subscriber of a channel closes, and the connection is torn down") {
    val (connection, _, transports) = make()
    val a                           = connection.subscribeChannels(Vector("news"))
    val b                           = connection.subscribeChannels(Vector("news"))

    a.close()
    assert(!wrote(transports.head, "UNSUBSCRIBE"), "another subscriber still wants the channel")
    assertEquals(transports.head.closeCount, 0)

    b.close()
    assert(wrote(transports.head, "UNSUBSCRIBE"), "the last subscriber leaving unsubscribes")
    assertEquals(transports.head.closeCount, 1)
  }

  test("pattern subscriptions deliver the matching pattern and concrete channel") {
    val (connection, _, transports) = make()
    val sub                         = connection.subscribePatterns(Vector("news.*"))
    assert(wrote(transports.head, "PSUBSCRIBE"))

    transports.head.emit(patternMessage("news.*", "news.sports", "goal"))
    assertPattern(nextBlocking(sub), "news.*", "news.sports", "goal")
  }

  test("shard subscriptions send SSUBSCRIBE and deliver an smessage as a channel delivery") {
    val (connection, _, transports) = make()
    val sub                         = connection.subscribeShard(Vector("orders"))
    assert(wrote(transports.head, "SSUBSCRIBE"))

    transports.head.emit(shardMessage("orders", "new"))
    assertChannel(nextBlocking(sub), "orders", "new")
  }

  test("a dropped connection reconnects and resubscribes every active subscription") {
    val (connection, scheduler, transports) = make()
    val sub                                 = connection.subscribeChannels(Vector("news"))

    transports.head.close() // simulate connection loss
    scheduler.advance(1.milli)

    assertEquals(transports.size, 2)
    assert(wrote(transports(1), "SUBSCRIBE"), "the new socket re-issues the active subscription")

    transports(1).emit(message("news", "after-reconnect"))
    assertChannel(nextBlocking(sub), "news", "after-reconnect")
  }

  test("closing the client terminates active subscription streams") {
    val (connection, _, transports) = make()
    val sub                         = connection.subscribeChannels(Vector("news"))
    connection.close()
    assertEquals(nextBlocking(sub), None)
    assertEquals(transports.head.closeCount, 1)
  }

  test("a watchdog kill does not poison the next generation (no stale pingSentAtMillis kill-loop)") {
    val silentToPing: Bytes => Seq[Frame]   = { payload =>
      val s = payload.asUtf8String
      if (s.contains("\r\nSELECT\r\n")) Seq(Frame.SimpleString("OK"))
      else if (s.contains("\r\nSUBSCRIBE\r\n")) confirmations("subscribe", s)
      else Nil
    }
    val watchdog                            = WatchdogConfig(pingInterval = 100.millis, pingTimeout = 50.millis, enabled = true)
    val (connection, scheduler, transports) = make(watchdog = watchdog, respond = silentToPing, bootstrap = Vector(Connection.select(0)))
    val sub                                 = connection.subscribeChannels(Vector("news"))
    assertEquals(transports.size, 1)

    scheduler.advance(250.millis)
    assertEquals(transports.size, 2, "the killed connection reconnects to a fresh transport")
    assertEquals(transports.head.closeCount, 1, "the unresponsive original connection was killed by the watchdog")

    scheduler.advance(200.millis)
    assertEquals(transports(1).closeCount, 0, "the fresh generation must not be killed by a ping left outstanding on the dead one")
    assertEquals(transports.size, 2, "no further reconnect churn")

    transports(1).emit(message("news", "after-reconnect"))
    assertChannel(nextBlocking(sub), "news", "after-reconnect")
  }

  test("a steady stream of message pushes does not suppress the keepalive PING (push proves only the read path)") {
    val silentToPing: Bytes => Seq[Frame]   = { payload =>
      val s = payload.asUtf8String
      if (s.contains("\r\nSELECT\r\n")) Seq(Frame.SimpleString("OK"))
      else if (s.contains("\r\nSUBSCRIBE\r\n")) confirmations("subscribe", s)
      else Nil // silent to PING, so a fired keepalive goes unanswered and the watchdog must eventually kill the socket
    }
    val watchdog                            = WatchdogConfig(pingInterval = 100.millis, pingTimeout = 50.millis, enabled = true)
    val (connection, scheduler, transports) = make(watchdog = watchdog, respond = silentToPing, bootstrap = Vector(Connection.select(0)))
    val _                                   = connection.subscribeChannels(Vector("news"))
    assertEquals(transports.size, 1)

    var iteration = 0
    while (iteration < 3) {
      transports.head.emit(message("news", "tick"))
      scheduler.advance(90.millis)
      iteration += 1
    }

    assertEquals(transports.head.closeCount, 1, "the watchdog killed the socket despite continuous inbound pushes")
    assertEquals(transports.size, 2, "the killed connection reconnected")
  }

  test("subscribing to multiple channels in one call delivers messages from any of them") {
    val (connection, _, transports) = make()
    val sub                         = connection.subscribeChannels(Vector("a", "b"))
    transports.head.emit(message("b", "from-b"))
    assertChannel(nextBlocking(sub), "b", "from-b")
    transports.head.emit(message("a", "from-a"))
    assertChannel(nextBlocking(sub), "a", "from-a")
  }

  test("closeIfEmpty keeps a connection holding a sink, closes it once empty, and then rejects a racing attach") {
    val (connection, _, transports) = make()
    val sink                        = new SubscriptionConnection.Sink(Vector("orders"), SubscriptionConnection.Kind.Shard, 16)
    connection.attach(sink, Vector("orders"), SubscriptionConnection.Kind.Shard)

    assert(!connection.closeIfEmpty(), "a connection carrying a live sink must not be evicted")
    assertEquals(transports.head.closeCount, 0)

    val _ = connection.detach(sink, Vector("orders"), SubscriptionConnection.Kind.Shard)
    assert(connection.closeIfEmpty(), "with its last sink gone the connection is empty and closes")
    assertEquals(transports.head.closeCount, 1)

    val late = new SubscriptionConnection.Sink(Vector("late"), SubscriptionConnection.Kind.Shard, 16)
    intercept[NotConnected](connection.attach(late, Vector("late"), SubscriptionConnection.Kind.Shard))
  }

  test("shutdown drops the socket but leaves the sink usable, so a re-home can re-attach it to a fresh connection") {
    val (conn1, _, transports1) = make()
    val sink                    = new SubscriptionConnection.Sink(Vector("news"), SubscriptionConnection.Kind.Channel, 16)
    conn1.attach(sink, Vector("news"), SubscriptionConnection.Kind.Channel)

    conn1.shutdown()
    assertEquals(transports1.head.closeCount, 1, "the abandoned socket is closed")

    val (conn2, _, transports2) = make()
    conn2.attach(sink, Vector("news"), SubscriptionConnection.Kind.Channel)
    transports2.head.emit(message("news", "after-rehome"))
    assertChannel(nextBlocking(sink), "news", "after-rehome")
  }

  test("an unexpected error reply (e.g. MOVED on a subscribe) drops the connection instead of being swallowed as a PONG") {
    var terminated                                      = false
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, serverResponder); transports += t; t
    }
    val connection                                      = new SubscriptionConnection(
      factory,
      Vector(Connection.ping()),
      scheduler,
      fixedBackoff,
      noWatchdog,
      1000L,
      16,
      () => true,
      cluster = true,
      onTerminated = () => terminated = true
    )
    val sink                                            = new SubscriptionConnection.Sink(Vector("orders"), SubscriptionConnection.Kind.Shard, 16)
    connection.attach(sink, Vector("orders"), SubscriptionConnection.Kind.Shard)
    assertEquals(transports.size, 1)

    transports.head.emit(Frame.SimpleError("MOVED 1234 10.0.0.2:6379"))
    scheduler.advance(Duration.Zero) // the close is scheduled off the reader thread
    assert(terminated, "a MOVED error on the subscribe connection must drop it so the manager re-homes")
    assertEquals(transports.head.closeCount, 1)
  }

  test("a socket dying in the establish->live window fires onTerminated in cluster mode instead of going silently Live") {
    final class DyingAfterBootstrap(onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {
      private var bootstrapped             = false
      def start(): Unit                    = ()
      def send(item: Transport.Item): Unit = {
        item.writeAttempted()
        serverResponder(item.payload).foreach(onFrame)
        if (!bootstrapped) { bootstrapped = true; onClosed() }
      }
      def close(): Unit                    = ()
    }
    var terminatedCalled = false
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => new DyingAfterBootstrap(onFrame, onClosed)
    val connection                                      = new SubscriptionConnection(
      factory,
      Vector(Connection.ping()),
      scheduler,
      fixedBackoff,
      noWatchdog,
      1000L,
      16,
      () => true,
      cluster = true,
      onTerminated = () => terminatedCalled = true
    )

    val sink = new SubscriptionConnection.Sink(Vector("news"), SubscriptionConnection.Kind.Channel, 16)
    connection.attach(sink, Vector("news"), SubscriptionConnection.Kind.Channel)

    assert(terminatedCalled, "a connection that died in the establish->live window must notify the manager, not go silently Live")
  }

  test("a concurrent consumer on one sink is rejected rather than silently evicting the parked waiter") {
    val sink                                                    = new SubscriptionConnection.Sink(Vector("news"), SubscriptionConnection.Kind.Channel, 16)
    val parked: Option[SubscriptionConnection.Delivery] => Unit = _ => ()
    sink.next(parked)
    intercept[IllegalStateException](sink.next(_ => ()))
  }

  test("deregistering an interrupted waiter lets the next poll re-arm instead of tripping the single-consumer guard") {
    val sink                                                       = new SubscriptionConnection.Sink(Vector("news"), SubscriptionConnection.Kind.Channel, 16)
    val abandoned: Option[SubscriptionConnection.Delivery] => Unit = _ => ()
    sink.next(abandoned)
    sink.cancelNext(abandoned)

    val box   = new AtomicReference[Option[SubscriptionConnection.Delivery]]()
    val latch = new CountDownLatch(1)
    sink.next { delivery => box.set(delivery); latch.countDown() }
    sink.offer(SubscriptionConnection.Delivery.Channel("news", Bytes.utf8("hello")))
    latch.await()
    assertChannel(box.get(), "news", "hello")
  }

  test("cancelNext is identity-checked and does not evict a live waiter registered by another call") {
    val sink                                                  = new SubscriptionConnection.Sink(Vector("news"), SubscriptionConnection.Kind.Channel, 16)
    val live: Option[SubscriptionConnection.Delivery] => Unit = _ => ()
    sink.next(live)
    sink.cancelNext(_ => ())
    intercept[IllegalStateException](sink.next(_ => ()))
  }

  test("a resubscribe write that fails during goLive tears the socket down instead of orphaning a subscribed connection") {
    final class FailingSubscribe(onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {
      var closeCount                       = 0
      def start(): Unit                    = ()
      def send(item: Transport.Item): Unit = {
        if (item.payload.asUtf8String.contains("\r\nSUBSCRIBE\r\n")) throw new RuntimeException("write failed")
        item.writeAttempted()
        serverResponder(item.payload).foreach(onFrame)
      }
      def close(): Unit                    = { closeCount += 1; onClosed() }
    }
    val transports = mutable.ArrayBuffer.empty[FailingSubscribe]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FailingSubscribe(onFrame, onClosed); transports += t; t
    }
    val connection                                      =
      new SubscriptionConnection(factory, Vector(Connection.ping()), new ManualScheduler, fixedBackoff, noWatchdog, 1000L, 16, () => true)

    intercept[RuntimeException](connection.subscribeChannels(Vector("news")))
    assertEquals(transports.head.closeCount, 1, "the socket whose resubscribe failed must be closed, not left dispatching")
    assert(connection.isEmpty, "the phantom sink is deregistered so the connection holds nothing")
  }

  test("a failed unsubscribe write during close still wakes the parked consumer and tears the socket down") {
    final class FailingUnsubscribe(onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {
      var closeCount                       = 0
      def start(): Unit                    = ()
      def send(item: Transport.Item): Unit = {
        if (item.payload.asUtf8String.contains("\r\nUNSUBSCRIBE\r\n")) throw new RuntimeException("write failed")
        item.writeAttempted()
        serverResponder(item.payload).foreach(onFrame)
      }
      def close(): Unit                    = { closeCount += 1; onClosed() }
    }
    val transports = mutable.ArrayBuffer.empty[FailingUnsubscribe]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FailingUnsubscribe(onFrame, onClosed); transports += t; t
    }
    val connection                                      =
      new SubscriptionConnection(factory, Vector(Connection.ping()), new ManualScheduler, fixedBackoff, noWatchdog, 1000L, 16, () => true)

    val sub   = connection.subscribeChannels(Vector("news"))
    val box   = new AtomicReference[Option[SubscriptionConnection.Delivery]]()
    val latch = new CountDownLatch(1)
    sub.next { delivery => box.set(delivery); latch.countDown() }

    intercept[RuntimeException](sub.close())
    assert(latch.await(1, java.util.concurrent.TimeUnit.SECONDS), "the parked consumer must be woken, not left hanging")
    assertEquals(box.get(), None)
    assertEquals(transports.head.closeCount, 1)
  }

  test("a connect failure on the subscribe path surfaces as a modeled ConnectionFailed, not a raw defect") {
    val factory: MultiplexedConnection.TransportFactory = (_, _) =>
      new Transport {
        def start(): Unit                    = throw new java.net.ConnectException("connection refused")
        def send(item: Transport.Item): Unit = ()
        def close(): Unit                    = ()
      }
    val connection                                      =
      new SubscriptionConnection(factory, Vector(Connection.ping()), new ManualScheduler, fixedBackoff, noWatchdog, 1000L, 16, () => true)

    intercept[sage.SageException.ConnectionFailed](connection.subscribeChannels(Vector("news")))
  }

  test("a failed subscription reconnect emits ReconnectFailed instead of retrying in silence") {
    val recorded                                        = new java.util.concurrent.ConcurrentLinkedQueue[sage.SageEvent]()
    val events                                          = new Events {
      def enabled: Boolean                      = true
      def emitsEvents: Boolean                  = true
      def tracer: Option[sage.CommandTracer]    = None
      def serverNode: Option[sage.cluster.Node] = None
      def emit(event: sage.SageEvent): Unit     = { val _ = recorded.add(event) }
      def close(): Unit                         = ()
    }
    var healthy                                         = true
    val respond: Bytes => Seq[Frame]                    = payload =>
      if (!healthy && payload.asUtf8String.contains("\r\nPING\r\n")) Seq(Frame.SimpleError("WRONGPASS invalid password"))
      else serverResponder(payload)
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, respond); transports += t; t
    }
    val scheduler                                       = new ManualScheduler
    val connection                                      =
      new SubscriptionConnection(factory, Vector(Connection.ping()), scheduler, fixedBackoff, noWatchdog, 1000L, 16, () => true, events = events)

    connection.subscribeChannels(Vector("news"))
    healthy = false
    transports.head.close()
    scheduler.advance(1.milli)
    assert(
      recorded.toArray.exists {
        case sage.SageEvent.Connection.ReconnectFailed(None, _: sage.SageException.ServerError) => true
        case _                                                                                  => false
      },
      recorded.toArray.mkString(", ")
    )
  }

  test("detach swallows an interrupted unsubscribe write so a cluster close can still terminate the sink") {
    final class InterruptingUnsubscribe(onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {
      def start(): Unit                    = ()
      def send(item: Transport.Item): Unit = {
        if (item.payload.asUtf8String.contains("\r\nUNSUBSCRIBE\r\n")) throw new InterruptedException("interrupted")
        item.writeAttempted()
        serverResponder(item.payload).foreach(onFrame)
      }
      def close(): Unit                    = onClosed()
    }
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => new InterruptingUnsubscribe(onFrame, onClosed)
    val connection =
      new SubscriptionConnection(
        factory,
        Vector(Connection.ping()),
        new ManualScheduler,
        fixedBackoff,
        noWatchdog,
        1000L,
        16,
        () => true,
        cluster = true
      )

    val sink = new SubscriptionConnection.Sink(Vector("news"), SubscriptionConnection.Kind.Channel, 16)
    connection.attach(sink, Vector("news"), SubscriptionConnection.Kind.Channel)
    assert(connection.detach(sink, Vector("news"), SubscriptionConnection.Kind.Channel)) // must return, not throw the interrupt
  }

  test("close aborts a subscription connection still blocked in the connect (start) phase") {
    val connecting                                      = new AtomicReference[ConnectingTransport]()
    val factory: MultiplexedConnection.TransportFactory = (_, onClosed) => {
      val transport = new ConnectingTransport(onClosed)
      connecting.set(transport)
      transport
    }
    val connection                                      =
      new SubscriptionConnection(factory, Vector(Connection.ping()), new ManualScheduler, fixedBackoff, noWatchdog, 1000L, 16, () => true)

    val subscribing = new Thread(() =>
      try { val _ = connection.subscribeChannels(Vector("news")) }
      catch { case _: Throwable => () }
    )
    subscribing.start() // blocks in the connect

    val deadline = System.currentTimeMillis() + 2000
    while (connecting.get() == null && System.currentTimeMillis() < deadline) Thread.sleep(1)
    assert(connecting.get() != null, "the establish never started")
    assert(connecting.get().reached.await(2, TimeUnit.SECONDS), "the establish never reached the connect phase")

    connection.close()
    subscribing.join(2000)

    assert(!subscribing.isAlive, "close must abort the establishing connection, not wait out the connect")
    assert(connecting.get().wasClosed, "close must abort the establishing transport")
  }

  test("close unblocks a subscriber parked waiting for the bootstrap reply") {
    val pinged                                          = new CountDownLatch(1)
    val factory: MultiplexedConnection.TransportFactory =
      (onFrame, onClosed) => new FakeTransport(onFrame, onClosed, payload => { if (payload.asUtf8String.contains("PING")) pinged.countDown(); Nil })
    val connection                                      =
      new SubscriptionConnection(factory, Vector(Connection.ping()), new ManualScheduler, fixedBackoff, noWatchdog, 60000L, 16, () => true)

    val subscribing = new Thread(() =>
      try { val _ = connection.subscribeChannels(Vector("news")) }
      catch { case _: Throwable => () }
    )
    subscribing.start()
    assert(pinged.await(2, TimeUnit.SECONDS), "the bootstrap PING was never sent")

    connection.close()
    subscribing.join(2000)

    assert(!subscribing.isAlive, "close must fail the bootstrap wait, not leave the caller parked for the connect timeout")
  }

  test("close aborts every overlapping establishment, not just the most recent") {
    // a reconnect and a fresh attach can be establishing at once; close must abort both
    val scheduler                                       = new ManualScheduler
    val attempt                                         = new java.util.concurrent.atomic.AtomicInteger(0)
    val connecting                                      = new java.util.concurrent.CopyOnWriteArrayList[ConnectingTransport]()
    var fake: FakeTransport                             = null
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) =>
      if (attempt.getAndIncrement() == 0) { fake = new FakeTransport(onFrame, onClosed, serverResponder); fake }
      else { val t = new ConnectingTransport(onClosed); connecting.add(t); t }
    val connection                                      =
      new SubscriptionConnection(factory, Vector(Connection.ping()), scheduler, fixedBackoff, noWatchdog, 5000L, 16, () => true)

    def awaitReached(i: Int): ConnectingTransport = {
      val deadline = System.currentTimeMillis() + 2000
      while (connecting.size <= i && System.currentTimeMillis() < deadline) Thread.sleep(1)
      assert(connecting.size > i, s"establishment $i never started")
      val t        = connecting.get(i)
      assert(t.reached.await(2, TimeUnit.SECONDS), s"establishment $i never reached connect")
      t
    }

    val sub = connection.subscribeChannels(Vector("a"))
    fake.close() // the socket drops -> Reconnecting, reconnect scheduled

    val reconnect = new Thread(() => scheduler.advance(1.milli)) // blocks in the reconnect's connect
    reconnect.start()
    val conn1     = awaitReached(0)

    sub.close() // Reconnecting -> Idle, leaving conn1 still establishing

    val attaching = new Thread(() =>
      try { val _ = connection.subscribeChannels(Vector("b")) }
      catch { case _: Throwable => () }
    )
    attaching.start() // blocks in the attach's connect
    val conn2     = awaitReached(1)

    connection.close()
    reconnect.join(2000)
    attaching.join(2000)

    assert(!reconnect.isAlive && !attaching.isAlive, "close must unblock both establishments")
    assert(conn1.wasClosed, "close must abort the reconnect establishment")
    assert(conn2.wasClosed, "close must abort the concurrent attach establishment")
  }
}

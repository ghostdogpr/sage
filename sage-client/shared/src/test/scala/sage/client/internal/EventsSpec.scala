package sage.client.internal

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

import sage.{Bytes, CommandTracer, SageEvent, SageListener}
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.{Connection, Strings}
import sage.protocol.Frame

class EventsSpec extends munit.FunSuite {

  private val fixedBackoff = BackoffConfig(initialDelay = 1.milli, maxDelay = 1.milli, multiplier = 1.0)
  private val noWatchdog   = WatchdogConfig(enabled = false)

  // a synchronous sink, so wiring assertions are deterministic; the real Dispatcher's async behavior is exercised separately
  final private class Recording(val tracer: Option[CommandTracer] = None, val serverNode: Option[Node] = None) extends Events {
    private val buf                  = mutable.ArrayBuffer.empty[SageEvent]
    def enabled: Boolean             = true
    def emitsEvents: Boolean         = true
    def emit(event: SageEvent): Unit = synchronized { val _ = buf += event }
    def close(): Unit                = ()
    def events: Vector[SageEvent]    = synchronized(buf.toVector)
  }

  final private class Capturing(latch: CountDownLatch) extends SageListener {
    val received                        = new ConcurrentLinkedQueue[SageEvent]()
    def onEvent(event: SageEvent): Unit = { received.add(event); latch.countDown() }
  }

  private def connect(
    node: Option[Node],
    events: Events,
    respond: Bytes => Seq[Frame] = _ => Nil
  ): (MultiplexedConnection, mutable.ArrayBuffer[FakeTransport]) = {
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, respond)
      transports += transport
      transport
    }
    val connection                                      =
      MultiplexedConnection
        .connect(factory, new ManualScheduler, Vector.empty, fixedBackoff, noWatchdog, 1.second, Duration.Zero, 1L << 20, node, events)
    (connection, transports)
  }

  // --- the dispatcher --------------------------------------------------------------------------------------------------------------------

  test("no listeners yields the disabled, no-op sink") {
    assert(!Events(Vector.empty).enabled)
    assert(Events(Vector.empty) eq Events.disabled)
  }

  test("delivers each event to every registered listener") {
    val latch = new CountDownLatch(2)
    val a     = new Capturing(latch)
    val b     = new Capturing(latch)
    val bus   = Events(Vector(a, b))
    try {
      bus.emit(SageEvent.TopologyChanged(Vector(Node("h", 1))))
      assert(latch.await(2, TimeUnit.SECONDS))
      assertEquals(a.received.asScala.toVector, Vector(SageEvent.TopologyChanged(Vector(Node("h", 1)))))
      assertEquals(b.received.asScala.toVector, Vector(SageEvent.TopologyChanged(Vector(Node("h", 1)))))
    } finally bus.close()
  }

  test("a throwing listener does not stop delivery to its peers") {
    val latch    = new CountDownLatch(1)
    val throwing = new SageListener { def onEvent(event: SageEvent): Unit = throw new RuntimeException("boom") }
    val healthy  = new Capturing(latch)
    val bus      = Events(Vector(throwing, healthy))
    try {
      bus.emit(SageEvent.Cache.Hit("GET"))
      assert(latch.await(2, TimeUnit.SECONDS))
      assertEquals(healthy.received.peek(), SageEvent.Cache.Hit("GET"))
    } finally bus.close()
  }

  test("a full queue drops events rather than blocking the producer") {
    val release  = new CountDownLatch(1)
    val seen     = new java.util.concurrent.atomic.AtomicInteger(0)
    // blocks on the first event so the queue fills behind it
    val blocking = new SageListener {
      def onEvent(event: SageEvent): Unit = { seen.incrementAndGet(); release.await() }
    }
    val bus      = Events(Vector(blocking))
    try {
      val started = System.nanoTime()
      var i       = 0
      while (i < 5000) { bus.emit(SageEvent.Cache.Miss("GET")); i += 1 }
      val elapsed = (System.nanoTime() - started).nanos
      assert(elapsed < 2.seconds, s"emit blocked: took $elapsed for 5000 events")
    } finally {
      release.countDown()
      bus.close()
    }
    assert(seen.get() <= 1100, s"expected drops, retained ${seen.get()}")
  }

  // --- command completion ----------------------------------------------------------------------------------------------------------------

  test("trackCommand emits a completion with name and outcome, and is transparent when disabled") {
    val rec                                = new Recording
    var settled                            = Option.empty[String]
    val tracked                            = Events.trackCommand[String](rec, Connection.ping(None), r => settled = r.toOption)
    tracked(Success("PONG"))
    assertEquals(settled, Some("PONG"))
    rec.events match {
      case Vector(SageEvent.CommandCompleted("PING", None, _, sage.Outcome.Succeeded)) => ()
      case other                                                                       => fail(s"unexpected: $other")
    }
    val cb: scala.util.Try[String] => Unit = _ => ()
    val plain                              = Events.trackCommand[String](Events.disabled, Connection.ping(None), cb)
    assert(plain eq cb) // disabled wraps nothing, so a listener-less client pays nothing
  }

  test("attributeNode tags the completion with the node the routing layer resolved") {
    val rec     = new Recording
    val tracked = Events.trackCommand[Long](rec, Strings.incr[String]("k"), _ => ())
    Events.attributeNode(tracked, Node("redis", 7000))
    tracked(Failure(sage.SageException.NotConnected()))
    rec.events match {
      case Vector(SageEvent.CommandCompleted("INCR", Some(Node("redis", 7000)), _, sage.Outcome.Failed(_))) => ()
      case other                                                                                            => fail(s"unexpected: $other")
    }
  }

  // --- tracing ---------------------------------------------------------------------------------------------------------------------------

  test("a tracer-only bus is enabled but emits no events, and drives the span lifecycle through trackCommand") {
    val tracer = new RecordingTracer
    val bus    = Events(Vector.empty, Some(tracer))
    assert(bus.enabled)
    assert(!bus.emitsEvents) // no listeners, so no CommandCompleted events are produced
    val tracked = Events.trackCommand[String](bus, Connection.ping(None), _ => ())
    Events.attributeNode(tracked, Node("redis", 7000))
    tracked(Success("PONG"))
    assertEquals(tracer.log.toVector, Vector("start:PING", "routed:redis:7000", "settled:Succeeded"))
  }

  test("startSpan starts the span once on the caller; the trackCommand overload reuses it and still emits the event") {
    val tracer  = new RecordingTracer
    val rec     = new Recording(Some(tracer))
    val span    = Events.startSpan(rec, Connection.ping(None))
    assertEquals(tracer.log.toVector, Vector("start:PING"))
    val tracked = Events.trackCommand[String](rec, Connection.ping(None), _ => (), span)
    assertEquals(tracer.log.toVector, Vector("start:PING"))
    tracked(Success("PONG"))
    assertEquals(tracer.log.toVector, Vector("start:PING", "settled:Succeeded"))
    assert(rec.events.exists { case _: SageEvent.CommandCompleted => true; case _ => false })
  }

  test("startSpan routes to a fixed serverNode when set (standalone), without a node ever being attributed") {
    val tracer = new RecordingTracer
    val rec    = new Recording(Some(tracer), serverNode = Some(Node("localhost", 6379)))
    val span   = Events.startSpan(rec, Connection.ping(None))
    assertEquals(tracer.log.toVector, Vector("start:PING", "routed:localhost:6379"))
    Events.settleSpan(span, sage.Outcome.Succeeded)
    assertEquals(tracer.log.toVector, Vector("start:PING", "routed:localhost:6379", "settled:Succeeded"))
  }

  test("deferSpan starts no span until startDeferred invokes its factory (a cache miss)") {
    val tracer = new RecordingTracer
    val rec    = new Recording(Some(tracer))
    val make   = Events.deferSpan(rec, Connection.ping(None))
    assertEquals(tracer.log.toVector, Vector.empty) // capturing the context starts no span; a hit never invokes it
    val span = Events.startDeferred(make)
    assertEquals(tracer.log.toVector, Vector("start:PING"))
    Events.settleSpan(span, sage.Outcome.Succeeded)
    assertEquals(tracer.log.last, "settled:Succeeded")
  }

  test("a failure settles the span as Failed") {
    val tracer  = new RecordingTracer
    val bus     = Events(Vector.empty, Some(tracer))
    val tracked = Events.trackCommand[Long](bus, Strings.incr[String]("k"), _ => ())
    tracked(Failure(sage.SageException.NotConnected()))
    assertEquals(tracer.log.head, "start:INCR")
    assert(tracer.log.last.startsWith("settled:Failed"))
  }

  test("trackSpan drives the span but emits no event, and is transparent when no tracer is set") {
    val tracer  = new RecordingTracer
    val rec     = new Recording(Some(tracer)) // emitsEvents is true, yet trackSpan must still not emit a CommandCompleted
    val tracked = Events.trackSpan[String](rec, Connection.ping(None), _ => ())
    tracked(Success("PONG"))
    assertEquals(tracer.log.toVector, Vector("start:PING", "settled:Succeeded"))
    assertEquals(rec.events, Vector.empty)

    val cb: scala.util.Try[String] => Unit = _ => ()
    assert(Events.trackSpan[String](Events.disabled, Connection.ping(None), cb) eq cb) // no tracer, no wrap
  }

  test("abandonSpan settles the span without invoking the callback (the fail-fast path)") {
    val tracer  = new RecordingTracer
    val bus     = Events(Vector.empty, Some(tracer))
    var called  = false
    val tracked = Events.trackCommand[String](bus, Connection.ping(None), _ => called = true)
    Events.abandonSpan(tracked, sage.SageException.NotConnected())
    assert(!called, "abandonSpan must not invoke the wrapped callback")
    assert(tracer.log.last.startsWith("settled:Failed"))
  }

  // --- connection lifecycle --------------------------------------------------------------------------------------------------------------

  test("emits Connected on connect and Disconnected on unexpected loss, tagged with the node") {
    val node            = Some(Node("shard-a", 6379))
    val rec             = new Recording
    val (_, transports) = connect(node, rec)
    transports.head.close()
    assertEquals(
      rec.events,
      Vector(SageEvent.Connection.Connected(node), SageEvent.Connection.Disconnected(node))
    )
  }

  test("a failed reconnect establish surfaces the cause as ReconnectFailed instead of retrying opaquely") {
    val node                                            = Some(Node("shard-a", 6379))
    val rec                                             = new Recording
    val scheduler                                       = new ManualScheduler
    var healthy                                         = true // first establish's PING succeeds; the reconnect's is rejected, as a rotated password would be
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val respond: Bytes => Seq[Frame] = _ => Seq(if (healthy) Frame.SimpleString("PONG") else Frame.SimpleError("WRONGPASS invalid password"))
      val t                            = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    val connection                                      =
      MultiplexedConnection
        .connect(factory, scheduler, Vector(Connection.ping()), fixedBackoff, noWatchdog, 1.second, Duration.Zero, 1L << 20, node, rec)
    val _                                               = connection

    healthy = false
    transports.head.close()
    scheduler.advance(1.milli)
    assert(
      rec.events.exists { case SageEvent.Connection.ReconnectFailed(`node`, _: sage.SageException.ServerError) => true; case _ => false },
      rec.events
    )
  }

  test("a reconnect establish aborted by close() is silent: shutdown is intentional, not a reconnect failure") {
    val node                                            = Some(Node("shard-a", 6379))
    val rec                                             = new Recording
    val scheduler                                       = new ManualScheduler
    var attempt                                         = 0
    var connection: MultiplexedConnection               = null
    val first                                           = new java.util.concurrent.atomic.AtomicReference[FakeTransport]()
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      attempt += 1
      if (attempt == 1) { val t = new FakeTransport(onFrame, onClosed, _ => Seq(Frame.SimpleString("PONG"))); first.set(t); t }
      else
        new Transport {
          def start(): Unit                    = { connection.close(); throw new RuntimeException("aborted by close") }
          def send(item: Transport.Item): Unit = ()
          def close(): Unit                    = ()
        }
    }
    connection = MultiplexedConnection
      .connect(factory, scheduler, Vector(Connection.ping()), fixedBackoff, noWatchdog, 1.second, Duration.Zero, 1L << 20, node, rec)

    first.get().close()
    scheduler.advance(1.milli)
    assert(!rec.events.exists { case _: SageEvent.Connection.ReconnectFailed => true; case _ => false }, rec.events)
  }

  // --- cache -----------------------------------------------------------------------------------------------------------------------------

  test("a cached read misses then hits") {
    var writes          = 0
    val rec             = new Recording
    val (connection, _) = connect(
      None,
      rec,
      respond = _ => {
        writes += 1
        // the first write is the [CLIENT CACHING YES, GET] batch: reply OK to the marker, the value to the read
        if (writes == 1) Seq(Frame.SimpleString("OK"), Frame.BulkString(Bytes.utf8("v"))) else Nil
      }
    )
    val get             = Strings.get[String, String]("k")
    connection.cachedSubmit(get, 60000L, (_: scala.util.Try[Option[String]]) => ())
    connection.cachedSubmit(get, 60000L, (_: scala.util.Try[Option[String]]) => ())
    val evs             = rec.events
    assertEquals(evs.collect { case c: SageEvent.Cache => c }, Vector(SageEvent.Cache.Miss("GET"), SageEvent.Cache.Hit("GET")))
    // the miss touched the server, so it also produces one CommandCompleted; the hit produces none
    assertEquals(
      evs.collect { case c: SageEvent.CommandCompleted => (c.name, c.node, c.outcome) },
      Vector(("GET", None, sage.Outcome.Succeeded))
    )
  }
}

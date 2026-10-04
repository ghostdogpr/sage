package sage.client.internal

import java.io.IOException
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

import sage.{Bytes, CommandTracer, SageEvent, SageListener}
import sage.client.{BackoffConfig, CacheConfig, SageConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.{Connection, Strings}
import sage.protocol.Frame

class EventsSpec extends munit.FunSuite {

  private val fixedBackoff = BackoffConfig(initialDelay = 1.milli, maxDelay = 1.milli, multiplier = 1.0)
  private val noWatchdog   = WatchdogConfig(enabled = false)

  // record events synchronously so tests can compare event and tracing order. Separate tests cover asynchronous dispatch.
  final private class Recording(val tracer: Option[CommandTracer] = None, val serverNode: Option[Node] = None) extends Events {
    private val buf                  = mutable.ArrayBuffer.empty[SageEvent]
    def enabled: Boolean             = true
    def emitsEvents: Boolean         = true
    def emit(event: SageEvent): Unit = synchronized(buf += event: Unit)
    def close(): Unit                = ()
    def events: Vector[SageEvent]    = synchronized(buf.toVector)
  }

  final private class Capturing(latch: CountDownLatch) extends SageListener {
    val received                        = new ConcurrentLinkedQueue[SageEvent]()
    def onEvent(event: SageEvent): Unit = {
      received.add(event)
      latch.countDown()
    }
  }

  private def connect(
    node: Option[Node],
    events: Events,
    respond: Bytes => Seq[Frame] = _ => Nil,
    clientCache: CacheConfig = CacheConfig(enabled = false)
  ): (MultiplexedConnection, mutable.ArrayBuffer[FakeTransport]) = {
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, Replies.withSetup(respond))
      transports += transport
      transport
    }
    val connection                                      =
      new MultiplexedConnection(
        factory,
        new ManualScheduler,
        SageConfig(
          reconnect = fixedBackoff,
          watchdog = noWatchdog,
          connectTimeout = 1.second,
          closeTimeout = Duration.Zero,
          clientCache = clientCache
        ),
        MultiplexedConnection.NodeRole.Master,
        node,
        events
      ).start()
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
      def onEvent(event: SageEvent): Unit = {
        seen.incrementAndGet()
        release.await()
      }
    }
    val bus      = Events(Vector(blocking))
    try {
      val started = System.nanoTime()
      var i       = 0
      while (i < 5000) {
        bus.emit(SageEvent.Cache.Miss("GET"))
        i += 1
      }
      val elapsed = (System.nanoTime() - started).nanos
      assert(elapsed < 2.seconds, s"emit blocked: took $elapsed for 5000 events")
    } finally {
      release.countDown()
      bus.close()
    }
    assert(seen.get() <= 1100, s"expected drops, retained ${seen.get()}")
  }

  test("ConnectFailed is emitted once per node outage and resets on Connected") {
    val node      = Node("reader", 6379)
    val other     = Node("other", 6379)
    val delivered = new CountDownLatch(4)
    val listener  = new Capturing(delivered)
    val bus       = Events(Vector(listener))
    try {
      bus.emit(SageEvent.Connection.ConnectFailed(Some(node), new IOException("first")))
      bus.emit(SageEvent.Connection.ConnectFailed(Some(node), new IOException("duplicate")))
      bus.emit(SageEvent.Connection.ConnectFailed(Some(other), new IOException("other")))
      bus.emit(SageEvent.Connection.Connected(Some(node)))
      bus.emit(SageEvent.Connection.ConnectFailed(Some(node), new IOException("new outage")))

      assert(delivered.await(2, TimeUnit.SECONDS), s"expected four connection events, got ${listener.received.asScala.toVector}")
      Thread.sleep(50)
      val received = listener.received.asScala.toVector
      assertEquals(received.map(_.asInstanceOf[SageEvent.Connection].node), Vector(Some(node), Some(other), Some(node), Some(node)))
      assertEquals(received.count(_.isInstanceOf[SageEvent.Connection.ConnectFailed]), 3)
    } finally bus.close()
  }

  test("a listener interrupted during shutdown drain still delivers to peers and completes the drain") {
    val started      = new CountDownLatch(1)
    val block        = new CountDownLatch(1)
    val calls        = new java.util.concurrent.atomic.AtomicInteger(0)
    val interrupting = new SageListener {
      def onEvent(event: SageEvent): Unit =
        if (calls.getAndIncrement() == 0) {
          started.countDown()
          block.await()
        } else throw new InterruptedException("interrupted again during the drain")
    }
    val peer         = new ConcurrentLinkedQueue[SageEvent]()
    val delivered    = new CountDownLatch(2)
    val healthy      = new SageListener {
      def onEvent(event: SageEvent): Unit = {
        peer.add(event)
        delivered.countDown()
      }
    }
    val bus          = Events(Vector(interrupting, healthy))
    try {
      bus.emit(SageEvent.Cache.Hit("first"))
      assert(started.await(2, TimeUnit.SECONDS), "the first callback never started")
      bus.emit(SageEvent.Cache.Hit("second"))
      bus.close()
      assert(delivered.await(2, TimeUnit.SECONDS), "the healthy peer did not receive both events")
      assertEquals(peer.asScala.toVector, Vector(SageEvent.Cache.Hit("first"), SageEvent.Cache.Hit("second")))
    } finally bus.close()
  }

  test("a listener that leaves the interrupt flag set does not permanently kill the worker") {
    val gotFirst         = new CountDownLatch(1)
    val gotSecond        = new CountDownLatch(1)
    val worker           = new java.util.concurrent.atomic.AtomicReference[Thread]()
    val first            = new java.util.concurrent.atomic.AtomicBoolean(true)
    val selfInterrupting = new SageListener {
      def onEvent(event: SageEvent): Unit = if (first.getAndSet(false)) {
        worker.set(Thread.currentThread())
        Thread.currentThread().interrupt()
      }
    }
    val healthy          = new SageListener {
      def onEvent(event: SageEvent): Unit = event match {
        case SageEvent.Cache.Hit("first")  => gotFirst.countDown()
        case SageEvent.Cache.Hit("second") => gotSecond.countDown()
        case _                             => ()
      }
    }
    val bus              = Events(Vector(selfInterrupting, healthy))
    try {
      bus.emit(SageEvent.Cache.Hit("first"))
      assert(gotFirst.await(2, TimeUnit.SECONDS), "the peer never received the first event")
      val t        = worker.get()
      val deadline = System.nanoTime() + 2.seconds.toNanos
      def settled  = t.getState == Thread.State.WAITING || t.getState == Thread.State.TERMINATED
      while (!settled && System.nanoTime() < deadline) Thread.sleep(5)
      assert(settled, "the worker never settled after the self-interrupt")
      bus.emit(SageEvent.Cache.Hit("second"))
      assert(gotSecond.await(2, TimeUnit.SECONDS), "the worker died after a self-interrupt; the later event was lost")
    } finally bus.close()
  }

  // --- command completion ----------------------------------------------------------------------------------------------------------------

  test("a submit interrupted on the caller's thread still completes the tracked command") {
    val rec         = new Recording
    var settled     = Option.empty[scala.util.Try[String]]
    val tracked     = Events.trackCommand[String](rec, Connection.ping(None), r => settled = Some(r))
    val interrupted = new InterruptedException
    Client.completing(tracked)(throw interrupted)
    assertEquals(settled, Some(Failure(interrupted)))
    rec.events match {
      case Vector(SageEvent.CommandCompleted("PING", None, _, sage.Outcome.Failed(`interrupted`))) => ()
      case other                                                                                   => fail(s"unexpected: $other")
    }
  }

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

  test("a tracked batch attributes each completion to the selected node, and routes its span there") {
    val tracer   = new RecordingTracer
    val rec      = new Recording(Some(tracer))
    val node     = Node("replica", 7001)
    val commands = Vector(Connection.ping(None))
    val batch    = new Client.TrackedBatch(rec, commands, Events.startSpans(rec, commands), _ => ())
    batch.callbacks(Some(node)).foreach(_(Success("PONG")))
    assertEquals(rec.events.collect { case c: SageEvent.CommandCompleted => c.node }, Vector(Some(node)))
    assert(tracer.log.contains(s"routed:${node.host}:${node.port}"), s"expected routedTo the selected node, got ${tracer.log.toVector}")
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
    assert(rec.events.exists {
      case _: SageEvent.CommandCompleted => true
      case _                             => false
    })
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
    assert(Events.trackSpan[String](Events.disabled, Connection.ping(None), cb) eq cb) // the disabled path returns the original callback
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
    var healthy                                         = true // first establish's HELLO succeeds; the reconnect's is rejected, as a rotated password would be
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val respond: Bytes => Seq[Frame] =
        payload => if (healthy) Replies.withSetup(_ => Nil)(payload) else Seq(Frame.SimpleError("WRONGPASS invalid password"))
      val t                            = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    new MultiplexedConnection(
      factory,
      scheduler,
      SageConfig(
        clientCache = CacheConfig(enabled = false),
        reconnect = fixedBackoff,
        watchdog = noWatchdog,
        connectTimeout = 1.second,
        closeTimeout = Duration.Zero
      ),
      MultiplexedConnection.NodeRole.Master,
      node,
      rec
    ).start(): Unit

    healthy = false
    transports.head.close()
    scheduler.advance(1.milli)
    assert(
      rec.events.exists {
        case SageEvent.Connection.ReconnectFailed(`node`, _: sage.SageException.ServerError) => true
        case _                                                                               => false
      },
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
      if (attempt == 1) {
        val t = new FakeTransport(onFrame, onClosed, Replies.withSetup(_ => Nil))
        first.set(t)
        t
      } else
        new Transport {
          def start(): Unit                    = {
            connection.close()
            throw new RuntimeException("aborted by close")
          }
          def send(item: Transport.Item): Unit = ()
          def close(): Unit                    = ()
        }
    }
    connection = new MultiplexedConnection(
      factory,
      scheduler,
      SageConfig(
        clientCache = CacheConfig(enabled = false),
        reconnect = fixedBackoff,
        watchdog = noWatchdog,
        connectTimeout = 1.second,
        closeTimeout = Duration.Zero
      ),
      MultiplexedConnection.NodeRole.Master,
      node,
      rec
    ).start()

    first.get().close()
    scheduler.advance(1.milli)
    assert(
      !rec.events.exists {
        case _: SageEvent.Connection.ReconnectFailed => true
        case _                                       => false
      },
      rec.events
    )
  }

  // --- cache -----------------------------------------------------------------------------------------------------------------------------

  test("a cached read misses then hits") {
    var writes          = 0
    val rec             = new Recording
    val (connection, _) = connect(
      None,
      rec,
      respond = payload =>
        if (payload.asUtf8String.contains("TRACKING")) Seq(Frame.SimpleString("OK"))
        else {
          writes += 1
          // the first write is the [CLIENT CACHING YES, GET] batch: reply OK to the marker, the value to the read
          if (writes == 1) Seq(Frame.SimpleString("OK"), Frame.BulkString(Bytes.utf8("v"))) else Nil
        },
      clientCache = CacheConfig(enabled = true, maxBytes = 1L << 20)
    )
    val get             = Strings.get[String, String]("k")
    for (_ <- 1 to 2)
      connection.cachedSubmit(get, 60000L, (_: scala.util.Try[Option[String]]) => (), Events.fetchTracking(rec))
    val evs             = rec.events
    assertEquals(evs.collect { case c: SageEvent.Cache => c }, Vector(SageEvent.Cache.Miss("GET"), SageEvent.Cache.Hit("GET")))
    // the miss touched the server, so it also produces one CommandCompleted; the hit produces none
    assertEquals(
      evs.collect { case c: SageEvent.CommandCompleted => (c.name, c.node, c.outcome) },
      Vector(("GET", None, sage.Outcome.Succeeded))
    )
  }

  test("the parts of a cached read that start fetching at once start one span") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val starts  = new java.util.concurrent.atomic.AtomicInteger
    val tracer  = new CommandTracer {
      def onCommand(command: sage.commands.Command[?]): sage.CommandSpan = {
        starts.incrementAndGet()
        entered.countDown()
        release.await()
        sage.CommandSpan.noop
      }
    }
    val get     = Strings.get[String, String]("k")
    Events.trackCached[Option[String]](new Recording(Some(tracer)), get, _ => ()) { (_, trace) =>
      def fetch(): Thread = Thread.ofPlatform().start(() => trace.fetching(get, (_: scala.util.Try[Option[String]]) => ()): Unit)
      val first           = fetch()
      entered.await()
      val second          = fetch()
      // the second part either waits for the first to finish starting the span or starts its own and parks in the tracer
      while (!Set(Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TERMINATED).contains(second.getState)) Thread.onSpinWait()
      release.countDown()
      first.join()
      second.join()
    }
    assertEquals(starts.get(), 1)
  }
}

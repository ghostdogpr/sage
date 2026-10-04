package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.concurrent.duration.*

import Replies.bulk

import sage.{Bytes, Message, PatternMessage}
import sage.SageException.{NotConnected, ServerError, TlsError}
import sage.client.{BackoffConfig, PubSubConfig, SageConfig, WatchdogConfig}
import sage.cluster.{ClusterTopology, Node, Slot, SlotRange}
import sage.protocol.Frame

class SubscriptionConnectionSpec extends munit.FunSuite {

  private val fixedBackoff = BackoffConfig(initialDelay = 1.milli, maxDelay = 1.milli, multiplier = 1.0)
  private val noWatchdog   = WatchdogConfig(enabled = false)

  // models the server's replies: the setup, PONG to a PING, server information to the HELLO written after an SUNSUBSCRIBE, and one
  // confirmation push per subscribed or unsubscribed name
  private val serverResponder: Bytes => Seq[Frame] = Replies.withSetup(payload => commands(payload).flatMap(reply))

  private def reply(command: List[String]): Seq[Frame] =
    command match {
      case List("PING")                                   => Seq(Replies.pong)
      case List("HELLO")                                  => Seq(Replies.hello)
      case List(verb, name) if verb.endsWith("SUBSCRIBE") => Seq(Frame.Push(Vector(bulk(verb.toLowerCase), bulk(name), Frame.Integer(1))))
      case _                                              => Nil
    }

  // the commands in a write, each as its arguments
  private def commands(payload: Bytes): List[List[String]] = {
    def parse(tokens: List[String]): List[List[String]] =
      tokens match {
        case head :: rest if head.startsWith("*") =>
          val n = head.tail.toInt
          rest.take(2 * n).grouped(2).map(_(1)).toList :: parse(rest.drop(2 * n))
        case _                                    => Nil
      }
    parse(payload.asUtf8String.split("\r\n").toList)
  }

  // each subscribed name is its own command in the write, acknowledged by one confirmation push
  private def confirmations(kind: String, payload: String): Seq[Frame] = {
    val names = payload.split("\r\n").count(_ == kind.toUpperCase)
    (1 to names).map(i => Frame.Push(Vector(bulk(kind), bulk("?"), Frame.Integer(i.toLong))))
  }

  private def make(
    isLive: () => Boolean = () => true,
    watchdog: WatchdogConfig = noWatchdog,
    bufferSize: Int = 16,
    respond: Bytes => Seq[Frame] = serverResponder,
    reconnect: BackoffConfig = fixedBackoff,
    refuseHello: () => Boolean = () => false
  ): (SubscriptionConnection, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val refused                                         = Seq(Frame.SimpleError("ERR server is down"))
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val reply: Bytes => Seq[Frame] = payload =>
        if (refuseHello() && payload.asUtf8String.contains("\r\nHELLO\r\n")) refused else Replies.withSetup(respond)(payload)
      val transport                  = new FakeTransport(onFrame, onClosed, reply)
      transports += transport
      transport
    }
    val connection                                      = new SubscriptionConnection(
      factory,
      scheduler,
      SageConfig(reconnect = reconnect, watchdog = watchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = bufferSize)),
      isLive
    )
    (connection, scheduler, transports)
  }

  // A SUBSCRIBE token is "\r\nSUBSCRIBE\r\n" on the wire. It cannot match UN/PSUBSCRIBE because those tokens start with UN/P.
  private def wrote(transport: FakeTransport, command: String): Boolean =
    transport.written.exists(_.asUtf8String.contains(s"\r\n$command\r\n"))

  private def message(channel: String, payload: String): Frame =
    Frame.Push(Vector(bulk("message"), bulk(channel), bulk(payload)))

  private def shardMessage(channel: String, payload: String): Frame =
    Frame.Push(Vector(bulk("smessage"), bulk(channel), bulk(payload)))

  private def patternMessage(pattern: String, channel: String, payload: String): Frame =
    Frame.Push(
      Vector(
        bulk("pmessage"),
        bulk(pattern),
        bulk(channel),
        bulk(payload)
      )
    )

  // next is async (callback); block on it for assertions
  private def nextBlocking(sub: SubscriptionConnection.RawSubscription): Option[SubscriptionConnection.Delivery] = {
    val box   = new AtomicReference[Option[SubscriptionConnection.Delivery]]()
    val latch = new CountDownLatch(1)
    sub.next { delivery =>
      box.set(delivery)
      latch.countDown()
    }
    latch.await()
    box.get()
  }

  // Bytes uses reference equality for `==`. Destructure the delivery and compare its payload as text.
  private def assertChannel(delivery: Option[SubscriptionConnection.Delivery], channel: String, payload: String)(using munit.Location): Unit =
    delivery match {
      case Some(Message(ch, p)) =>
        assertEquals(ch, channel)
        assertEquals(p.asUtf8String, payload)
      case other                => fail(s"expected a channel delivery, got $other")
    }

  private def assertPattern(delivery: Option[SubscriptionConnection.Delivery], pattern: String, channel: String, payload: String)(
    using munit.Location
  ): Unit =
    delivery match {
      case Some(PatternMessage(pat, ch, p)) =>
        assertEquals(pat, pattern)
        assertEquals(ch, channel)
        assertEquals(p.asUtf8String, payload)
      case other                            => fail(s"expected a pattern delivery, got $other")
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

  test("a second subscriber to a channel whose SUBSCRIBE is still unconfirmed waits for that confirmation") {
    val held                        = (payload: Bytes) => if (payload.asUtf8String.contains("news")) Nil else serverResponder(payload)
    val (connection, _, transports) = make(respond = held)
    connection.subscribeChannels(Vector("other"))
    def subscribeInThread(): Thread = {
      val thread = new Thread(() => connection.subscribeChannels(Vector("news")): Unit)
      thread.start()
      while (thread.getState != Thread.State.TIMED_WAITING && thread.getState != Thread.State.TERMINATED) Thread.onSpinWait()
      thread
    }
    val first                       = subscribeInThread()
    val second                      = subscribeInThread()
    assertEquals(second.getState, Thread.State.TIMED_WAITING, "the second subscriber returned before the server confirmed the channel")
    transports.head.emit(Frame.Push(Vector(bulk("subscribe"), bulk("news"), Frame.Integer(2L))))
    first.join()
    second.join()
  }

  test("UNSUBSCRIBE is sent only when the last subscriber of a channel closes, and closing the last subscription closes the connection") {
    val (connection, _, transports) = make()
    val a                           = connection.subscribeChannels(Vector("news"))
    val b                           = connection.subscribeChannels(Vector("news"))
    val other                       = connection.subscribeChannels(Vector("other"))

    a.close()
    assert(!wrote(transports.head, "UNSUBSCRIBE"), "another subscriber still wants the channel")
    b.close()
    assert(wrote(transports.head, "UNSUBSCRIBE"), "the last subscriber of the channel leaving unsubscribes")
    assertEquals(transports.head.closeCount, 0)

    other.close()
    assertEquals(transports.head.written.count(_.asUtf8String.contains("\r\nUNSUBSCRIBE\r\n")), 1, "the closing connection needs no UNSUBSCRIBE")
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

  test("a reconnect loop from an earlier loss stops once a later loss starts its own") {
    @volatile var refusing                  = false
    val (connection, scheduler, transports) = make(refuseHello = () => refusing)
    val first                               = connection.subscribeChannels(Vector("news"))
    refusing = true
    transports(0).close() // its loop waits out the backoff
    first.close()
    refusing = false
    connection.subscribeChannels(Vector("news"))
    refusing = true
    transports(1).close()
    scheduler.advance(1.milli)
    assertEquals(transports.size, 3, "only the latest loss's loop may attempt a reconnect")
  }

  test("a connection that keeps dropping right after going live backs off further on each loss") {
    val (connection, scheduler, transports) = make(reconnect = BackoffConfig(initialDelay = 10.millis, maxDelay = 1.second, multiplier = 2.0))
    connection.subscribeChannels(Vector("news"))
    transports(0).close()
    scheduler.advance(10.millis)
    assertEquals(transports.size, 2, "the first loss retries after the initial delay")
    transports(1).close()
    scheduler.advance(10.millis)
    assertEquals(transports.size, 2, "the second loss must wait longer than the first")
    scheduler.advance(10.millis)
    assertEquals(transports.size, 3)
  }

  test("a subscribe the server rejects fails with its error and keeps the connection for other subscribers") {
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val respond: Bytes => Seq[Frame]                    = payload =>
      if (payload.asUtf8String.contains("secret")) Seq(Frame.SimpleError("NOPERM no permissions to access the 'secret' channel"))
      else serverResponder(payload)
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    // a real clock, so a subscribe that never confirms gives up after connectTimeout
    val connection                                      = new SubscriptionConnection(
      factory,
      Scheduler.real,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 200.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true
    )
    val news                                            = connection.subscribeChannels(Vector("news"))
    val error                                           = intercept[ServerError](connection.subscribeChannels(Vector("secret")))
    assertEquals(error.code, "NOPERM")
    assertEquals((transports.size, transports.head.closeCount), (1, 0))
    transports.head.emit(message("news", "still here"))
    assertChannel(nextBlocking(news), "news", "still here")
  }

  test("a name the server rejected can be subscribed again once the server accepts it") {
    @volatile var denied                                = true
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val respond: Bytes => Seq[Frame]                    = payload =>
      if (denied && payload.asUtf8String.contains("secret")) Seq(Frame.SimpleError("NOPERM no permissions to access the 'secret' channel"))
      else serverResponder(payload)
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    val connection                                      = new SubscriptionConnection(
      factory,
      Scheduler.real,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1.second, pubsub = PubSubConfig(bufferSize = 16)),
      () => true
    )
    connection.subscribeChannels(Vector("news"))
    intercept[ServerError](connection.subscribeChannels(Vector("secret")))
    denied = false
    val secret                                          = connection.subscribeChannels(Vector("secret"))
    transports.head.emit(message("secret", "granted"))
    assertChannel(nextBlocking(secret), "secret", "granted")
  }

  test("a subscribe whose SUBSCRIBE is dropped unwritten is not reported as confirmed") {
    final class DroppingSubscribe(onFrame: Frame => Unit) extends Transport {
      var closeCount                       = 0
      def start(): Unit                    = ()
      // a terminating socket drops a queued write without writing it; onClosed comes later
      def send(item: Transport.Item): Unit =
        if (item.payload.asUtf8String.contains("\r\nSUBSCRIBE\r\n")) item.dropped()
        else {
          item.writeAttempted()
          serverResponder(item.payload).foreach(onFrame)
        }
      def close(): Unit                    = closeCount += 1
    }
    val transports = mutable.ArrayBuffer.empty[DroppingSubscribe]
    val connection = new SubscriptionConnection(
      (onFrame, _) => {
        val t = new DroppingSubscribe(onFrame)
        transports += t
        t
      },
      Scheduler.real,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 200.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true
    )
    intercept[NotConnected](connection.subscribeChannels(Vector("news")))
    assertEquals(transports.map(_.closeCount), mutable.ArrayBuffer(1))
    assert(connection.isEmpty)
  }

  test("a name the server rejects when a reconnect restores it ends its subscription with the server's error") {
    @volatile var denied                    = false
    val respond: Bytes => Seq[Frame]        = payload =>
      if (denied && payload.asUtf8String.contains("secret")) Seq(Frame.SimpleError("NOPERM no permissions to access the 'secret' channel"))
      else serverResponder(payload)
    val (connection, scheduler, transports) = make(respond = respond)
    val secret                              = connection.subscribeChannels(Vector("secret"))
    denied = true
    transports(0).close()
    scheduler.advance(1.milli)
    val outcome                             = new AtomicReference[Option[SubscriptionConnection.Delivery]]()
    secret.next(outcome.set)
    (outcome.get(), secret.failure) match {
      case (None, Some(error: ServerError)) => assertEquals(error.code, "NOPERM")
      case other                            => fail(s"the stream must end with the server's error instead of staying open and silent, got $other")
    }
  }

  test("a name a busy server refuses while a reconnect restores it is restored by a later reconnect") {
    @volatile var busy                      = false
    val respond: Bytes => Seq[Frame]        = payload =>
      if (busy && payload.asUtf8String.contains("\r\nSUBSCRIBE\r\n")) Seq(Frame.SimpleError("BUSY Redis is busy running a script"))
      else serverResponder(payload)
    val (connection, scheduler, transports) = make(respond = respond)
    val news                                = connection.subscribeChannels(Vector("news"))
    busy = true
    transports(0).close()
    scheduler.advance(1.milli)
    assertEquals((transports.size, transports(1).closeCount), (2, 1))
    busy = false
    scheduler.advance(1.milli)
    transports(2).emit(message("news", "after-busy"))
    val outcome                             = new AtomicReference[Option[SubscriptionConnection.Delivery]]()
    news.next(outcome.set)
    assertChannel(outcome.get(), "news", "after-busy")
  }

  test("a shard channel a busy owner refuses is placed again instead of ending its subscription") {
    val respond: Bytes => Seq[Frame]                    = Replies.withSetup(_ => Seq(Frame.SimpleError("BUSY Redis is busy running a script")))
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => new FakeTransport(onFrame, onClosed, respond)
    var moved                                           = 0
    val connection                                      = new SubscriptionConnection(
      factory,
      new ManualScheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Report(_ => (), () => (), () => moved += 1)
    )
    val sink                                            = new SubscriptionConnection.Sink(Vector("x"), SubscriptionConnection.Kind.Shard, 16)
    connection.attach(sink, sink.names)
    assertEquals((moved, sink.failure, connection.namesOf(sink)), (1, None, Vector.empty[String]))
  }

  test("closing a subscription wakes its parked consumer with None and tears the socket down, even when its UNSUBSCRIBE is dropped") {
    final class DroppingUnsubscribe(onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {
      var closeCount                       = 0
      var droppedCount                     = 0
      def start(): Unit                    = ()
      def send(item: Transport.Item): Unit =
        if (item.payload.asUtf8String.contains("\r\nUNSUBSCRIBE\r\n")) {
          droppedCount += 1
          item.dropped()
        } else {
          item.writeAttempted()
          Replies.withSetup(serverResponder)(item.payload).foreach(onFrame)
        }
      def close(): Unit                    = {
        closeCount += 1
        if (closeCount == 1) onClosed()
      }
    }
    val transports = mutable.ArrayBuffer.empty[DroppingUnsubscribe]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new DroppingUnsubscribe(onFrame, onClosed)
      transports += t
      t
    }
    val connection                                      = new SubscriptionConnection(
      factory,
      new ManualScheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true
    )
    val sub                                             = connection.subscribeChannels(Vector("news"))
    val other                                           = connection.subscribeChannels(Vector("other"))
    val woken                                           = new AtomicReference[Option[SubscriptionConnection.Delivery]]()
    sub.next(woken.set)
    sub.close()
    assertEquals(woken.get(), None)
    assertEquals(transports.head.droppedCount, 1)
    other.close()
    assertEquals(transports.head.closeCount, 1)
    assert(connection.isEmpty)
  }

  test("a failed first establish deregisters the subscription and closes its socket") {
    val (connection, _, transports) = make(refuseHello = () => true)
    intercept[sage.SageException](connection.subscribeChannels(Vector("news")))
    assertEquals(transports.map(_.closeCount), mutable.ArrayBuffer(1))
    assert(connection.isEmpty, "the failed subscription must not be restored by a later connection")
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
      if (s.contains("\r\nSUBSCRIBE\r\n")) confirmations("subscribe", s)
      else Nil
    }
    val watchdog                            = WatchdogConfig(pingInterval = 100.millis, pingTimeout = 50.millis, enabled = true)
    val (connection, scheduler, transports) = make(watchdog = watchdog, respond = silentToPing)
    val sub                                 = connection.subscribeChannels(Vector("news"))
    assertEquals(transports.size, 1)

    scheduler.advance(250.millis)
    assertEquals(transports.size, 2, "the killed connection reconnects to a fresh transport")
    assertEquals(transports.head.closeCount, 1, "the unresponsive original connection was killed by the watchdog")

    // one watchdog tick on the fresh generation, which only sends its own probe
    scheduler.advance(140.millis)
    assert(wrote(transports(1), "PING"), "the fresh generation's watchdog ticked")
    assertEquals(transports(1).closeCount, 0, "the fresh generation must not be killed by a ping left outstanding on the dead one")
    assertEquals(transports.size, 2, "no further reconnect churn")

    transports(1).emit(message("news", "after-reconnect"))
    assertChannel(nextBlocking(sub), "news", "after-reconnect")
  }

  test("a steady stream of message pushes does not suppress the keepalive PING (push proves only the read path)") {
    val silentToPing: Bytes => Seq[Frame]   = { payload =>
      val s = payload.asUtf8String
      if (s.contains("\r\nSUBSCRIBE\r\n")) confirmations("subscribe", s)
      else Nil // do not answer the probe; the watchdog eventually closes the connection after the keepalive times out
    }
    val watchdog                            = WatchdogConfig(pingInterval = 100.millis, pingTimeout = 50.millis, enabled = true)
    val (connection, scheduler, transports) = make(watchdog = watchdog, respond = silentToPing)
    connection.subscribeChannels(Vector("news"))
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

  test("the watchdog of a pub/sub-only user, whom the server refuses PING, keeps the connection") {
    val respond: Bytes => Seq[Frame]        = payload =>
      commands(payload).flatMap {
        case List("PING")            => Seq(Frame.SimpleError("NOPERM User u has no permissions to run the 'ping' command"))
        case List("SUBSCRIBE", name) => Seq(Frame.Push(Vector(bulk("subscribe"), bulk(name), Frame.Integer(1))))
        case _                       => Nil
      }
    val watchdog                            = WatchdogConfig(pingInterval = 100.millis, pingTimeout = 50.millis, enabled = true)
    val (connection, scheduler, transports) = make(watchdog = watchdog, respond = respond)
    connection.subscribeChannels(Vector("news"))
    scheduler.advance(350.millis)
    assertEquals((transports.size, transports.head.closeCount), (1, 0))
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
    connection.attach(sink, Vector("orders"))

    assert(!connection.closeIfEmpty(), "a connection carrying a live sink must not be evicted")
    assertEquals(transports.head.closeCount, 0)

    connection.detach(sink, Vector("orders"))
    assert(connection.closeIfEmpty(), "with its last sink gone the connection is empty and closes")
    assertEquals(transports.head.closeCount, 1)

    val late = new SubscriptionConnection.Sink(Vector("late"), SubscriptionConnection.Kind.Shard, 16)
    intercept[NotConnected](connection.attach(late, Vector("late")))
  }

  test("an error reply with nothing pending drops the connection instead of being swallowed as a PONG") {
    var terminated                                      = false
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, serverResponder)
      transports += t
      t
    }
    val connection                                      = new SubscriptionConnection(
      factory,
      scheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Report(_ => terminated = true, () => (), () => ())
    )
    val sink                                            = new SubscriptionConnection.Sink(Vector("orders"), SubscriptionConnection.Kind.Shard, 16)
    connection.attach(sink, Vector("orders"))
    assertEquals(transports.size, 1)

    transports.head.emit(Frame.SimpleError("MOVED 1234 10.0.0.2:6379"))
    assert(terminated, "a MOVED error on the subscribe connection must drop it so the manager re-homes")
    assertEquals(transports.head.closeCount, 1)
  }

  test("the confirmation of our own SUNSUBSCRIBE leaves a channel subscribed again meanwhile in place") {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    // the reader sees the SUNSUBSCRIBE reply only once the next SSUBSCRIBE is written, and in write order, before its confirmation
    var unanswered                                      = Seq.empty[Frame]
    val respond: Bytes => Seq[Frame]                    = payload =>
      if (payload.asUtf8String.contains("\r\nSUNSUBSCRIBE\r\n")) {
        unanswered = Seq(Frame.Push(Vector(bulk("sunsubscribe"), bulk("x"), Frame.Integer(1))), Replies.hello); Nil
      } else { val replies = unanswered ++ serverResponder(payload); unanswered = Nil; replies }
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    var dropped                                         = 0
    val connection                                      = new SubscriptionConnection(
      factory,
      scheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Report(_ => (), () => dropped += 1, () => ())
    )
    def shardSink(name: String)                         = new SubscriptionConnection.Sink(Vector(name), SubscriptionConnection.Kind.Shard, 16)
    val (first, other, second)                          = (shardSink("x"), shardSink("y"), shardSink("x"))
    connection.attach(first, Vector("x"))
    connection.attach(other, Vector("y"))
    connection.detach(first, Vector("x"))
    connection.attach(second, Vector("x"))

    assertEquals(dropped, 0)
    transports.head.emit(shardMessage("x", "hello"))
    assertChannel(nextBlocking(new SubscriptionConnection.RawSubscription(second, () => ())), "x", "hello")
  }

  test("a shard channel dropped while another channel's SSUBSCRIBE is unanswered is placed again, and the SSUBSCRIBE keeps its own reply") {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    // x's slot moves away just before the server reads SSUBSCRIBE y
    val respond: Bytes => Seq[Frame]                    = payload =>
      if (payload.asUtf8String.contains("\r\ny\r\n"))
        Seq(
          Frame.Push(Vector(bulk("sunsubscribe"), bulk("x"), Frame.Integer(0))),
          Frame.Push(Vector(bulk("ssubscribe"), bulk("y"), Frame.Integer(1)))
        )
      else serverResponder(payload)
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    var (terminated, dropped)                           = (false, 0)
    val connection                                      = new SubscriptionConnection(
      factory,
      scheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Report(_ => terminated = true, () => dropped += 1, () => ())
    )
    def shardSink(name: String)                         = new SubscriptionConnection.Sink(Vector(name), SubscriptionConnection.Kind.Shard, 16)
    val (x, y)                                          = (shardSink("x"), shardSink("y"))
    connection.attach(x, x.names)
    connection.attach(y, y.names)

    assertEquals((terminated, dropped, transports.head.closeCount, connection.namesOf(x)), (false, 1, 0, Vector.empty[String]))
    transports.head.emit(shardMessage("y", "hello"))
    assertChannel(nextBlocking(new SubscriptionConnection.RawSubscription(y, () => ())), "y", "hello")
  }

  test("SUNSUBSCRIBE errors like Valkey's leave the shard connection and its other subscribers in place") {
    // like Valkey 9.1: a SUNSUBSCRIBE naming channels in two slots gets CROSSSLOT, and one for a slot the node no longer serves gets MOVED,
    // after the push that drops the channel when its slot moved while the SUNSUBSCRIBE was on its way
    val redirect                                        = Frame.SimpleError("MOVED 1234 10.0.0.2:6379")
    val respond: Bytes => Seq[Frame]                    = Replies.withSetup { payload =>
      commands(payload).flatMap {
        case List("SSUBSCRIBE", name)         => Seq(Frame.Push(Vector(bulk("ssubscribe"), bulk(name), Frame.Integer(1))))
        case List("SUNSUBSCRIBE", "moved")    => Seq(redirect)
        case List("SUNSUBSCRIBE", "migrated") => Seq(Frame.Push(Vector(bulk("sunsubscribe"), bulk("migrated"), Frame.Integer(0))), redirect)
        case List("SUNSUBSCRIBE", name)       => Seq(Frame.Push(Vector(bulk("sunsubscribe"), bulk(name), Frame.Integer(0))))
        case "SUNSUBSCRIBE" :: _              => Seq(Frame.SimpleError("CROSSSLOT Keys in request don't hash to the same slot"))
        case List("HELLO")                    => Seq(Replies.hello)
        case _                                => Nil
      }
    }
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    var (terminated, moved)                             = (false, 0)
    val connection                                      = new SubscriptionConnection(
      factory,
      scheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Report(_ => terminated = true, () => (), () => moved += 1)
    )
    def shardSink(names: String*)                       = new SubscriptionConnection.Sink(names.toVector, SubscriptionConnection.Kind.Shard, 16)
    val (multi, unowned, migrated, other)               = (shardSink("x", "y"), shardSink("moved"), shardSink("migrated"), shardSink("z"))
    Seq(multi, unowned, migrated, other).foreach(sink => connection.attach(sink, sink.names))
    Seq(multi, unowned, migrated).foreach(sink => connection.detach(sink, sink.names))

    assert(!terminated, "an error reply to an unsubscribe must not end the shared shard connection")
    assertEquals((transports.size, transports.head.closeCount, moved), (1, 0, 0))
    transports.head.emit(shardMessage("z", "still here"))
    assertChannel(nextBlocking(new SubscriptionConnection.RawSubscription(other, () => ())), "z", "still here")
    val late = shardSink("w")
    connection.attach(late, late.names)
    assertEquals(connection.namesOf(late), Vector("w"))
  }

  test("detaching many shard channels writes one HELLO, and later replies still match their commands") {
    val respond: Bytes => Seq[Frame]                    = Replies.withSetup { payload =>
      commands(payload).flatMap {
        case List("SUNSUBSCRIBE", "c5") => Seq(Frame.SimpleError("MOVED 1234 10.0.0.2:6379"))
        case command                    => reply(command)
      }
    }
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    var terminated                                      = false
    val connection                                      = new SubscriptionConnection(
      factory,
      new ManualScheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Report(_ => terminated = true, () => (), () => ())
    )
    val names                                           = Vector.tabulate(1000)(i => s"c$i")
    val (many, kept, late)                              =
      (
        new SubscriptionConnection.Sink(names, SubscriptionConnection.Kind.Shard, 16),
        new SubscriptionConnection.Sink(Vector("kept"), SubscriptionConnection.Kind.Shard, 16),
        new SubscriptionConnection.Sink(Vector("late"), SubscriptionConnection.Kind.Shard, 16)
      )
    Seq(many, kept).foreach(sink => connection.attach(sink, sink.names))
    connection.detach(many, names)

    val hellos = transports.head.sent.map(_.asUtf8String.split("\r\n").count(_ == "HELLO")).sum
    assertEquals((hellos, terminated, transports.head.closeCount), (1, false, 0))
    connection.attach(late, late.names)
    assertEquals(connection.namesOf(late), Vector("late"))
    transports.head.emit(shardMessage("kept", "still here"))
    assertChannel(nextBlocking(new SubscriptionConnection.RawSubscription(kept, () => ())), "kept", "still here")
  }

  test("an unsubscribe settles only its own replies for a pub/sub-only user, whom the server refuses PING") {
    // like a user granted only +@pubsub: PING gets NOPERM, while (UN)SUBSCRIBE succeed
    val respond: Bytes => Seq[Frame] = payload =>
      commands(payload).flatMap {
        case List("PING")                                     => Seq(Frame.SimpleError("NOPERM User u has no permissions to run the 'ping' command"))
        case List(verb @ ("SUBSCRIBE" | "UNSUBSCRIBE"), name) => Seq(Frame.Push(Vector(bulk(verb.toLowerCase), bulk(name), Frame.Integer(1))))
        case _                                                => Nil
      }
    val (connection, _, transports)  = make(respond = respond)
    val kept                         = connection.subscribeChannels(Vector("kept"))
    connection.subscribeChannels(Vector("gone")).close()
    val late                         = new AtomicReference[SubscriptionConnection.RawSubscription]()
    val subscribing                  = new Thread(() =>
      try late.set(connection.subscribeChannels(Vector("late")))
      catch { case _: Throwable => () }
    )
    subscribing.start()
    subscribing.join(2000)
    if (subscribing.isAlive) {
      connection.close()
      subscribing.join()
      fail("the late subscribe's confirmation answered another command")
    }

    assert(late.get() != null, "the late subscribe failed")
    assertEquals(transports.head.closeCount, 0)
    transports.head.emit(message("late", "hello"))
    assertChannel(nextBlocking(late.get()), "late", "hello")
    kept.close()
  }

  // Like the ACL user `+subscribe +unsubscribe +psubscribe +punsubscribe +ssubscribe +sunsubscribe +ping +hello +client allchannels`: every
  // other command gets NOPERM, and `denials` counts them.
  final private class LeastPrivilege {
    @volatile var denials            = 0
    val respond: Bytes => Seq[Frame] = payload =>
      commands(payload).flatMap {
        case command @ (verb :: _) if verb.endsWith("SUBSCRIBE") || verb == "PING" || verb == "HELLO" => reply(command)
        case verb :: _                                                                                =>
          denials += 1
          Seq(Frame.SimpleError(s"NOPERM User u has no permissions to run the '${verb.toLowerCase}' command"))
        case Nil                                                                                      => Nil
      }
  }

  test("unsubscribing keeps the connection of a user granted only pub/sub, PING, HELLO and CLIENT, and later replies match") {
    val user                        = new LeastPrivilege
    val (connection, _, transports) = make(respond = user.respond)
    val kept                        = connection.subscribeChannels(Vector("kept"))
    val gone                        =
      Vector(connection.subscribeChannels(Vector("gone")), connection.subscribePatterns(Vector("gone.*")), connection.subscribeShard(Vector("gone")))
    gone.foreach(_.close())
    assertEquals((transports.size, transports.head.closeCount, user.denials), (1, 0, 0))

    val late = connection.subscribeChannels(Vector("late"))
    transports.head.emit(message("late", "hello"))
    assertChannel(nextBlocking(late), "late", "hello")
    transports.head.emit(message("kept", "still here"))
    assertChannel(nextBlocking(kept), "kept", "still here")
  }

  test("an SUNSUBSCRIBE keeps the cluster shard connection of a user granted only pub/sub, PING, HELLO and CLIENT") {
    val user                                            = new LeastPrivilege
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, Replies.withSetup(user.respond))
      transports += t
      t
    }
    var terminated                                      = false
    val connection                                      = new SubscriptionConnection(
      factory,
      new ManualScheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Report(_ => terminated = true, () => (), () => ())
    )
    def shardSink(name: String)                         = new SubscriptionConnection.Sink(Vector(name), SubscriptionConnection.Kind.Shard, 16)
    val (gone, kept, late)                              = (shardSink("gone"), shardSink("kept"), shardSink("late"))
    Seq(gone, kept).foreach(sink => connection.attach(sink, sink.names))
    connection.detach(gone, gone.names)
    assertEquals((terminated, transports.size, transports.head.closeCount, user.denials), (false, 1, 0, 0))

    connection.attach(late, late.names)
    assertEquals(connection.namesOf(late), Vector("late"))
    transports.head.emit(shardMessage("kept", "still here"))
    assertChannel(nextBlocking(new SubscriptionConnection.RawSubscription(kept, () => ())), "kept", "still here")
  }

  test("the watchdog of a user granted only pub/sub, PING, HELLO and CLIENT probes with PING, which the ACL allows") {
    val user                                = new LeastPrivilege
    val watchdog                            = WatchdogConfig(pingInterval = 100.millis, pingTimeout = 50.millis, enabled = true)
    val (connection, scheduler, transports) = make(watchdog = watchdog, respond = user.respond)
    connection.subscribeChannels(Vector("news"))
    scheduler.advance(350.millis)
    assert(wrote(transports.head, "PING"), "the watchdog never probed")
    assertEquals((transports.size, transports.head.closeCount, user.denials), (1, 0, 0))
  }

  test("a socket dying in the establish->live window fires onTerminated in cluster mode instead of going silently Live") {
    final class DyingAfterBootstrap(onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {
      def start(): Unit                    = ()
      // answers the setup, then drops the connection right after the last setup command
      def send(item: Transport.Item): Unit = {
        item.writeAttempted()
        serverResponder(item.payload).foreach(onFrame)
        if (item.payload.asUtf8String.contains("LIB-VER")) onClosed()
      }
      def close(): Unit                    = ()
    }
    var terminatedCalled = false
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => new DyingAfterBootstrap(onFrame, onClosed)
    val connection                                      = new SubscriptionConnection(
      factory,
      scheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Report(_ => terminatedCalled = true, () => (), () => ())
    )

    val sink = new SubscriptionConnection.Sink(Vector("news"), SubscriptionConnection.Kind.Channel, 16)
    connection.attach(sink, Vector("news"))

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
    sink.next { delivery =>
      box.set(delivery)
      latch.countDown()
    }
    sink.offer(Message("news", Bytes.utf8("hello")))
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

  test("a connect failure on the subscribe path surfaces as a modeled ConnectionFailed, not a raw defect") {
    val factory: MultiplexedConnection.TransportFactory = (_, _) =>
      new Transport {
        def start(): Unit                    = throw new java.net.ConnectException("connection refused")
        def send(item: Transport.Item): Unit = ()
        def close(): Unit                    = ()
      }
    val connection                                      =
      new SubscriptionConnection(
        factory,
        new ManualScheduler,
        SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
        () => true
      )

    intercept[sage.SageException.ConnectionFailed](connection.subscribeChannels(Vector("news")))
  }

  test("a failed subscription reconnect emits ReconnectFailed instead of retrying in silence") {
    val recorded                                        = new java.util.concurrent.ConcurrentLinkedQueue[sage.SageEvent]()
    val events                                          = new Events {
      def enabled: Boolean                      = true
      def emitsEvents: Boolean                  = true
      def tracer: Option[sage.CommandTracer]    = None
      def serverNode: Option[sage.cluster.Node] = None
      def emit(event: sage.SageEvent): Unit     = recorded.add(event): Unit
      def close(): Unit                         = ()
    }
    var healthy                                         = true
    val respond: Bytes => Seq[Frame]                    = payload =>
      if (!healthy && payload.asUtf8String.contains("\r\nHELLO\r\n")) Seq(Frame.SimpleError("WRONGPASS invalid password"))
      else serverResponder(payload)
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, respond)
      transports += t
      t
    }
    val scheduler                                       = new ManualScheduler
    val connection                                      =
      new SubscriptionConnection(
        factory,
        scheduler,
        SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
        () => true,
        SubscriptionConnection.OnLoss.Reconnect(() => (), events)
      )

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

  test("a failed master-replica subscription reconnect names the node it tried, or none when there is no master") {
    val recorded                                                    = new java.util.concurrent.ConcurrentLinkedQueue[sage.SageEvent]()
    val events                                                      = new Events {
      def enabled: Boolean                      = true
      def emitsEvents: Boolean                  = true
      def tracer: Option[sage.CommandTracer]    = None
      def serverNode: Option[sage.cluster.Node] = None
      def emit(event: sage.SageEvent): Unit     = recorded.add(event): Unit
      def close(): Unit                         = ()
    }
    val (first, second)                                             = (Node("first", 1), Node("second", 2))
    @volatile var master                                            = Option(first)
    val transports                                                  = mutable.ArrayBuffer.empty[FakeTransport]
    val nodeFactory: Node => MultiplexedConnection.TransportFactory = node =>
      (onFrame, onClosed) => {
        if (node == second) throw new java.net.ConnectException("connection refused")
        val t = new FakeTransport(onFrame, onClosed, serverResponder)
        transports += t
        t
      }
    val following                                                   = new SubscriptionConnection.Following(nodeFactory, () => master)
    val scheduler                                                   = new ManualScheduler
    val connection                                                  = new SubscriptionConnection(
      following.factory,
      scheduler,
      SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
      () => true,
      SubscriptionConnection.OnLoss.Reconnect(() => (), events, () => following.node)
    )
    def failedOn(): Vector[Option[Node]]                            =
      recorded.toArray.toVector.collect { case sage.SageEvent.Connection.ReconnectFailed(node, _) => node }

    connection.subscribeChannels(Vector("news"))
    master = Some(second)
    transports.head.close()
    scheduler.advance(1.milli)
    assertEquals(failedOn(), Vector(Some(second)))
    master = None
    scheduler.advance(1.milli)
    assertEquals(failedOn(), Vector(Some(second), None))
    connection.close()
  }

  test("close aborts a subscription connection still blocked in the connect (start) phase") {
    val connecting                                      = new AtomicReference[ConnectingTransport]()
    val factory: MultiplexedConnection.TransportFactory = (_, onClosed) => {
      val transport = new ConnectingTransport(onClosed)
      connecting.set(transport)
      transport
    }
    val connection                                      =
      new SubscriptionConnection(
        factory,
        new ManualScheduler,
        SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 1000.millis, pubsub = PubSubConfig(bufferSize = 16)),
        () => true
      )

    val subscribing = new Thread(() =>
      try connection.subscribeChannels(Vector("news")): Unit
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
    val greeted                                         = new CountDownLatch(1)
    val factory: MultiplexedConnection.TransportFactory =
      (onFrame, onClosed) =>
        new FakeTransport(
          onFrame,
          onClosed,
          payload => {
            if (payload.asUtf8String.contains("HELLO")) greeted.countDown()
            Nil
          }
        )
    val connection                                      =
      new SubscriptionConnection(
        factory,
        new ManualScheduler,
        SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 60000.millis, pubsub = PubSubConfig(bufferSize = 16)),
        () => true
      )

    val subscribing = new Thread(() =>
      try connection.subscribeChannels(Vector("news")): Unit
      catch { case _: Throwable => () }
    )
    subscribing.start()
    assert(greeted.await(2, TimeUnit.SECONDS), "the bootstrap HELLO was never sent")

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
      if (attempt.getAndIncrement() == 0) {
        fake = new FakeTransport(onFrame, onClosed, serverResponder)
        fake
      } else {
        val t = new ConnectingTransport(onClosed)
        connecting.add(t)
        t
      }
    val connection                                      =
      new SubscriptionConnection(
        factory,
        scheduler,
        SageConfig(reconnect = fixedBackoff, watchdog = noWatchdog, connectTimeout = 5000.millis, pubsub = PubSubConfig(bufferSize = 16)),
        () => true
      )

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
      try connection.subscribeChannels(Vector("b")): Unit
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

  test("a shard connection that reports its loss late does not unregister the connection that replaced it") {
    val (n, m, mid)                                             = (Node("n", 1), Node("m", 2), Slot.Count / 2)
    def channelOn(node: Node)                                   = Iterator.from(0).map(i => s"c$i").find(c => (Slot.of(Bytes.utf8(c)).value < mid) == (node == n)).get
    def owned(from: Int, to: Int, node: Node)                   = SlotRange(Slot.at(from).get, Slot.at(to).get, node, Vector.empty)
    val topology                                                = ClusterTopology.from(Vector(owned(0, mid - 1, n), owned(mid, Slot.Count - 1, m)))
    val transports                                              = mutable.Map.empty[Node, mutable.ArrayBuffer[FakeTransport]]
    var whileOpeningM: () => Unit                               = () => ()
    val factory: Node => MultiplexedConnection.TransportFactory = node => {
      if (node == m) whileOpeningM() // runs while the manager holds its lock
      (onFrame, onClosed) => {
        val transport = new FakeTransport(onFrame, onClosed, serverResponder)
        transports.getOrElseUpdate(node, mutable.ArrayBuffer.empty) += transport
        transport
      }
    }
    val idle                                                    = new Scheduler {
      def nowMillis: Long                                                      = 0L
      def jitterMillis(boundExclusive: Long): Long                             = 0L
      def after(delay: FiniteDuration)(task: => Unit): Unit                    = ()
      def every(interval: FiniteDuration)(task: => Unit): Scheduler.Cancelable = () => ()
    }
    val manager                                                 =
      new ClusterSubscriptions(factory, idle, SageConfig(watchdog = noWatchdog), () => topology, () => (), () => Some(n), Events.disabled)
    val first                                                   = manager.subscribeShard(Vector(channelOn(n)))
    whileOpeningM = () => {
      whileOpeningM = () => ()
      // the first connection dies, and its report waits for the manager lock that this thread holds
      val dying = new Thread(() => transports(n).head.close())
      dying.start()
      while (dying.getState != Thread.State.WAITING) Thread.onSpinWait()
      first.close() // evicts the dead connection, which is now empty
      manager.subscribeShard(Vector(channelOn(n))): Unit
    }
    manager.subscribeShard(Vector(channelOn(m)))
    manager.close()
    assertEquals(transports(n).map(_.closeCount).toVector, Vector(1, 1), "close must reach the replacement connection")
  }

  test("a shard owner whose connection cannot be created, as with unusable trust material, is retried instead of failing the subscribe") {
    val node                                                    = Node("n", 1)
    val topology                                                = ClusterTopology.from(Vector(SlotRange(Slot.at(0).get, Slot.at(Slot.Count - 1).get, node, Vector.empty)))
    @volatile var unusable                                      = true
    val transports                                              = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: Node => MultiplexedConnection.TransportFactory = _ =>
      if (unusable) throw TlsError("unusable TLS trust material")
      else
        (onFrame, onClosed) => {
          val transport = new FakeTransport(onFrame, onClosed, serverResponder)
          transports += transport
          transport
        }
    val scheduler                                               = new ManualScheduler
    val manager                                                 =
      new ClusterSubscriptions(factory, scheduler, SageConfig(watchdog = noWatchdog), () => topology, () => (), () => Some(node), Events.disabled)
    manager.subscribeShard(Vector("orders"))
    unusable = false
    scheduler.advance(1.second)
    assert(transports.exists(wrote(_, "SSUBSCRIBE")), "the retry never placed the channel")
    manager.close()
  }
}

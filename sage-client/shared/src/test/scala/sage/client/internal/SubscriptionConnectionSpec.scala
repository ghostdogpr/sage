package sage.client.internal

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.concurrent.duration.*

import sage.Bytes
import sage.SageException.NotConnected
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.commands.Connection
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
    respond: Bytes => Seq[Frame] = serverResponder
  ): (SubscriptionConnection, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, respond, autoWrite = true)
      transports += transport
      transport
    }
    val connection                                      = new SubscriptionConnection(factory, Vector(Connection.ping()), scheduler, fixedBackoff, watchdog, 1000L, bufferSize, isLive)
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

  test("subscribing to multiple channels in one call delivers messages from any of them") {
    val (connection, _, transports) = make()
    val sub                         = connection.subscribeChannels(Vector("a", "b"))
    transports.head.emit(message("b", "from-b"))
    assertChannel(nextBlocking(sub), "b", "from-b")
    transports.head.emit(message("a", "from-a"))
    assertChannel(nextBlocking(sub), "a", "from-a")
  }
}

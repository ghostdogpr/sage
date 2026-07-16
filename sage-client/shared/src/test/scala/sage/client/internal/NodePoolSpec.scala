package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference, AtomicReferenceArray}

import scala.concurrent.duration.*
import scala.util.Try

import sage.Bytes
import sage.SageException.NotConnected
import sage.client.{BackoffConfig, DedicatedPoolConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.Connection
import sage.protocol.Frame

class NodePoolSpec extends munit.FunSuite {

  private val helloReply: Frame =
    Frame.Map(
      Vector(
        Frame.BulkString(Bytes.utf8("server"))  -> Frame.BulkString(Bytes.utf8("redis")),
        Frame.BulkString(Bytes.utf8("version")) -> Frame.BulkString(Bytes.utf8("8.0.0")),
        Frame.BulkString(Bytes.utf8("proto"))   -> Frame.Integer(3),
        Frame.BulkString(Bytes.utf8("role"))    -> Frame.BulkString(Bytes.utf8("master"))
      )
    )

  private def respond(payload: Bytes): Seq[Frame] =
    if (payload.asUtf8String.contains("HELLO")) Seq(helloReply) else Nil

  private def newPool(factory: Node => MultiplexedConnection.TransportFactory): NodePool =
    new NodePool(
      factory,
      Scheduler.real,
      Vector(Connection.hello(None)),
      BackoffConfig(),
      WatchdogConfig(enabled = false),
      1.second,
      Duration.Zero,
      DedicatedPoolConfig()
    )

  // gates the nth connect attempt: it signals `reached(n)`, blocks until `release(n)`, then opens a transport captured at `transport(n)`
  final private class GatedFactory(count: Int) {
    val reached                                                 = Vector.fill(count)(new CountDownLatch(1))
    private val gate                                            = Vector.fill(count)(new CountDownLatch(1))
    private val opened                                          = new AtomicReferenceArray[FakeTransport](count)
    private val attempt                                         = new AtomicInteger(0)
    val factory: Node => MultiplexedConnection.TransportFactory = _ =>
      (onFrame, onClosed) => {
        val i = attempt.getAndIncrement()
        reached(i).countDown()
        gate(i).await()
        val t = new FakeTransport(onFrame, onClosed, respond)
        opened.set(i, t)
        t
      }
    def awaitReached(i: Int): Unit                              = assert(reached(i).await(2, TimeUnit.SECONDS), s"attempt $i never reached connect")
    def release(i: Int): Unit                                   = gate(i).countDown()
    def transport(i: Int): FakeTransport                        = opened.get(i)
  }

  private def awaitTrue(cond: => Boolean, clue: String): Unit = {
    val deadline = System.nanoTime() + 2.seconds.toNanos
    while (!cond && System.nanoTime() < deadline) Thread.sleep(10)
    assert(cond, clue)
  }

  // every waiter parks on the single-flight latch (or, briefly, the pool lock); once all are WAITING at once none holds the lock, so each
  // must be blocked on the establishment, not still racing toward it
  private def awaitAllWaiting(threads: Seq[Thread]): Unit =
    awaitTrue(threads.forall(_.getState == Thread.State.WAITING), "a waiter never blocked on the in-flight establishment")

  test("retain invalidates an in-flight establishment for a rejected node: the client is closed, absent, and every waiter fails") {
    val node  = Node("gated", 6379)
    val gated = new GatedFactory(1)
    val pool  = newPool(gated.factory)

    val establisher  = new AtomicReference[Try[NodeClient]]()
    val establishing = new Thread(() => establisher.set(Try(pool.getOrEstablish(node))), "establisher")
    establishing.start()
    gated.awaitReached(0)

    val waiters = (1 to 3).map { i =>
      val result = new AtomicReference[Try[NodeClient]]()
      val thread = new Thread(() => result.set(Try(pool.getOrEstablish(node))), s"waiter-$i")
      thread.start()
      (thread, result)
    }
    awaitAllWaiting(waiters.map(_._1)) // every waiter is parked on the original token before retain runs

    pool.retain(_ => false)

    waiters.foreach { case (thread, _) => thread.join(2000) }
    waiters.foreach { case (_, result) =>
      assert(result.get() != null && result.get().isFailure, "a waiter did not fail")
      assert(result.get().failed.get.isInstanceOf[NotConnected], s"unexpected waiter error: ${result.get()}")
    }

    gated.release(0)
    establishing.join(2000)
    assert(
      establisher.get() != null && establisher.get().isFailure && establisher.get().failed.get.isInstanceOf[NotConnected],
      s"establisher: ${establisher.get()}"
    )

    awaitTrue(gated.transport(0).closeCount > 0, "the discarded NodeClient was not closed")
    assert(pool.existingLive(node).isEmpty, "the rejected node leaked into the pool")
    assert(!pool.candidatesByLiveness.contains(node), "the rejected node leaked into refresh candidates")
    pool.close()
  }

  test("retain leaves an accepted in-flight establishment to complete and publish") {
    val node  = Node("kept", 6379)
    val gated = new GatedFactory(1)
    val pool  = newPool(gated.factory)

    val result       = new AtomicReference[Try[NodeClient]]()
    val establishing = new Thread(() => result.set(Try(pool.getOrEstablish(node))), "establisher")
    establishing.start()
    gated.awaitReached(0)

    pool.retain(_ => true)

    gated.release(0)
    establishing.join(2000)
    assert(result.get() != null && result.get().isSuccess, s"expected success, got ${result.get()}")
    assert(pool.existingLive(node).isDefined, "the accepted node should be live in the pool")
    pool.close()
  }

  test("an attempt invalidated by retain neither removes nor prevents a newer attempt for the same node") {
    val node  = Node("racing", 6379)
    val gated = new GatedFactory(2)
    val pool  = newPool(gated.factory)

    val first       = new AtomicReference[Try[NodeClient]]()
    val firstThread = new Thread(() => first.set(Try(pool.getOrEstablish(node))), "attempt-1")
    firstThread.start()
    gated.awaitReached(0)

    pool.retain(_ => false) // clears attempt 1 from pending while its connect is still in flight

    val second       = new AtomicReference[Try[NodeClient]]()
    val secondThread = new Thread(() => second.set(Try(pool.getOrEstablish(node))), "attempt-2")
    secondThread.start()
    gated.awaitReached(1) // a fresh attempt, since attempt 1 is no longer pending

    gated.release(0) // attempt 1 completes while attempt 2 is pending: it must discard without touching attempt 2
    firstThread.join(2000)
    assert(
      first.get() != null && first.get().isFailure && first.get().failed.get.isInstanceOf[NotConnected],
      s"attempt 1: ${first.get()}"
    )
    awaitTrue(gated.transport(0).closeCount > 0, "attempt 1's discarded client was not closed")

    gated.release(1)
    secondThread.join(2000)
    assert(second.get() != null && second.get().isSuccess, s"attempt 2: ${second.get()}")
    assert(pool.existingLive(node).isDefined, "attempt 2 did not publish")
    assertEquals(gated.transport(1).closeCount, 0, "attempt 2's published client must not be closed")
    pool.close()
  }

  test("close aborts an in-flight node establishment still blocked in the connect (start) phase") {
    val connecting                                              = new AtomicReference[ConnectingTransport]()
    val factory: Node => MultiplexedConnection.TransportFactory = _ =>
      (_, onClosed) => {
        val transport = new ConnectingTransport(onClosed)
        connecting.set(transport)
        transport
      }
    val pool                                                    = newPool(factory)
    val node                                                    = Node("slow", 6379)

    val result       = new AtomicReference[Try[NodeClient]]()
    val establishing = new Thread(() => result.set(Try(pool.getOrEstablish(node))), "establisher")
    establishing.start()
    awaitTrue(connecting.get() != null, "the establish never started")
    assert(connecting.get().reached.await(2, TimeUnit.SECONDS), "the establish never reached the connect phase")

    pool.close()
    establishing.join(2000)

    assert(!establishing.isAlive, "close must abort the in-flight establishment, not wait out the connect")
    assert(connecting.get().wasClosed, "close must abort the establishing transport")
    assert(result.get() != null && result.get().isFailure, s"the aborted establisher should fail: ${result.get()}")
  }
}

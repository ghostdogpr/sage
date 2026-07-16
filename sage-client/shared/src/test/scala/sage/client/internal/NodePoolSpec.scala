package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

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

  // a factory that blocks the first (multiplexed) connect on `gate`, signalling `reached` first, and captures the opened transport
  private def gatedFactory(
    reached: CountDownLatch,
    gate: CountDownLatch,
    transport: AtomicReference[FakeTransport]
  ): Node => MultiplexedConnection.TransportFactory =
    _ =>
      (onFrame, onClosed) => {
        if (transport.get() == null) { reached.countDown(); gate.await() }
        val t = new FakeTransport(onFrame, onClosed, respond)
        transport.compareAndSet(null, t)
        t
      }

  private def awaitTrue(cond: => Boolean, clue: String): Unit = {
    val deadline = System.nanoTime() + 2.seconds.toNanos
    while (!cond && System.nanoTime() < deadline) Thread.sleep(10)
    assert(cond, clue)
  }

  test("retain invalidates an in-flight establishment for a rejected node: the client is closed, absent, and every waiter fails") {
    val node      = Node("gated", 6379)
    val reached   = new CountDownLatch(1)
    val gate      = new CountDownLatch(1)
    val transport = new AtomicReference[FakeTransport]()
    val pool      = newPool(gatedFactory(reached, gate, transport))

    val establisher  = new AtomicReference[Try[NodeClient]]()
    val establishing = new Thread(() => establisher.set(Try(pool.getOrEstablish(node))), "establisher")
    establishing.start()
    assert(reached.await(2, TimeUnit.SECONDS), "the establisher never reached connect")

    val waiters = (1 to 3).map { i =>
      val result = new AtomicReference[Try[NodeClient]]()
      val thread = new Thread(() => result.set(Try(pool.getOrEstablish(node))), s"waiter-$i")
      thread.start()
      (thread, result)
    }
    Thread.sleep(100) // let the waiters block on the shared establishment before retain runs

    pool.retain(_ => false)

    waiters.foreach { case (thread, _) => thread.join(2000) }
    waiters.foreach { case (_, result) =>
      assert(result.get() != null && result.get().isFailure, "a waiter did not fail")
      assert(result.get().failed.get.isInstanceOf[NotConnected], s"unexpected waiter error: ${result.get()}")
    }

    gate.countDown()
    establishing.join(2000)
    assert(
      establisher.get() != null && establisher.get().isFailure && establisher.get().failed.get.isInstanceOf[NotConnected],
      s"establisher: ${establisher.get()}"
    )

    val opened = transport.get()
    assert(opened != null, "no transport was ever created")
    awaitTrue(opened.closeCount > 0, "the discarded NodeClient was not closed")
    assert(pool.existingLive(node).isEmpty, "the rejected node leaked into the pool")
    assert(!pool.candidatesByLiveness.contains(node), "the rejected node leaked into refresh candidates")
    pool.close()
  }

  test("retain leaves an accepted in-flight establishment to complete and publish") {
    val node      = Node("kept", 6379)
    val reached   = new CountDownLatch(1)
    val gate      = new CountDownLatch(1)
    val transport = new AtomicReference[FakeTransport]()
    val pool      = newPool(gatedFactory(reached, gate, transport))

    val result       = new AtomicReference[Try[NodeClient]]()
    val establishing = new Thread(() => result.set(Try(pool.getOrEstablish(node))), "establisher")
    establishing.start()
    assert(reached.await(2, TimeUnit.SECONDS), "the establisher never reached connect")

    pool.retain(_ => true)

    gate.countDown()
    establishing.join(2000)
    assert(result.get() != null && result.get().isSuccess, s"expected success, got ${result.get()}")
    assert(pool.existingLive(node).isDefined, "the accepted node should be live in the pool")
    pool.close()
  }
}

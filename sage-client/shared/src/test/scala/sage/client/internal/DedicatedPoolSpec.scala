package sage.client.internal

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.SageException.{ConnectionLost, NotConnected, TimedOut}
import sage.client.{BackoffConfig, DedicatedPoolConfig, WatchdogConfig}
import sage.commands.{BlockTimeout, Connection, Lists}
import sage.protocol.Frame

class DedicatedPoolSpec extends munit.FunSuite {

  private val helloReply: Frame =
    Frame.Map(
      Vector(
        Frame.BulkString(Bytes.utf8("server"))  -> Frame.BulkString(Bytes.utf8("redis")),
        Frame.BulkString(Bytes.utf8("version")) -> Frame.BulkString(Bytes.utf8("8.0.0")),
        Frame.BulkString(Bytes.utf8("proto"))   -> Frame.Integer(3),
        Frame.BulkString(Bytes.utf8("role"))    -> Frame.BulkString(Bytes.utf8("master"))
      )
    )

  private val popReply: Frame = Frame.Array(Vector(Frame.BulkString(Bytes.utf8("k")), Frame.BulkString(Bytes.utf8("v"))))

  // HELLO always answers so the bootstrap succeeds; the blocking command's reply is the test's to script
  private def replyWith(blocking: Seq[Frame]): Bytes => Seq[Frame] =
    payload => if (payload.asUtf8String.contains("HELLO")) Seq(helloReply) else blocking

  private def make(
    respond: Bytes => Seq[Frame],
    isLive: () => Boolean = () => true,
    liveGeneration: () => Option[MultiplexedConnection.Generation] = () => Some(MultiplexedConnection.Generation.initial),
    config: DedicatedPoolConfig = DedicatedPoolConfig()
  ): (DedicatedPool, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                              = new ManualScheduler
    val transports                                             = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory        = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, respond)
      transports += transport
      transport
    }
    // mirrors the real MultiplexedConnection: a stamp is current iff the connection is live and the generation matches
    val isCurrent: MultiplexedConnection.Generation => Boolean = g => liveGeneration().contains(g)
    val pool                                                   =
      new DedicatedPool(factory, Vector(Connection.hello()), scheduler, isLive, liveGeneration, isCurrent, config, 1000L)
    (pool, scheduler, transports)
  }

  private val blPop = Lists.blPop[String, String]("k")(BlockTimeout.Forever)

  test("a blocking command runs on a freshly established connection and returns its reply") {
    val (pool, scheduler, transports)                 = make(replyWith(Seq(popReply)))
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    scheduler.advance(Duration.Zero)
    assertEquals(result, Some(Success(Some(("k", "v")))))
    assertEquals(transports.size, 1)
  }

  test("a released connection is reused rather than reopened") {
    val (pool, scheduler, transports) = make(replyWith(Seq(popReply)))
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)
  }

  test("a blocking command issued while not connected fails fast NotConnected") {
    val (pool, _, transports)                         = make(replyWith(Nil), isLive = () => false)
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    assertEquals(result, Some(Failure(NotConnected())))
    assertEquals(transports.size, 0)
  }

  test("connection loss while blocking fails the command as possibly executed and discards the connection") {
    val (pool, scheduler, transports)                 = make(replyWith(Nil))
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    scheduler.advance(Duration.Zero)
    assertEquals(result, None)

    transports.head.close()
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))

    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 2)
  }

  test("a dropped queued command marks the connection dead before failing the work, so the pool cannot recycle it") {
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, replyWith(Nil)); transports += t; t
    }
    val conn                                            = DedicatedConnection.establish(factory, Vector(Connection.hello()), 1000L)
    transports.head.autoWrite = false // hold the write so the command stays queued: the path the transport drops on teardown

    var healthyWhenFailed: Option[Boolean] = None
    conn.submit(blPop, _ => healthyWhenFailed = Some(conn.isHealthy))
    transports.head.close()
    assertEquals(healthyWhenFailed, Some(false))
  }

  test("acquire waits for a slot and fails TimedOut when the pool stays exhausted") {
    val config                        = DedicatedPoolConfig(maxConnections = 1, acquireTimeout = 50.millis, idleTimeout = Duration.Inf)
    val (pool, scheduler, transports) = make(replyWith(Nil), config = config) // the first BLPOP parks, holding the only slot
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)

    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    scheduler.advance(Duration.Zero) // the offloaded acquire waits 50ms, then times out
    assert(result.exists(_.isFailure), s"expected a failure, got $result")
    assert(result.get.failed.get.isInstanceOf[TimedOut], s"expected TimedOut, got ${result.get}")
    assertEquals(transports.size, 1)
  }

  test("close force-closes an in-flight blocking command at once") {
    val (pool, scheduler, _)                          = make(replyWith(Nil))
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    scheduler.advance(Duration.Zero)
    assertEquals(result, None)

    pool.close()
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
  }

  test("an idle connection from a previous generation is discarded rather than reused") {
    var live                          = Option(MultiplexedConnection.Generation.initial)
    val (pool, scheduler, transports) = make(replyWith(Seq(popReply)), liveGeneration = () => live)
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)

    live = Some(MultiplexedConnection.Generation.initial.next) // the multiplexed connection reconnected (e.g. failover) under the pool
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 2)
  }

  test("a connection built across a reconnect is admitted under the new generation, not discarded") {
    var live                                          = Option(MultiplexedConnection.Generation.initial)
    var bumped                                        = false
    // the multiplexed connection reconnects (generation bumps) while the first dedicated connection is running its HELLO bootstrap
    val respond: Bytes => Seq[Frame]                  = payload =>
      if (payload.asUtf8String.contains("HELLO")) {
        if (!bumped) { bumped = true; live = Some(MultiplexedConnection.Generation.initial.next) }
        Seq(helloReply)
      } else Seq(popReply)
    val (pool, scheduler, transports)                 = make(respond, liveGeneration = () => live)
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    scheduler.advance(Duration.Zero)
    assertEquals(result, Some(Success(Some(("k", "v")))))
    // commit-time stamping records the new epoch; the connection is born current, so there is no born-stale discard and no retry
    assertEquals(transports.size, 1)
  }

  test("an idle connection is not reused when the connection leaves Live between lease and acquire") {
    var live                          = true
    val gen                           = MultiplexedConnection.Generation.initial
    val (pool, scheduler, transports) =
      make(replyWith(Seq(popReply)), isLive = () => live, liveGeneration = () => if (live) Some(gen) else None)
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1) // established and returned to idle at generation `gen`

    // the lease gate observes Live, then the multiplexed connection drops into a reconnect window before the offloaded acquire runs:
    // the idle connection is at the same generation but the connection is no longer live, so it must be refused, not reused
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    live = false
    scheduler.advance(Duration.Zero)
    assertEquals(result, Some(Failure(NotConnected())))
    assertEquals(transports.size, 1) // and it fails fast without opening a fresh socket during the reconnect window
  }

  test("an exhausted pool fails fast NotConnected, not TimedOut, when the connection is not live") {
    var live                          = true
    val gen                           = MultiplexedConnection.Generation.initial
    val config                        = DedicatedPoolConfig(maxConnections = 1, acquireTimeout = 50.millis, idleTimeout = Duration.Inf)
    val (pool, scheduler, transports) =
      make(replyWith(Nil), isLive = () => live, liveGeneration = () => if (live) Some(gen) else None, config = config)
    pool.use(blPop, _ => ()) // the only slot is held by a parked BLPOP that never replies
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)

    // the lease gate observes Live, then the connection drops before the offloaded acquire runs against the exhausted pool: the waiter
    // must fail fast rather than park for acquireTimeout — proven by resolving on a zero-delay advance instead of needing the 50ms timeout
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    live = false
    scheduler.advance(Duration.Zero)
    assertEquals(result, Some(Failure(NotConnected())))
  }

  test("a READONLY reply poisons the connection so it is not returned to the pool") {
    val readonly                                      = Frame.SimpleError("READONLY You can't write against a read only replica.")
    val (pool, scheduler, transports)                 = make(replyWith(Seq(readonly)))
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    scheduler.advance(Duration.Zero)
    assert(result.exists(_.isFailure), s"expected a failure, got $result")

    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 2)
  }

  test("an idle connection is evicted and closed after idleTimeout") {
    val config                        = DedicatedPoolConfig(idleTimeout = 30.seconds)
    val (pool, scheduler, transports) = make(replyWith(Seq(popReply)), config = config)
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.head.closeCount, 0)

    scheduler.advance(31.seconds)
    assertEquals(transports.head.closeCount, 1)
  }

  test("a transaction lease returns a reusable connection to the pool") {
    val (pool, scheduler, transports) = make(replyWith(Seq(popReply)))
    val conn                          = pool.acquireForTransaction()
    assertEquals(transports.size, 1)

    pool.releaseTransaction(conn, reusable = true)
    val reused = pool.acquireForTransaction()
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)
    pool.releaseTransaction(reused, reusable = true)
  }

  test("a transaction lease discards a non-reusable connection and opens a fresh one next time") {
    val (pool, scheduler, transports) = make(replyWith(Seq(popReply)))
    val conn                          = pool.acquireForTransaction()
    assertEquals(transports.size, 1)

    pool.releaseTransaction(conn, reusable = false)
    scheduler.advance(Duration.Zero)
    assertEquals(transports.head.closeCount, 1)

    val _ = pool.acquireForTransaction()
    assertEquals(transports.size, 2)
  }

  test("a transaction lease issued while not connected fails fast NotConnected") {
    val (pool, _, transports) = make(replyWith(Nil), isLive = () => false)
    intercept[NotConnected](pool.acquireForTransaction())
    assertEquals(transports.size, 0)
  }

  test("a parked acquire is woken to fail fast NotConnected when the multiplexed connection loses liveness") {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, replyWith(Nil)); transports += t; t
    }
    val connection                                      = MultiplexedConnection.connect(
      factory,
      scheduler,
      Vector(Connection.hello()),
      BackoffConfig(1.milli, 1.milli, 1.0),
      WatchdogConfig(enabled = false),
      1.second,
      Duration.Zero
    )
    val config                                          = DedicatedPoolConfig(maxConnections = 1, acquireTimeout = 10.seconds, idleTimeout = Duration.Inf)
    val pool                                            = DedicatedPool.forConnection(factory, Vector(Connection.hello()), scheduler, connection, config, 1000L)

    val held = pool.acquireForTransaction()

    @volatile var result: Option[Try[DedicatedConnection]] = None
    val waiter                                             = new Thread(() => result = Some(Try(pool.acquireForTransaction())))
    waiter.start()
    Thread.sleep(100) // let the second acquire park in awaitNanos

    transports.head.emit(Frame.SimpleString("stray"))
    assert(!connection.isLive)

    waiter.join(3000)
    assert(!waiter.isAlive, "the parked waiter must be woken, not sleep out the 10s acquireTimeout")
    assert(result.exists(_.failed.toOption.exists(_.isInstanceOf[NotConnected])), s"expected NotConnected, got $result")
    pool.releaseTransaction(held, reusable = false)
  }
}

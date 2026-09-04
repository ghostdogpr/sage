package sage.client.internal

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

import Replies.bulk

import sage.Bytes
import sage.SageException.{ConnectionLost, NotConnected, TimedOut}
import sage.client.{BackoffConfig, DedicatedPoolConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.{BlockTimeout, Connection, Lists, Server}
import sage.protocol.Frame

class DedicatedPoolSpec extends munit.FunSuite {

  private val popReply: Frame = Frame.Array(Vector(bulk("k"), bulk("v")))

  // HELLO always answers so the bootstrap succeeds; the blocking command's reply is the test's to script
  private def replyWith(blocking: Seq[Frame]): Bytes => Seq[Frame] =
    payload => if (payload.asUtf8String.contains("HELLO")) Seq(Replies.hello) else blocking

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
    // mirrors the real MultiplexedConnection: a generation is current when the connection is live and the recorded generation matches
    val isCurrent: MultiplexedConnection.Generation => Boolean = g => liveGeneration().contains(g)
    val pool                                                   =
      new DedicatedPool(factory, Vector(Connection.hello()), scheduler, isLive, liveGeneration, isCurrent, config, 1000L)
    (pool, scheduler, transports)
  }

  private val lockWrite =
    new LockCommands[String](3.seconds, "lock").command(Bytes.utf8("key"), "owner", LockCommands.Operation.Acquire, cached = true)

  private def replication(
    scheduler: Scheduler,
    knownReplicaCount: Int,
    deadlineMillis: Long,
    onConfirmationFailure: () => Unit = () => ()
  ): LockReplication =
    new LockReplication(scheduler, knownReplicaCount, deadlineMillis, onConfirmationFailure, replicaAcknowledgement = true)

  test("WAIT accounts for elapsed work and leaves time to return a shortfall on a reusable socket") {
    val (pool, scheduler, transports) = make(replyWith(Nil))
    var result: Option[Try[Boolean]]  = None
    pool.useLockWrite(lockWrite, false, r => result = Some(r), new DedicatedPool.Lease, replication(scheduler, 1, 100L))
    scheduler.advance(40.millis)
    val transport                     = transports.head
    transport.emit(Frame.Integer(1))
    scheduler.advance(20.millis)
    transport.emit(Replies.masterRole(Node("replica", 6380)))
    assert(transport.written.last.sameBytes(Server.waitReplicas(1, 20.millis).encode))
    scheduler.advance(20.millis)
    transport.emit(Frame.Integer(0))
    assert(result.get.failed.get.isInstanceOf[TimedOut])
    pool.useLockWrite(lockWrite, false, _ => (), new DedicatedPool.Lease, replication(scheduler, 0, 200L))
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)
    pool.close()
  }

  test("an exhausted confirmation budget never sends an unbounded WAIT") {
    val (pool, scheduler, transports) = make(replyWith(Nil))
    var result: Option[Try[Boolean]]  = None
    pool.useLockWrite(lockWrite, false, r => result = Some(r), new DedicatedPool.Lease, replication(scheduler, 1, 100L))
    scheduler.advance(Duration.Zero)
    transports.head.emit(Frame.Integer(1))
    scheduler.advance(99.millis)
    transports.head.emit(Replies.masterRole(Node("replica", 6380)))
    assert(result.get.failed.get.isInstanceOf[TimedOut])
    assert(!transports.head.written.exists(_.asUtf8String.contains("WAIT")))
    pool.close()
  }

  test("lock writes keep their socket until every required replica acknowledges") {
    val (pool, scheduler, transports) = make(replyWith(Nil))
    var result: Option[Try[Boolean]]  = None
    pool.useLockWrite(lockWrite, false, r => result = Some(r), new DedicatedPool.Lease, replication(scheduler, 2, 100L))
    scheduler.advance(Duration.Zero)
    val transport                     = transports.head
    transport.emit(Frame.Integer(1))
    assertEquals(result, None)
    assert(transport.written.last.asUtf8String.contains("ROLE"))
    transport.emit(Replies.masterRole(Node("replica", 6380)))
    assert(transport.written.last.sameBytes(Server.waitReplicas(2, 50.millis).encode))
    assertEquals(result, None)
    transport.emit(Frame.Integer(2))
    assertEquals(result, Some(Success(true)))
    pool.useLockWrite(lockWrite, false, _ => (), new DedicatedPool.Lease, replication(scheduler, 0, 100L))
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)
    pool.close()
  }

  for ((known, connected) <- Vector((0, Vector(Node("replica", 6380))), (1, Vector.empty[Node])))
    test(s"lock confirmation requires discovered replicas when topology has $known and ROLE has ${connected.size}") {
      val (pool, scheduler, transports) = make(replyWith(Nil))
      var result: Option[Try[Boolean]]  = None
      var refreshed                     = false
      pool.useLockWrite(
        lockWrite,
        false,
        r => result = Some(r),
        new DedicatedPool.Lease,
        replication(scheduler, known, 100L, () => refreshed = true)
      )
      scheduler.advance(Duration.Zero)
      transports.head.emit(Frame.Integer(1))
      transports.head.emit(Replies.masterRole(connected*))
      assert(transports.head.written.last.sameBytes(Server.waitReplicas(1, 50.millis).encode))
      transports.head.emit(Frame.Integer(0))
      assert(result.get.failed.get.isInstanceOf[TimedOut])
      assert(refreshed)
      pool.close()
    }

  test("busy locks skip confirmation and masters without replicas skip WAIT") {
    val (pool, scheduler, transports) = make(replyWith(Nil))
    var result: Option[Try[Boolean]]  = None
    pool.useLockWrite(lockWrite, false, r => result = Some(r), new DedicatedPool.Lease, replication(scheduler, 1, 100L))
    scheduler.advance(Duration.Zero)
    transports.head.emit(Frame.Integer(0))
    assertEquals(result, Some(Success(false)))
    assert(!transports.head.written.exists(_.asUtf8String.contains("ROLE")))
    pool.useLockWrite(lockWrite, false, r => result = Some(r), new DedicatedPool.Lease, replication(scheduler, 0, 100L))
    scheduler.advance(Duration.Zero)
    transports.head.emit(Frame.Integer(1))
    transports.head.emit(Replies.masterRole())
    assertEquals(result, Some(Success(true)))
    assert(!transports.head.written.exists(_.asUtf8String.contains("WAIT")))
    pool.close()
  }

  test("ASKING precedes the lock write on the socket that confirms it") {
    val (pool, scheduler, transports) = make(replyWith(Nil))
    var result: Option[Try[Boolean]]  = None
    pool.useLockWrite(lockWrite, true, r => result = Some(r), new DedicatedPool.Lease, replication(scheduler, 1, 100L))
    scheduler.advance(Duration.Zero)
    assert(transports.head.written.last.sameBytes(Bytes.concat(Vector(Connection.asking.encode, lockWrite.encode))))
    transports.head.emit(Replies.ok)
    transports.head.emit(Frame.Integer(1))
    transports.head.emit(Replies.masterRole(Node("replica", 6380)))
    transports.head.emit(Frame.Integer(1))
    assertEquals(result, Some(Success(true)))
    pool.close()
  }

  test("connection loss during confirmation keeps the write ambiguous and refreshes topology") {
    val (pool, scheduler, transports) = make(replyWith(Nil))
    var result: Option[Try[Boolean]]  = None
    var refreshed                     = false
    pool.useLockWrite(
      lockWrite,
      false,
      r => result = Some(r),
      new DedicatedPool.Lease,
      replication(scheduler, 1, 100L, () => refreshed = true)
    )
    scheduler.advance(Duration.Zero)
    transports.head.emit(Frame.Integer(1))
    transports.head.close()
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    assert(refreshed)
    pool.close()
  }

  test("a stalled confirmation leaves ordinary commands free and cancellation releases the pool slot") {
    val (pool, scheduler, transports) = make(replyWith(Nil), config = DedicatedPoolConfig(maxConnections = 1))
    val shared                        = MultiplexedConnection.connect(
      (onFrame, onClosed) => new FakeTransport(onFrame, onClosed, _ => Seq(Frame.SimpleString("PONG"))),
      scheduler,
      Vector.empty,
      BackoffConfig(),
      WatchdogConfig(enabled = false),
      1.second,
      Duration.Zero
    )
    val node                          = new NodeClient(shared, pool)
    val lease                         = new DedicatedPool.Lease
    var result: Option[Try[Boolean]]  = None
    var refreshed                     = false
    node.submitLockWrite(
      lockWrite,
      false,
      r => result = Some(r),
      lease,
      replication(scheduler, 1, 100L, () => refreshed = true)
    )
    scheduler.advance(Duration.Zero)
    transports.head.emit(Frame.Integer(1))
    transports.head.emit(Replies.masterRole(Node("replica", 6380)))
    var ping: Option[Try[String]]     = None
    node.submit(Connection.ping(), false, r => ping = Some(r))
    assertEquals(ping, Some(Success("PONG")))
    assertEquals(result, None)
    lease.cancel()
    scheduler.advance(Duration.Zero)
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    assert(refreshed)
    node.submitLockWrite(lockWrite, false, _ => (), new DedicatedPool.Lease, replication(scheduler, 0, 100L))
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 2)
    assertEquals(transports.head.closeCount, 1)
    node.close()
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

  test("interrupting an in-flight blocking command settles the tracked callback and discards the slot") {
    val (pool, scheduler, transports)                 = make(replyWith(Nil)) // the BLPOP waits without receiving a reply
    val lease                                         = new DedicatedPool.Lease
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r), lease)
    scheduler.advance(Duration.Zero)
    assertEquals(result, None)

    lease.cancel()
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    scheduler.advance(Duration.Zero)
    assertEquals(transports.head.closeCount, 1)
  }

  test("interrupting before the lease attaches (offloaded acquire window) still settles the tracked callback") {
    val (pool, scheduler, transports)                 = make(replyWith(Seq(popReply)))
    val lease                                         = new DedicatedPool.Lease
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r), lease)
    lease.cancel()
    scheduler.advance(Duration.Zero)
    assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 0)
  }

  test("cancelling a parked acquisition wakes it without consuming the next available connection") {
    val config                                                  = DedicatedPoolConfig(maxConnections = 1, acquireTimeout = 5.seconds, idleTimeout = Duration.Inf)
    val (pool, scheduler, transports)                           = make(replyWith(Seq(popReply)), config = config)
    val held                                                    = pool.acquireForTransaction()
    val lease                                                   = new DedicatedPool.Lease
    @volatile var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r), lease)
    val waiter                                                  = new Thread(() => scheduler.advance(Duration.Zero))
    waiter.start()
    try {
      val deadline = System.nanoTime() + 2.seconds.toNanos
      while (waiter.getState != Thread.State.TIMED_WAITING && waiter.isAlive && System.nanoTime() < deadline) Thread.sleep(1)
      assertEquals(waiter.getState, Thread.State.TIMED_WAITING)
      lease.cancel()
      waiter.join(2000)
      assert(!waiter.isAlive, "cancellation must wake acquisition while the slot is still occupied")
      assertEquals(result, Some(Failure(ConnectionLost(mayHaveExecuted = true))))
      pool.releaseTransaction(held, reusable = true)
      pool.use(blPop, _ => ())
      scheduler.advance(Duration.Zero)
      assertEquals(transports.size, 1)
      assertEquals(transports.head.closeCount, 0)
    } finally {
      pool.close()
      waiter.join(2000)
    }
  }

  test("a lock deadline ends pool acquisition before acquireTimeout and leaves the occupied connection reusable") {
    val config                        = DedicatedPoolConfig(maxConnections = 1, acquireTimeout = 5.seconds, idleTimeout = Duration.Inf)
    val (pool, scheduler, transports) = make(replyWith(Seq(popReply)), config = config)
    val held                          = pool.acquireForTransaction()
    var result: Option[Try[Boolean]]  = None
    pool.useLockWrite(lockWrite, false, r => result = Some(r), new DedicatedPool.Lease, replication(scheduler, 0, 40L))
    val started                       = System.nanoTime()
    scheduler.advance(Duration.Zero)
    assert((System.nanoTime() - started).nanos < 1.second)
    assert(result.get.failed.get.isInstanceOf[TimedOut])
    pool.releaseTransaction(held, reusable = true)
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)
    assertEquals(transports.head.closeCount, 0)
    assert(!transports.head.written.exists(_.asUtf8String.contains("EVALSHA")))
    pool.close()
  }

  test("cancellation during bootstrap returns the unused healthy connection to the pool") {
    val bootstrapping                 = new java.util.concurrent.CountDownLatch(1)
    val proceed                       = new java.util.concurrent.CountDownLatch(1)
    val (pool, scheduler, transports) = make { payload =>
      if (payload.asUtf8String.contains("HELLO")) {
        bootstrapping.countDown()
        // MUnit holds the suite monitor while evaluating assertions. Wait outside it so the test thread can release this latch.
        val resumed = proceed.await(2, java.util.concurrent.TimeUnit.SECONDS)
        assert(resumed)
        Seq(Replies.hello)
      } else Seq(popReply)
    }
    val lease                         = new DedicatedPool.Lease
    pool.use(blPop, _ => (), lease)
    val acquirer                      = new Thread(() => scheduler.advance(Duration.Zero))
    acquirer.start()
    try {
      val started = bootstrapping.await(2, java.util.concurrent.TimeUnit.SECONDS)
      assert(started)
      lease.cancel()
      proceed.countDown()
      acquirer.join(2000)
      assert(!acquirer.isAlive)
      assert(!transports.head.written.exists(_.asUtf8String.contains("BLPOP")))
      pool.use(blPop, _ => ())
      scheduler.advance(Duration.Zero)
      assertEquals(transports.size, 1)
      assertEquals(transports.head.closeCount, 0)
    } finally {
      proceed.countDown()
      acquirer.join(2000)
      pool.close()
    }
  }

  test("cancelling a lease after the blocking reply landed does not re-fire the callback") {
    val (pool, scheduler, _) = make(replyWith(Seq(popReply)))
    val lease                = new DedicatedPool.Lease
    var count                = 0
    pool.use(blPop, _ => count += 1, lease)
    scheduler.advance(Duration.Zero)
    assertEquals(count, 1)
    lease.cancel()
    assertEquals(count, 1)
  }

  test("a dropped queued command marks the connection dead before failing the work, so the pool cannot recycle it") {
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val t = new FakeTransport(onFrame, onClosed, replyWith(Nil))
      transports += t
      t
    }
    val conn                                            = DedicatedConnection.create(factory, 1000L)
    conn.establish(Vector(Connection.hello()))
    transports.head.autoWrite = false // keep the command in the queue so transport teardown reports it as unsent

    var healthyWhenFailed: Option[Boolean] = None
    conn.submit(blPop, _ => healthyWhenFailed = Some(conn.isHealthy))
    transports.head.close()
    assertEquals(healthyWhenFailed, Some(false))
  }

  test("acquire waits for a slot and fails TimedOut when the pool stays exhausted") {
    val config                        = DedicatedPoolConfig(maxConnections = 1, acquireTimeout = 50.millis, idleTimeout = Duration.Inf)
    val (pool, scheduler, transports) = make(replyWith(Nil), config = config) // the first BLPOP waits while holding the only slot
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
        if (!bumped) {
          bumped = true
          live = Some(MultiplexedConnection.Generation.initial.next)
        }
        Seq(Replies.hello)
      } else Seq(popReply)
    val (pool, scheduler, transports)                 = make(respond, liveGeneration = () => live)
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    scheduler.advance(Duration.Zero)
    assertEquals(result, Some(Success(Some(("k", "v")))))
    // recording the generation after establishment makes the connection current. The pool keeps it and does not retry.
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

    // The lease check observes Live, but the multiplexed connection starts reconnecting before the offloaded acquire runs. The idle connection
    // has the same generation but is no longer live, so the pool refuses it.
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
    pool.use(blPop, _ => ()) // the only slot is held by a BLPOP that is still waiting for a reply
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)

    // The lease check observes Live, but the connection drops before the offloaded acquire runs against the exhausted pool. The waiter fails
    // immediately, as shown by its completion after a zero-delay advance instead of the 50 ms acquisition timeout.
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

    pool.acquireForTransaction()
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
      val t = new FakeTransport(onFrame, onClosed, replyWith(Nil))
      transports += t
      t
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
    Thread.sleep(100) // let the second acquisition wait in awaitNanos

    transports.head.emit(Frame.SimpleString("stray"))
    assert(!connection.isLive)

    waiter.join(3000)
    assert(!waiter.isAlive, "the parked waiter must be woken, not sleep out the 10s acquireTimeout")
    assert(result.exists(_.failed.toOption.exists(_.isInstanceOf[NotConnected])), s"expected NotConnected, got $result")
    pool.releaseTransaction(held, reusable = false)
  }

  test("close aborts a dedicated connection still blocked in the connect (start) phase") {
    val connecting                                      = new java.util.concurrent.atomic.AtomicReference[ConnectingTransport]()
    val scheduler                                       = new ManualScheduler
    val factory: MultiplexedConnection.TransportFactory = (_, onClosed) => {
      val transport = new ConnectingTransport(onClosed)
      connecting.set(transport)
      transport
    }
    val gen                                             = MultiplexedConnection.Generation.initial
    val pool                                            =
      new DedicatedPool(factory, Vector(Connection.hello()), scheduler, () => true, () => Some(gen), _ == gen, DedicatedPoolConfig(), 1000L)

    pool.use(blPop, _ => ())
    val establishing = new Thread(() => scheduler.advance(Duration.Zero)) // blocks inside ConnectingTransport.start()
    establishing.start()

    val deadline = System.currentTimeMillis() + 2000
    while (connecting.get() == null && System.currentTimeMillis() < deadline) Thread.sleep(1)
    assert(connecting.get() != null, "the establish never started")
    assert(connecting.get().reached.await(2, java.util.concurrent.TimeUnit.SECONDS), "the establish never reached the connect phase")

    pool.close()
    establishing.join(2000)

    assert(!establishing.isAlive, "close must abort the establishing connection, not wait out the connect")
    assert(connecting.get().wasClosed, "close must abort the establishing transport")
  }
}

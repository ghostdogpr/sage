package sage.client.internal

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.SageException.{ConnectionLost, NotConnected, TimedOut}
import sage.client.DedicatedPoolConfig
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
    generation: () => Long = () => 0L,
    config: DedicatedPoolConfig = DedicatedPoolConfig()
  ): (DedicatedPool, ManualScheduler, mutable.ArrayBuffer[FakeTransport]) = {
    val scheduler                                       = new ManualScheduler
    val transports                                      = mutable.ArrayBuffer.empty[FakeTransport]
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      val transport = new FakeTransport(onFrame, onClosed, respond)
      transports += transport
      transport
    }
    val pool                                            =
      new DedicatedPool(factory, Vector(Connection.hello()), scheduler, isLive, generation, config, 1000L)
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
    var gen                           = 0L
    val (pool, scheduler, transports) = make(replyWith(Seq(popReply)), generation = () => gen)
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 1)

    gen = 1L // the multiplexed connection reconnected (e.g. failover) under the pool
    pool.use(blPop, _ => ())
    scheduler.advance(Duration.Zero)
    assertEquals(transports.size, 2)
  }

  test("a connection that races a reconnect during establish is discarded and re-established before use") {
    var gen                                           = 0L
    var bumped                                        = false
    // the multiplexed connection reconnects (generation bumps) while the first dedicated connection is running its HELLO bootstrap
    val respond: Bytes => Seq[Frame]                  = payload =>
      if (payload.asUtf8String.contains("HELLO")) {
        if (!bumped) { bumped = true; gen = 1L }
        Seq(helloReply)
      } else Seq(popReply)
    val (pool, scheduler, transports)                 = make(respond, generation = () => gen)
    var result: Option[Try[Option[(String, String)]]] = None
    pool.use(blPop, r => result = Some(r))
    scheduler.advance(Duration.Zero)
    assertEquals(result, Some(Success(Some(("k", "v")))))
    assertEquals(transports.size, 2)
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
}

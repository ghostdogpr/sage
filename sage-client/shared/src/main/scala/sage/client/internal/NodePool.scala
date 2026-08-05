package sage.client.internal

import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import sage.SageEvent
import sage.SageException.NotConnected
import sage.client.{BackoffConfig, DedicatedPoolConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.Command

/**
  * Stores one [[NodeClient]] for each [[Node]]. Concurrent callers for the same node share one connection attempt and receive the same result.
  * If an attempt finishes after [[close]], its connection is closed. Master-replica clients use these pools for both roles. Cluster clients
  * use one for replicas and a separate pool for masters because master failures affect redirects and topology refresh.
  *
  * The `bootstrap` is fixed per pool, so a replica pool can append `READONLY` while a master pool stays read-write.
  */
final private[client] class NodePool(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  bootstrap: Vector[Command[?]],
  reconnect: BackoffConfig,
  watchdog: WatchdogConfig,
  connectTimeout: FiniteDuration,
  closeTimeout: FiniteDuration,
  dedicatedPool: DedicatedPoolConfig,
  cacheMaxBytes: Long = 0L,
  events: Events = Events.disabled,
  dedicatedBootstrap: Option[Vector[Command[?]]] = None
) {

  private val lock             = new ReentrantLock()
  // lock-free reads; every mutation stays under `lock`
  private val established      = new java.util.concurrent.ConcurrentHashMap[Node, NodeClient]()
  private val pendingEstablish = mutable.HashMap.empty[Node, NodePool.Establish]
  // connections whose socket is still being opened, so close() can abort one still connecting
  private val establishing     = mutable.Set.empty[MultiplexedConnection]
  @volatile private var closed = false

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  /**
    * The node's established client, or `null` — never blocks.
    */
  def existing(node: Node): NodeClient = established.get(node)

  def firstLiveNode: Option[Node] = established.asScala.collectFirst { case (node, nc) if nc.isLive => node }

  def foreachEstablished(f: NodeClient => Unit): Unit = established.values.forEach(nc => f(nc))

  private[internal] def pendingWaiterCount(node: Node): Int =
    locked(pendingEstablish.get(node).fold(0)(_.waiterCount))

  // live nodes first, so a refresh prefers a known-good node
  def candidatesByLiveness: Vector[Node] = {
    val (live, others) = established.asScala.toVector.partition(_._2.isLive)
    live.map(_._1) ++ others.map(_._1)
  }

  def getOrEstablish(node: Node): NodeClient = {
    val fast                       = established.get(node)
    if (fast != null) return fast
    var existing: NodeClient       = null
    var waitOn: NodePool.Establish = null
    var mine: NodePool.Establish   = null
    locked {
      if (closed) throw NotConnected()
      existing = established.get(node)
      if (existing == null)
        pendingEstablish.get(node) match {
          case Some(p) => waitOn = p
          case None    =>
            mine = new NodePool.Establish
            pendingEstablish.put(node, mine): Unit
        }
    }
    if (existing != null) existing
    else if (waitOn != null) waitOn.get()
    else {
      val connRef = new java.util.concurrent.atomic.AtomicReference[MultiplexedConnection]()
      val nc      =
        try
          NodeClient.connect(
            nodeFactory(node),
            scheduler,
            bootstrap,
            reconnect,
            watchdog,
            connectTimeout,
            closeTimeout,
            dedicatedPool,
            cacheMaxBytes,
            node,
            events,
            dedicatedBootstrap,
            onConstructed = conn => {
              connRef.set(conn)
              val poolClosed = locked {
                establishing += conn
                closed
              }
              if (poolClosed) conn.close()
            }
          )
        catch {
          case error: Throwable =>
            locked {
              val conn = connRef.get()
              if (conn != null) establishing -= conn
              if (pendingEstablish.get(node).exists(_ eq mine)) { pendingEstablish.remove(node): Unit }
            }
            mine.fail(error)
            if (!closed) events.emit(SageEvent.Connection.ConnectFailed(Some(node), error))
            throw error
        }
      // Publish the client only while this attempt is current. A retain, close, or newer attempt supersedes it, in which case it is closed below.
      val publish = locked {
        val conn    = connRef.get()
        if (conn != null) establishing -= conn
        val current = pendingEstablish.get(node).exists(_ eq mine)
        if (current) { pendingEstablish.remove(node): Unit }
        if (current && !closed) {
          established.put(node, nc)
          true
        } else false
      }
      if (publish) {
        mine.succeed(nc)
        nc
      } else {
        nc.close()
        mine.fail(NotConnected())
        throw NotConnected()
      }
    }
  }

  /**
    * As [[getOrEstablish]], blocking to connect if need be, but `null` rather than throwing when the connect fails.
    */
  def getOrEstablishOrNull(node: Node): NodeClient =
    try getOrEstablish(node)
    catch { case NonFatal(_) => null }

  // remove and close clients for nodes rejected by keep. Also fail connection attempts for those nodes. Schedule closes outside the pool lock.
  def retain(keep: Node => Boolean): Unit = {
    val (gone, rejected) = locked {
      val absent          = established.keySet.asScala.toVector.filterNot(keep).flatMap(node => Option(established.remove(node)))
      val rejectedPending = pendingEstablish.keysIterator.filterNot(keep).toVector.flatMap(node => pendingEstablish.remove(node))
      (absent, rejectedPending)
    }
    gone.foreach(nc => scheduler.after(Duration.Zero)(nc.close()))
    rejected.foreach(_.fail(NotConnected()))
  }

  def close(): Unit = {
    val (all, waiters, opening) = locked {
      closed = true
      val snap     = established.values.asScala.toVector
      val pending  = pendingEstablish.values.toVector
      val inFlight = establishing.toVector
      established.clear()
      pendingEstablish.clear()
      (snap, pending, inFlight)
    }
    // Fail callers waiting for a connection immediately instead of making them wait for the connection timeout; an opening connection
    // observes `closed` when it finishes and closes the node
    waiters.foreach(_.fail(NotConnected()))
    opening.foreach(_.close())
    all.foreach(_.close())
  }
}

private[client] object NodePool {

  // Share one connection attempt among concurrent callers. One caller opens the connection while the others wait for the same result.
  // The first result is final; once close has failed the waiters, a late establishment is ignored.
  final private class Establish {
    private val latch                                           = new CountDownLatch(1)
    private val settled                                         = new java.util.concurrent.atomic.AtomicBoolean(false)
    private val waiters                                         = new java.util.concurrent.atomic.AtomicInteger(0)
    @volatile private var result: Either[Throwable, NodeClient] = null

    def waiterCount: Int = waiters.get()

    def succeed(nc: NodeClient): Unit = settle(Right(nc))
    def fail(error: Throwable): Unit  = settle(Left(error))

    private def settle(outcome: Either[Throwable, NodeClient]): Unit =
      if (settled.compareAndSet(false, true)) {
        result = outcome
        latch.countDown()
      }

    def get(): NodeClient = {
      waiters.incrementAndGet()
      try {
        latch.await()
        result match {
          case Right(nc)   => nc
          case Left(error) => throw error
        }
      } finally waiters.decrementAndGet(): Unit
    }
  }
}

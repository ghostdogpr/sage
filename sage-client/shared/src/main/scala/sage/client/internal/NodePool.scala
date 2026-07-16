package sage.client.internal

import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*

import sage.SageException.NotConnected
import sage.client.{BackoffConfig, DedicatedPoolConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.Command

/**
  * A registry of [[NodeClient]] bundles keyed by [[Node]], with single-flight establishment: concurrent callers for the same node block on
  * one in-flight connect and observe the same success or failure, and a connect that completes after [[close]] is discarded rather than
  * published. Shared by the cluster runtime (for its replica connections) and the master-replica runtime (for both roles); the cluster
  * runtime keeps its own master registry, since that one drives the redirect/refresh state machine.
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
  private val established      = mutable.HashMap.empty[Node, NodeClient]
  private val pendingEstablish = mutable.HashMap.empty[Node, NodePool.Establish]
  // connections whose socket is still being opened, so close() can abort one still connecting
  private val establishing     = mutable.Set.empty[MultiplexedConnection]
  @volatile private var closed = false

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  def existingLive(node: Node): Option[NodeClient] = locked(established.get(node)).filter(_.isLive)

  def firstLiveNode: Option[Node] = locked(established.collectFirst { case (node, nc) if nc.isLive => node })

  // live nodes first, so a refresh prefers a known-good node
  def candidatesByLiveness: Vector[Node] = {
    val (live, others) = locked(established.toVector).partition(_._2.isLive)
    live.map(_._1) ++ others.map(_._1)
  }

  def getOrEstablish(node: Node): NodeClient = {
    var existing: NodeClient       = null
    var waitOn: NodePool.Establish = null
    var mine: NodePool.Establish   = null
    locked {
      if (closed) throw NotConnected()
      established.get(node) match {
        case Some(nc) => existing = nc
        case None     =>
          pendingEstablish.get(node) match {
            case Some(p) => waitOn = p
            case None    => mine = new NodePool.Establish; pendingEstablish.put(node, mine)
          }
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
            Some(node),
            events,
            dedicatedBootstrap,
            onConstructed = conn => { connRef.set(conn); if (locked { establishing += conn; closed }) conn.close() }
          )
        catch {
          case error: Throwable =>
            locked {
              val conn = connRef.get()
              if (conn != null) { val _ = establishing -= conn }
              if (pendingEstablish.get(node).exists(_ eq mine)) { val _ = pendingEstablish.remove(node) }
            }
            mine.fail(error)
            throw error
        }
      // publish only if our token is still current: a retain, close, or newer attempt supersedes us, and the new client is discarded not leaked
      val publish = locked {
        val conn    = connRef.get()
        if (conn != null) { val _ = establishing -= conn }
        val current = pendingEstablish.get(node).exists(_ eq mine)
        if (current) { val _ = pendingEstablish.remove(node) }
        if (current && !closed) { established.put(node, nc); true }
        else false
      }
      if (publish) { mine.succeed(nc); nc }
      else { nc.close(); mine.fail(NotConnected()); throw NotConnected() }
    }
  }

  // drops/closes bundles `keep` rejects and invalidates in-flight establishments for rejected nodes, so neither leaks; closes are offloaded
  def retain(keep: Node => Boolean): Unit = {
    val (gone, rejected) = locked {
      val absent          = established.keysIterator.filterNot(keep).toVector.flatMap(node => established.remove(node))
      val rejectedPending = pendingEstablish.keysIterator.filterNot(keep).toVector.flatMap(node => pendingEstablish.remove(node))
      (absent, rejectedPending)
    }
    gone.foreach(nc => scheduler.after(Duration.Zero)(nc.close()))
    rejected.foreach(_.fail(NotConnected()))
  }

  def close(): Unit = {
    val (all, waiters, opening) = locked {
      closed = true
      val snap     = established.values.toVector
      val pending  = pendingEstablish.values.toVector
      val inFlight = establishing.toVector
      established.clear()
      pendingEstablish.clear()
      (snap, pending, inFlight)
    }
    // release callers blocked on an in-flight connect now, rather than stranding them for the connect timeout; the establisher still
    // observes `closed` on return and closes the node it opened
    waiters.foreach(_.fail(NotConnected()))
    opening.foreach(_.close())
    all.foreach(_.close())
  }
}

private[client] object NodePool {

  // one-shot single-flight cell: the establisher fills it, concurrent callers block on `get` and observe the same success or failure. The
  // first settle wins, so a `close` that fails the waiters cannot be overwritten by a late establisher.
  final private class Establish {
    private val latch                                           = new CountDownLatch(1)
    private val settled                                         = new java.util.concurrent.atomic.AtomicBoolean(false)
    @volatile private var result: Either[Throwable, NodeClient] = null

    def succeed(nc: NodeClient): Unit = settle(Right(nc))
    def fail(error: Throwable): Unit  = settle(Left(error))

    private def settle(outcome: Either[Throwable, NodeClient]): Unit =
      if (settled.compareAndSet(false, true)) { result = outcome; latch.countDown() }

    def get(): NodeClient = {
      latch.await()
      result match {
        case Right(nc)   => nc
        case Left(error) => throw error
      }
    }
  }
}

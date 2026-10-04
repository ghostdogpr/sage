package sage.client.internal

import java.util.concurrent.{CompletableFuture, ExecutionException}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import sage.SageEvent
import sage.SageException.NotConnected
import sage.client.SageConfig
import sage.cluster.Node

/**
  * Stores one [[MultiplexedConnection]] for each [[Node]]. Concurrent callers for the same node share one connection attempt and receive the same result.
  * If an attempt finishes after [[close]], its connection is closed. Master-replica clients use these pools for both roles. Cluster clients
  * use one for replicas and a separate pool for masters because master failures affect redirects and topology refresh.
  */
final private[client] class NodePool(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  config: SageConfig,
  role: MultiplexedConnection.NodeRole,
  events: Events = Events.disabled
) {

  private val lock             = new ReentrantLock()
  // lock-free reads; every mutation stays under `lock`
  private val established      = new java.util.concurrent.ConcurrentHashMap[Node, MultiplexedConnection]()
  // one attempt shared by concurrent callers for a node; the first result is final, so a late establishment after close is ignored
  private val pendingEstablish = mutable.HashMap.empty[Node, CompletableFuture[MultiplexedConnection]]
  // connections whose socket is still being opened, so close() can abort one still connecting
  private val establishing     = mutable.Set.empty[MultiplexedConnection]
  @volatile private var closed = false

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  /**
    * Returns the established client for the node, or `null`. This method never blocks.
    */
  def existing(node: Node): MultiplexedConnection = established.get(node)

  def firstLiveNode: Option[Node] = established.asScala.collectFirst { case (node, nc) if nc.isLive => node }

  def foreachEstablished(f: MultiplexedConnection => Unit): Unit = established.values.forEach(nc => f(nc))

  private[internal] def pendingWaiterCount(node: Node): Int =
    locked(pendingEstablish.get(node).fold(0)(_.getNumberOfDependents))

  // live nodes first, so a refresh prefers a known-good node
  def candidatesByLiveness: Vector[Node] = {
    val (live, others) = established.asScala.toVector.partition(_._2.isLive)
    live.map(_._1) ++ others.map(_._1)
  }

  def getOrEstablish(node: Node): MultiplexedConnection = {
    val fast    = established.get(node)
    if (fast != null) return fast
    // Left joins an attempt in flight, Right owns a new one
    val attempt = locked {
      if (closed) throw NotConnected()
      val existing = established.get(node)
      if (existing != null) return existing
      pendingEstablish.get(node).toLeft {
        val mine = new CompletableFuture[MultiplexedConnection]
        pendingEstablish.put(node, mine)
        mine
      }
    }
    attempt match {
      case Left(waitOn) =>
        try waitOn.get()
        catch { case e: ExecutionException => throw e.getCause }
      case Right(mine)  =>
        var nc: MultiplexedConnection = null
        onThrow {
          nc = new MultiplexedConnection(nodeFactory(node), scheduler, config, role, Some(node), events)
          // register before the blocking connect so that close() can abort it
          if (locked { establishing += nc; closed }) nc.close()
          nc.start(): Unit
        } { error =>
          locked {
            establishing -= nc
            if (pendingEstablish.get(node).exists(_ eq mine)) { pendingEstablish.remove(node): Unit }
          }
          // a joiner gets NotConnected for this thread's interrupt, as it does for an abandoned attempt
          mine.completeExceptionally(error match { case NonFatal(e) => e; case _ => NotConnected() })
          if (!closed) events.emit(SageEvent.Connection.ConnectFailed(Some(node), error))
        }
        // Publish the client only while this attempt is current. A retain, close, or newer attempt supersedes it, in which case it is closed below.
        val publish                   = locked {
          establishing -= nc
          val current = pendingEstablish.get(node).exists(_ eq mine)
          if (current) { pendingEstablish.remove(node): Unit }
          if (current && !closed) {
            established.put(node, nc)
            true
          } else false
        }
        if (publish) {
          mine.complete(nc)
          nc
        } else {
          mine.completeExceptionally(NotConnected())
          nc.close()
          throw NotConnected()
        }
    }
  }

  /**
    * As [[getOrEstablish]], blocking to connect if need be, but `null` rather than throwing when the connect fails.
    */
  def getOrEstablishOrNull(node: Node): MultiplexedConnection =
    try getOrEstablish(node)
    catch { case NonFatal(_) => null }

  /**
    * Runs `use` on the caller's thread when the node already has a client. Otherwise connects on the scheduler and runs `use` with the new
    * client, or `unreachable` when the connect fails. Inlining keeps the established path free of a closure allocation.
    */
  inline def withClient(node: Node)(inline unreachable: => Unit)(inline use: MultiplexedConnection => Unit): Unit = {
    val nc = existing(node)
    if (nc != null) use(nc)
    else
      scheduler.offload {
        val connected = getOrEstablishOrNull(node)
        if (connected != null) use(connected) else unreachable
      }
  }

  // remove and close clients for nodes rejected by keep. Also fail connection attempts for those nodes. Schedule closes outside the pool lock.
  def retain(keep: Node => Boolean): Unit = {
    val (gone, rejected) = locked {
      val absent          = established.keySet.asScala.toVector.filterNot(keep).flatMap(node => Option(established.remove(node)))
      val rejectedPending = pendingEstablish.keysIterator.filterNot(keep).toVector.flatMap(node => pendingEstablish.remove(node))
      (absent, rejectedPending)
    }
    gone.foreach(nc => scheduler.after(Duration.Zero)(nc.close()))
    rejected.foreach(_.completeExceptionally(NotConnected()))
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
    waiters.foreach(_.completeExceptionally(NotConnected()))
    opening.foreach(_.close())
    all.foreach(_.close())
  }
}

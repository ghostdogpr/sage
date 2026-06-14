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
  @volatile private var closed = false

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  def existingLive(node: Node): Option[NodeClient] = locked(established.get(node)).filter(_.isLive)

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
    else
      try {
        val nc    = NodeClient.connect(
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
          dedicatedBootstrap
        )
        val stale = locked { pendingEstablish.remove(node); if (closed) true else { established.put(node, nc); false } }
        if (stale) { nc.close(); throw NotConnected() }
        mine.succeed(nc)
        nc
      } catch {
        case error: Throwable =>
          locked(pendingEstablish.remove(node))
          mine.fail(error)
          throw error
      }
  }

  // drops and closes every bundle whose node `keep` rejects, so a vanished node's reconnect loop cannot leak; closes are offloaded
  def retain(keep: Node => Boolean): Unit = {
    val gone = locked {
      val absent = established.keysIterator.filterNot(keep).toVector
      absent.flatMap(node => established.remove(node))
    }
    gone.foreach(nc => scheduler.after(Duration.Zero)(nc.close()))
  }

  def close(): Unit = {
    val all = locked { closed = true; val snap = established.values.toVector; established.clear(); snap }
    all.foreach(_.close())
  }
}

private[client] object NodePool {

  // one-shot single-flight cell: the establisher fills it, concurrent callers block on `get` and observe the same success or failure
  final private class Establish {
    private val latch                                           = new CountDownLatch(1)
    @volatile private var result: Either[Throwable, NodeClient] = null

    def succeed(nc: NodeClient): Unit = { result = Right(nc); latch.countDown() }
    def fail(error: Throwable): Unit  = { result = Left(error); latch.countDown() }

    def get(): NodeClient = {
      latch.await()
      result match {
        case Right(nc)   => nc
        case Left(error) => throw error
      }
    }
  }
}

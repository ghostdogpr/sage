package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.util.control.NonFatal

import SubscriptionConnection.{Kind, RawSubscription, Sink}

import sage.Bytes
import sage.SageException.NotConnected
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.cluster.{ClusterTopology, Node, Slot}
import sage.commands.Command

/**
  * Manages pub/sub subscriptions in a cluster. Classic channel and pattern subscriptions share one connection to an arbitrary master
  * because `PUBLISH` broadcasts across the cluster. If that node becomes unavailable, the manager chooses another master. Shard channel
  * subscriptions use one connection per owning node, created when first needed and closed after its last subscription ends.
  *
  * Cluster subscription connections do not reconnect themselves. When one closes, the manager refreshes the topology and assigns its
  * subscribers to the current owner. It also performs this reconciliation after topology changes discovered by commands. Each
  * `SSUBSCRIBE` contains channels from one slot to avoid `CROSSSLOT` errors.
  */
final private[client] class ClusterSubscriptions(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  bootstrap: Vector[Command[?]],
  scheduler: Scheduler,
  reconnect: BackoffConfig,
  watchdog: WatchdogConfig,
  connectTimeoutMillis: Long,
  bufferSize: Int,
  topologyOf: () => ClusterTopology,
  refresh: () => Unit,
  pickMaster: () => Option[Node]
) {

  private val lock = new ReentrantLock()

  // --- classic state (guarded by lock) ---
  private var classicConn: SubscriptionConnection = null
  private val classicSubs                         = mutable.LinkedHashSet.empty[ClassicSub]

  // --- sharded state (guarded by lock) ---
  private val shardConns = mutable.HashMap.empty[Node, SubscriptionConnection]
  private val shardSubs  = mutable.LinkedHashSet.empty[ShardSub]

  private var closed = false

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  // run one pass at a time while using the outer lock to update its state. If schedule is called during a pass, run one more pass afterward.
  final private class CoalescedPass(body: () => Unit) {
    private var running = false
    private var queued  = false

    def schedule(): Unit = {
      val go = locked {
        if (closed) false
        else if (running) {
          queued = true
          false
        } else {
          running = true
          true
        }
      }
      if (go) scheduler.offload(run())
    }

    private def run(): Unit =
      try body()
      finally {
        val again = locked {
          if (!closed && queued) {
            queued = false
            true
          } else {
            running = false
            false
          }
        }
        if (again) scheduler.offload(run())
      }
  }

  // refresh first so a replacement for the failed master is chosen from the current topology.
  private val classicRehome  = new CoalescedPass(() => {
    refresh()
    rehomeClassic()
  })
  private val shardReconcile = new CoalescedPass(() => reconcileShard())

  // --- classic (channels / patterns) -------------------------------------------------------------------------------------------------------

  def subscribeChannels(channels: Vector[String]): RawSubscription = classic(channels, Kind.Channel)

  def subscribePatterns(patterns: Vector[String]): RawSubscription = classic(patterns, Kind.Pattern)

  private def classic(names: Vector[String], kind: Kind): RawSubscription = {
    val sink = new Sink(names, kind, bufferSize)
    val sub  = ClassicSub(sink, names, kind)
    try {
      val conn =
        locked {
          if (closed) throw NotConnected()
          classicSubs += sub
          ensureClassicConn()
        }
      conn.attach(sink, names, kind)
    } catch {
      case e: Throwable =>
        locked(classicSubs -= sub)
        sink.terminate()
        throw e
    }
    new RawSubscription(sink, () => closeClassic(sub))
  }

  // must hold lock
  private def ensureClassicConn(): SubscriptionConnection = {
    if (classicConn == null)
      pickMaster() match {
        case Some(node) => classicConn = newConnection(node, () => onClassicTerminated())
        case None       => throw NotConnected()
      }
    classicConn
  }

  private def closeClassic(sub: ClassicSub): Unit = {
    val (conn, teardown) =
      locked {
        classicSubs -= sub
        val c        = classicConn
        val teardown = classicSubs.isEmpty
        if (teardown) classicConn = null
        (c, teardown)
      }
    if (conn != null) {
      conn.detach(sub.sink, sub.names, sub.kind)
      sub.sink.terminate()
      if (teardown) conn.close()
    } else sub.sink.terminate()
  }

  private def onClassicTerminated(): Unit = {
    locked { classicConn = null }
    classicRehome.schedule()
  }

  private def rehomeClassic(): Unit = {
    val subs = locked(if (closed || classicSubs.isEmpty) Vector.empty[ClassicSub] else classicSubs.toVector)
    if (subs.nonEmpty) {
      val conn = locked {
        if (closed || classicSubs.isEmpty) null
        else {
          if (classicConn == null) pickMaster().foreach(node => classicConn = newConnection(node, () => onClassicTerminated()))
          classicConn
        }
      }
      if (conn == null) scheduleRehomeRetry() // a master is not available yet; retry when the topology may contain one
      else {
        // When attachment fails during establishment, it resets the connection to Idle and rethrows without calling onTerminated. Ignoring
        // that failure would leave every classic subscription on the dead connection. Drop the connection and retry here.
        var failed = false
        subs.foreach { sub =>
          try {
            conn.attach(sub.sink, sub.names, sub.kind)
            // if the subscription closed while attach was running, closeClassic could not detach it yet. Detach it here after attach finishes.
            if (!locked(classicSubs.contains(sub))) { conn.detach(sub.sink, sub.names, sub.kind): Unit }
          } catch { case NonFatal(_) => failed = true }
        }
        if (failed) {
          // Attachment may have restored some subscriptions on this connection. Close it before retrying to avoid duplicate delivery and an
          // unused open socket.
          locked(if (classicConn eq conn) classicConn = null)
          conn.shutdown()
          scheduleRehomeRetry()
        }
      }
    }
  }

  private def scheduleRehomeRetry(): Unit =
    if (!locked(closed)) scheduler.after(reconnect.initialDelay)(classicRehome.schedule())

  // --- sharded (shard channels) ------------------------------------------------------------------------------------------------------------

  // ensure/get both yield nothing once closed, so no connection is created during teardown
  private val conns: Placement.Conns = new Placement.Conns {
    def ensure(node: Node): Option[Placement.ShardConn] = locked(if (closed) None else Some(ensureShardConn(node)))
    def get(node: Node): Option[Placement.ShardConn]    = locked(shardConns.get(node))
  }

  def subscribeShard(channels: Vector[String]): RawSubscription = {
    val sink = new Sink(channels, Kind.Shard, bufferSize)
    val sub  = ShardSub(sink, channels)
    locked {
      if (closed) throw NotConnected()
      shardSubs += sub
    }
    // if placement fails, closeShard detaches any completed subscriptions and terminates the sink
    try place(sub)
    catch {
      case e: Throwable =>
        closeShard(sub)
        throw e
    }
    new RawSubscription(sink, () => closeShard(sub))
  }

  // if the initial connection attempt fails, cancel the whole subscription. Keep channels with an unowned slot pending and retry them.
  private def place(sub: ShardSub): Unit = {
    if (hasUnownedSlot(sub.channels)) refresh()
    sub.placement.place(planFor(sub.channels), conns)
    if (!sub.placement.fullyPlaced) scheduleRetry()
  }

  // Group channels by owning node and then by slot, with one SSUBSCRIBE for each group. Omit an unowned slot from this attempt; the caller
  // refreshes the topology before each retry.
  private def planFor(channels: Vector[String]): Placement.Plan = {
    val topo   = topologyOf()
    val byNode = mutable.HashMap.empty[Node, mutable.HashMap[Slot, mutable.ArrayBuffer[String]]]
    channels.foreach { channel =>
      val slot = Slot.of(Bytes.utf8(channel))
      topo.nodeForSlot(slot).foreach { node =>
        byNode.getOrElseUpdate(node, mutable.HashMap.empty).getOrElseUpdate(slot, mutable.ArrayBuffer.empty) += channel
      }
    }
    byNode.iterator.map { case (node, slots) => node -> slots.valuesIterator.map(_.toVector).toVector }.toMap
  }

  private def hasUnownedSlot(channels: Vector[String]): Boolean = {
    val topo = topologyOf()
    channels.exists(channel => topo.nodeForSlot(Slot.of(Bytes.utf8(channel))).isEmpty)
  }

  // must hold lock
  private def ensureShardConn(node: Node): SubscriptionConnection =
    shardConns.getOrElseUpdate(node, newConnection(node, () => onShardConnTerminated(node)))

  private def onShardConnTerminated(node: Node): Unit = {
    locked(shardConns.remove(node))
    // A drop may mean the slot migrated (server sends sunsubscribe then disconnects); force a refresh — stale topology still names the dead
    // owner, which planFor would not see as unowned — then reconcile onto the current owner
    scheduler.offload {
      refresh()
      shardReconcile.schedule()
    }
  }

  def onTopologyChanged(): Unit = shardReconcile.schedule()

  // Assign each subscription to the current owners of its channels. Refresh at most once per pass and retry incomplete work after transient
  // failover errors.
  private def reconcileShard(): Unit = {
    val subs = locked(if (closed) Vector.empty else shardSubs.toVector)
    if (subs.nonEmpty) {
      if (subs.exists(sub => hasUnownedSlot(sub.channels))) refresh()
      var incomplete = false
      subs.foreach { sub =>
        val failed = sub.placement.reconcile(planFor(sub.channels), conns)
        // If the subscription closed during this pass, closeShard may have detached it before reconcile attached it again. Reconcile with an
        // empty plan to remove those attachments.
        if (!locked(shardSubs.contains(sub))) { sub.placement.reconcile(Map.empty, conns): Unit }
        // The plan omits an unowned slot, and reconcile does not treat that omission as a failure. Check fullyPlaced and retry while a requested
        // channel remains unattached.
        else if (failed || !sub.placement.fullyPlaced) incomplete = true
      }
      evictEmptyShardConns()
      if (incomplete) scheduleRetry()
    }
  }

  // an incomplete placement (owner unreachable, or a Slot still unowned mid-failover) retries after a short delay until it converges
  private def scheduleRetry(): Unit =
    if (!locked(closed)) scheduler.after(reconnect.initialDelay)(shardReconcile.schedule())

  private def closeShard(sub: ShardSub): Unit = {
    locked(shardSubs -= sub)
    sub.placement.reconcile(Map.empty, conns) // detach every placement; the empty plan leaves the ledger empty
    sub.sink.terminate()
    evictEmptyShardConns()
  }

  private def evictEmptyShardConns(): Unit = {
    val candidates = locked(shardConns.iterator.collect { case (node, conn) if conn.isEmpty => node -> conn }.toVector)
    candidates.foreach { case (node, conn) =>
      if (conn.closeIfEmpty()) locked(if (shardConns.get(node).contains(conn)) shardConns -= node)
    }
  }

  // --- shared ------------------------------------------------------------------------------------------------------------------------------

  private def newConnection(node: Node, onTerminated: () => Unit): SubscriptionConnection =
    new SubscriptionConnection(
      nodeFactory(node),
      bootstrap,
      scheduler,
      reconnect,
      watchdog,
      connectTimeoutMillis,
      bufferSize,
      isLive = () => true,
      cluster = true,
      onTerminated = onTerminated
    )

  def close(): Unit = {
    val (classic, shard) =
      locked {
        closed = true
        val c = classicConn
        classicConn = null
        val s = shardConns.values.toVector
        shardConns.clear()
        // terminate sinks before closing connections. This releases any reader waiting because of backpressure before close waits for it.
        (classicSubs.toVector.map(_.sink) ++ shardSubs.toVector.map(_.sink)).foreach(_.terminate())
        classicSubs.clear()
        shardSubs.clear()
        (c, s)
      }
    if (classic != null) classic.close()
    shard.foreach(_.close())
  }

  final private case class ClassicSub(sink: Sink, names: Vector[String], kind: Kind)

  final private class ShardSub(val sink: Sink, val channels: Vector[String]) {
    val placement = new Placement(sink, channels)
  }
}

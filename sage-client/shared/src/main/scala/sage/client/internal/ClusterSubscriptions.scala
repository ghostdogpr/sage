package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.control.NonFatal

import SubscriptionConnection.{Kind, RawSubscription, Sink}

import sage.Bytes
import sage.SageException.NotConnected
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.cluster.{ClusterTopology, Node, Slot}
import sage.commands.Command

/**
  * The cluster pub/sub manager: it owns every subscription Sink and routes each to the right [[SubscriptionConnection]]. Classic (channel and
  * pattern) subscriptions share one connection pinned to an arbitrary master — classic `PUBLISH` broadcasts across the whole cluster bus —
  * re-homed to another master if that node dies. Shard Channel subscriptions take one [[Sharded Subscription Connection]] per owning Node,
  * created on demand and evicted when their last subscription ends.
  *
  * Connections in cluster mode never self-reconnect: a socket loss fires `onTerminated`, and the manager [[reconcile]]s — it refreshes the
  * topology and re-attaches the surviving sinks onto the current owner (the new slot owner for shard, a live master for classic). Reconcile
  * also runs on every topology change ([[onTopologyChanged]]), so a `MOVED` seen on the command path proactively corrects the subscription
  * side. Each `SSUBSCRIBE` is grouped to a single Slot, so a Node owning several slot ranges never triggers `CROSSSLOT`.
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
  private var rehomeRunning                       = false
  private var rehomeQueued                        = false

  // --- sharded state (guarded by lock) ---
  private val shardConns       = mutable.HashMap.empty[Node, SubscriptionConnection]
  private val shardSubs        = mutable.LinkedHashSet.empty[ShardSub]
  private var reconcileRunning = false
  private var reconcileQueued  = false

  private var closed = false

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  private def offload(body: => Unit): Unit = scheduler.after(Duration.Zero)(body)

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
      case e: Throwable => locked(classicSubs -= sub); sink.terminate(); throw e
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
    scheduleRehomeClassic()
  }

  // single-flight (one running, at most one queued): a mid-pass trigger queues a follow-up rather than racing a concurrent pass. Refresh the
  // topology first — the master may be gone, so the re-home target comes from the current topology, not the stale one.
  private def scheduleRehomeClassic(): Unit = {
    val go = locked {
      if (closed) false
      else if (rehomeRunning) { rehomeQueued = true; false }
      else { rehomeRunning = true; true }
    }
    if (go) offload(runRehomeClassicPass())
  }

  private def runRehomeClassicPass(): Unit = {
    refresh()
    rehomeClassic()
    val again = locked(if (!closed && rehomeQueued) {
      rehomeQueued = false; true
    } else { rehomeRunning = false; false })
    if (again) offload(runRehomeClassicPass())
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
      if (conn == null) scheduleRehomeRetry() // no master to re-home onto yet; retry until the topology yields one
      else {
        // attach's establish-failure path resets the connection to Idle and rethrows without firing onTerminated, so a swallowed failure
        // would otherwise strand every classic sub: drop the dead connection and retry rather than wait for a termination that never comes
        var failed = false
        subs.foreach { sub =>
          try {
            conn.attach(sub.sink, sub.names, sub.kind)
            // closed during attach: closeClassic couldn't detach a not-yet-attached sub, so detach here or its channels leak on `conn`
            if (!locked(classicSubs.contains(sub))) { val _ = conn.detach(sub.sink, sub.names, sub.kind) }
          } catch { case NonFatal(_) => failed = true }
        }
        if (failed) {
          // conn may be Live with re-attached subs: shut it down or the retry double-delivers and leaks the socket
          locked(if (classicConn eq conn) classicConn = null)
          conn.shutdown()
          scheduleRehomeRetry()
        }
      }
    }
  }

  private def scheduleRehomeRetry(): Unit =
    if (!locked(closed)) scheduler.after(reconnect.initialDelay)(scheduleRehomeClassic())

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
    // roll back partial placements on failure: closeShard detaches whatever landed (no orphaned SSUBSCRIBE) and terminates the sink
    try place(sub)
    catch { case e: Throwable => closeShard(sub); throw e }
    new RawSubscription(sink, () => closeShard(sub))
  }

  // fail-fast initial placement: an attach failure rolls the whole subscribe back; an unowned Slot is left unplaced and retried, not dropped
  private def place(sub: ShardSub): Unit = {
    if (hasUnownedSlot(sub.channels)) refresh()
    sub.placement.place(planFor(sub.channels), conns)
    if (!sub.placement.fullyPlaced) scheduleRetry()
  }

  // group channels by owning Node, then by Slot so each group becomes one SSUBSCRIBE; an unowned Slot is dropped (the caller refreshes per pass)
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
    // a drop may mean the slot migrated (server sends sunsubscribe then disconnects); force a refresh — stale topology still names the dead
    // owner, which planFor would not see as unowned — then reconcile onto the current owner
    offload { refresh(); scheduleReconcile() }
  }

  def onTopologyChanged(): Unit = scheduleReconcile()

  // single-flight (one running, at most one queued): a mid-pass trigger queues a follow-up rather than racing a concurrent pass that would corrupt the ledger
  private def scheduleReconcile(): Unit = {
    val go = locked {
      if (closed) false
      else if (reconcileRunning) { reconcileQueued = true; false }
      else { reconcileRunning = true; true }
    }
    if (go) offload(runReconcilePass())
  }

  private def runReconcilePass(): Unit = {
    reconcileShard()
    val again = locked(if (!closed && reconcileQueued) {
      reconcileQueued = false; true
    } else { reconcileRunning = false; false })
    if (again) offload(runReconcilePass())
  }

  // re-home each sub onto its channels' current owners. One refresh per pass; an incomplete pass retries so a transient failover failure converges.
  private def reconcileShard(): Unit = {
    val subs = locked(if (closed) Vector.empty else shardSubs.toVector)
    if (subs.nonEmpty) {
      if (subs.exists(sub => hasUnownedSlot(sub.channels))) refresh()
      var incomplete = false
      subs.foreach { sub =>
        val failed = sub.placement.reconcile(planFor(sub.channels), conns)
        // closed during this pass: a racing closeShard may have detached before we re-attached, so reconcile back to empty
        if (!locked(shardSubs.contains(sub))) { val _ = sub.placement.reconcile(Map.empty, conns) }
        // an unowned slot is dropped from the plan, so reconcile reports no failure though the sub isn't placed; retry until fullyPlaced
        else if (failed || !sub.placement.fullyPlaced) incomplete = true
      }
      evictEmptyShardConns()
      if (incomplete) scheduleRetry()
    }
  }

  // an incomplete placement (owner unreachable, or a Slot still unowned mid-failover) retries after a short delay until it converges
  private def scheduleRetry(): Unit =
    if (!locked(closed)) scheduler.after(reconnect.initialDelay)(scheduleReconcile())

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
        // terminate sinks before closing connections so a reader parked on backpressure is released (else conn.close() deadlocks)
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

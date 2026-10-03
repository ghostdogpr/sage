package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.util.Try

import SubscriptionConnection.{Kind, RawSubscription, Sink}

import sage.Bytes
import sage.SageException.NotConnected
import sage.client.SageConfig
import sage.cluster.{ClusterTopology, Node, Slot}

/**
  * Manages pub/sub subscriptions in a cluster. Classic channel and pattern subscriptions share one connection to an arbitrary master
  * because `PUBLISH` broadcasts across the cluster. If that node becomes unavailable or leaves the cluster, the connection reconnects to
  * another master. Shard channel subscriptions use one connection per owning node, created when first needed and closed after its last
  * subscription ends.
  *
  * Shard connections do not reconnect themselves. When one closes, the manager refreshes the topology and assigns its subscribers to the
  * current owner. It also performs this reconciliation after topology changes discovered by commands.
  */
final private[client] class ClusterSubscriptions(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  config: SageConfig,
  topologyOf: () => ClusterTopology,
  refresh: () => Unit,
  pickMaster: () => Option[Node],
  events: Events
) extends SubscriptionConnection.PubSub {

  private val lock = new ReentrantLock()

  // --- sharded state (guarded by lock) ---
  private val shardConns = mutable.HashMap.empty[Node, SubscriptionConnection]
  private val shardSubs  = mutable.LinkedHashSet.empty[ShardSub]

  private var closed = false

  // Retries wait at most four initial delays, so a subscription resumes soon after a long failover ends. They refresh the topology at most
  // once per that delay.
  private val retryBackoff = config.reconnect.copy(maxDelay = config.reconnect.maxDelay.min(config.reconnect.initialDelay * 4))
  private val retryRefresh = new RefreshThrottle(scheduler, retryBackoff.maxDelay.toMillis, refresh)

  // one retry reconciles every subscription, including those placed while it waits
  private val retries = new Reconnects(scheduler, retryBackoff, lock)

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  // one pass at a time; a request during a pass runs one more pass afterward
  private val shardReconcile = new RefreshThrottle(scheduler, 0L, () => reconcileShard())

  // --- classic (channels / patterns) -------------------------------------------------------------------------------------------------------

  private val classicOn = new SubscriptionConnection.Following(nodeFactory, pickMaster)

  // Each attempt connects to the master picked at that time. After a loss, every reconnect attempt refreshes the topology first.
  private val classic = new SubscriptionConnection(
    classicOn.factory,
    scheduler,
    config.copy(reconnect = retryBackoff),
    isLive = () => true,
    SubscriptionConnection.OnLoss.Reconnect(() => retryRefresh(force = false), events, () => classicOn.node, immediately = true)
  )

  def subscribeChannels(channels: Vector[String]): RawSubscription = classic.owned(channels, Kind.Channel, failIfUnconfirmed = false)

  def subscribePatterns(patterns: Vector[String]): RawSubscription = classic.owned(patterns, Kind.Pattern, failIfUnconfirmed = false)

  // `redis-cli --cluster del-node` resets a removed node without stopping it, so PUBLISH stops reaching it while the connection stays open
  def retain(listed: Node => Boolean): Unit = classicOn.retain(listed)

  // --- sharded (shard channels) ------------------------------------------------------------------------------------------------------------

  def subscribeShard(channels: Vector[String]): RawSubscription = {
    val sink = new Sink(channels, Kind.Shard, config.pubsub.bufferSize)
    val sub  = ShardSub(sink)
    locked {
      if (closed) throw NotConnected()
      shardSubs += sub
    }
    // Keep channels with an unowned slot or an unreachable owner pending and retry them. If the owner refuses a channel, closeShard detaches
    // any completed subscriptions and terminates the sink.
    onThrow {
      // a slot with no owner is usually mid-failover, and a refresh may already name its new owner
      val topo = topologyOf()
      if (channels.exists(channel => topo.nodeForSlot(Slot.of(Bytes.utf8(channel))).isEmpty)) refresh()
      if (!reconcile(sub, planFor(sub.channels))) { sink.failure.foreach(throw _); scheduleRetry() }
    }(_ => closeShard(sub))
    new RawSubscription(sink, () => closeShard(sub))
  }

  // Evaluates the plan under the subscription's lock, so a plan from an older topology cannot replace a newer one. No connection is created
  // once the manager is closed.
  private def reconcile(sub: ShardSub, planned: => ClusterSubscriptions.Plan): Boolean = {
    sub.lock.lock()
    try
      ClusterSubscriptions.reconcile(sub.sink, planned, locked(shardConns.toVector), node => locked(Option.unless(closed)(ensureShardConn(node))))
    finally sub.lock.unlock()
  }

  // Group channels by owning node. Omit a channel whose slot is unowned from this attempt; the caller refreshes the topology before each retry.
  private def planFor(channels: Vector[String]): ClusterSubscriptions.Plan = {
    val topo = topologyOf()
    channels.groupBy(channel => topo.nodeForSlot(Slot.of(Bytes.utf8(channel)))).collect { case (Some(node), names) => node -> names }
  }

  // must hold lock
  private def ensureShardConn(node: Node): SubscriptionConnection =
    shardConns.getOrElseUpdate(
      node, {
        // a dropped channel is not a failure, so it re-homes without backoff
        val report =
          SubscriptionConnection.OnLoss.Report(onShardConnTerminated(node, _), () => scheduler.offload(refreshAndReconcile()), () => scheduleRetry())
        new SubscriptionConnection(nodeFactory(node), scheduler, config, () => true, report)
      }
    )

  // a connection that reports its loss late must not remove the connection that replaced it
  private def forget(node: Node, conn: SubscriptionConnection): Unit = locked(if (shardConns.get(node).contains(conn)) shardConns -= node)

  private def onShardConnTerminated(node: Node, conn: SubscriptionConnection): Unit = {
    forget(node, conn)
    scheduleRetry(immediately = true)
  }

  def onTopologyChanged(): Unit = shardReconcile.request()

  // Assign each subscription to the current owners of its channels, and retry incomplete work after transient failover errors.
  private def reconcileShard(): Unit = {
    val subs = locked(if (closed) Vector.empty else shardSubs.toVector)
    if (subs.nonEmpty) {
      var incomplete = false
      subs.foreach { sub =>
        val placed = sub.sink.failure.isEmpty && Try(reconcile(sub, planFor(sub.channels))).getOrElse(false)
        // If the subscription closed during this pass, closeShard may have detached it before reconcile attached it again. Reconcile with an
        // empty plan to remove those attachments, as for a subscription the owner refused.
        if (!locked(shardSubs.contains(sub)) || sub.sink.failure.nonEmpty) { reconcile(sub, Map.empty): Unit }
        else if (!placed) incomplete = true
      }
      evictEmptyShardConns()
      if (incomplete) scheduleRetry() else locked(retries.live())
    }
  }

  // A lost shard connection or an incomplete placement (owner unreachable, or a slot still unowned mid-failover) retries with backoff. A
  // retry refreshes first unless another refreshed within the retry delay, because the topology still names the old owner until a refresh
  // or failover replaces it. With `immediately`, the first retry after a stable period does not wait.
  private def scheduleRetry(immediately: Boolean = false): Unit = locked(retries.schedule(!closed, _ => (), immediately)(refreshAndReconcile()))

  private def refreshAndReconcile(): Unit = {
    retryRefresh(force = false)
    shardReconcile.request()
  }

  private def closeShard(sub: ShardSub): Unit = {
    locked(shardSubs -= sub)
    reconcile(sub, Map.empty): Unit // detach every placement
    sub.sink.terminate()
    evictEmptyShardConns()
  }

  private def evictEmptyShardConns(): Unit = {
    val candidates = locked(shardConns.iterator.collect { case (node, conn) if conn.isEmpty => node -> conn }.toVector)
    candidates.foreach { case (node, conn) =>
      if (conn.closeIfEmpty()) forget(node, conn)
    }
  }

  // --- shared ------------------------------------------------------------------------------------------------------------------------------

  def close(): Unit = {
    val shard =
      locked {
        closed = true
        val s = shardConns.values.toVector
        shardConns.clear()
        // terminate sinks before closing connections. This releases any reader waiting because of backpressure before close waits for it.
        shardSubs.foreach(_.sink.terminate())
        shardSubs.clear()
        s
      }
    shardReconcile.stop()
    classic.close()
    shard.foreach(_.close())
  }

  final private class ShardSub(val sink: Sink) {
    def channels: Vector[String] = sink.names
    val lock                     = new ReentrantLock()
  }
}

private[internal] object ClusterSubscriptions {

  type Plan = Map[Node, Vector[String]]

  // what reconciliation needs of a shard connection; [[SubscriptionConnection]] is the production one
  trait ShardConn {
    def attach(sink: Sink, names: Vector[String]): Unit
    def detach(sink: Sink, names: Vector[String]): Unit
    def namesOf(sink: Sink): Vector[String]
  }

  // Detaches what `sink` holds outside the plan on the `current` connections, attaches every planned node's channels, and returns true when
  // every requested channel is attached. The caller omits unowned slots from the plan and retries until this holds.
  def reconcile(sink: Sink, plan: Plan, current: Vector[(Node, ShardConn)], ensure: Node => Option[ShardConn]): Boolean = {
    current.foreach { case (node, conn) =>
      val keep = plan.getOrElse(node, Vector.empty).toSet
      val gone = conn.namesOf(sink).filterNot(keep)
      if (gone.nonEmpty) conn.detach(sink, gone)
    }
    // a concurrent eviction, a refused channel or a failure to create the connection leaves the channels pending for a retry
    val attached = plan.iterator.flatMap { case (node, names) =>
      Try(ensure(node)).toOption.flatten.filter(conn => Try(conn.attach(sink, names)).isSuccess).iterator.flatMap(_.namesOf(sink))
    }.toSet
    // count distinct attached channels across all nodes. Counting each node separately could hide a missing channel when another is recorded twice.
    attached.size >= sink.names.distinct.size
  }
}

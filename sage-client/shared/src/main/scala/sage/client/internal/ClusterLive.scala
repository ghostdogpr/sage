package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{CommandSpan, Message, PatternMessage, SageEvent, SageException}
import sage.SageException.{ConnectionLost, CrossSlot, NotConnected, ServerError}
import sage.client.{BackoffConfig, ClusterConfig, DedicatedPoolConfig, ReadFrom, SageConfig, WatchdogConfig}
import sage.cluster.{ClusterTopology, Node, NodeGroup, Redirect, RedirectKind, Rejected, Route, Shard, Slot, SplitPlan}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Cluster, Command, Connection, Pipeline, Reply}
import sage.protocol.Frame

/**
  * The cluster runtime: one [[NodeClient]] bundle per master, a refreshable [[ClusterTopology]], and the routing/redirect/failover state
  * machine that executes the pure engine's [[Route]] classifications. The same `Client` type as standalone; only configuration selects it.
  *
  * A Pipeline is split per node (the pure [[ClusterTopology.split]]), each group sent as one batch, and the results scattered back into
  * submission order; positions a stale topology can't resolve fall back to per-command [[dispatch]]. A Transaction leases one Dedicated
  * Connection, pinned lazily to the slot of its first key, and rejects any key on another slot with [[CrossSlot]].
  *
  * Routing and redirect-following run on offloaded virtual threads (never the reply thread), since establishing a node and refreshing the
  * topology both block. A submit's reply callback re-offloads before any blocking continuation.
  */
final private[client] class ClusterLive(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  bootstrap: Vector[Command[?]],
  reconnect: BackoffConfig,
  watchdog: WatchdogConfig,
  connectTimeout: FiniteDuration,
  closeTimeout: FiniteDuration,
  dedicatedPool: DedicatedPoolConfig,
  cluster: ClusterConfig,
  pubsubBufferSize: Int,
  seeds: Vector[Node],
  readFrom: ReadFrom = ReadFrom.Master,
  events: Events = Events.disabled
) extends Client[CIO, String] {

  private val topologyRef = new AtomicReference[ClusterTopology](ClusterTopology.from(Vector.empty))

  // READONLY at setup, so a replica serves reads for its master's slots instead of answering MOVED; separate from the master registry, which
  // drives redirects
  private val replicaPool    = new NodePool(
    nodeFactory,
    scheduler,
    bootstrap :+ Connection.readonly,
    reconnect,
    watchdog,
    connectTimeout,
    closeTimeout,
    dedicatedPool,
    events = events
  )
  // per-master round-robin cursor over its shard's replicas
  private val replicaCursors = new java.util.concurrent.ConcurrentHashMap[Node, java.util.concurrent.atomic.AtomicInteger]()
  private val keylessCursor  = new java.util.concurrent.atomic.AtomicInteger()

  private val subscriptions = new ClusterSubscriptions(
    nodeFactory,
    bootstrap,
    scheduler,
    reconnect,
    watchdog,
    connectTimeout.toMillis,
    pubsubBufferSize,
    () => topologyRef.get(),
    () => refresh(force = true),
    () => pickNode(topologyRef.get())
  )

  // the master registry, driving redirects/refresh; read-write bootstrap (no READONLY) keeps it distinct from replicaPool
  private val masterPool       = new NodePool(
    nodeFactory,
    scheduler,
    bootstrap,
    reconnect,
    watchdog,
    connectTimeout,
    closeTimeout,
    dedicatedPool,
    events = events
  )
  // set once by close; routing refuses afterwards, so close is terminal like the standalone client's
  @volatile private var closed = false

  private val refreshThrottle = new RefreshThrottle(scheduler, cluster.minRefreshInterval.toMillis)

  // if no seed answers, throws the last failure so the first connect surfaces a handshake/TLS error like a standalone connect, not None
  private[client] def bootstrapTopology(): Unit = {
    var lastError: Throwable = NotConnected()
    val candidates           = seeds.iterator
    while (candidates.hasNext) {
      val node = candidates.next()
      try
        querySlotsVia(getOrEstablish(node)) match {
          case Some(shards) => adopt(node, shards); return
          case None         => ()
        }
      catch { case NonFatal(error) => lastError = error }
    }
    closeAll()
    throw lastError
  }

  def run[A](command: Command[A]): CIO[A] = {
    val lease = if (command.isBlocking) new DedicatedPool.Lease else null
    val body  =
      CIO.async[A] { complete =>
        val span = Events.startSpan(events, command)
        offload {
          val tracked = Events.trackCommand(events, command, complete, span)
          Client.completing(tracked)(dispatch(command, cluster.maxRedirects, tracked, lease = lease))
        }
      }
    if (lease != null) CIO.ensure(CIO.blocking(lease.cancel()))(body) else body
  }

  // caching is not applied in cluster mode (per-node tracking through redirects/failover is unsupported): the read runs uncached but on the
  // master (allowReplica = false), so it never honors ReadFrom and the call stays portable; the cacheability guard still applies
  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command))
    else
      CIO.async[A] { complete =>
        val span = Events.startSpan(events, command)
        offload {
          val tracked = Events.trackCommand(events, command, complete, span)
          Client.completing(tracked)(dispatch(command, cluster.maxRedirects, tracked, allowReplica = false))
        }
      }

  // SCAN cursors are node-local, so a full SCAN must sweep every slot-owning master; a reshard mid-scan can still miss or duplicate keys
  def scanTargets: CIO[Vector[ScanTarget]] =
    CIO.blocking {
      val masters = slotOwningMasters(topologyRef.get())
      if (masters.isEmpty) Vector(ScanTarget.any) else masters.map(node => ScanTarget(Some(node)))
    }

  private def slotOwningMasters(topology: ClusterTopology): Vector[Node] =
    topology.shards.collect { case shard if shard.slots.nonEmpty => shard.master }.distinct

  // a SCAN page must resume on the node its cursor came from, so an unreachable node fails the walk rather than rerouting (redirectsLeft = 0):
  // resuming a node-local cursor on another master would scan the wrong keyspace
  def runOn[A](target: ScanTarget, command: Command[A]): CIO[A] =
    target.node match {
      case Some(node) =>
        CIO.async[A] { complete =>
          val span = Events.startSpan(events, command)
          offload {
            val tracked = Events.trackCommand(events, command, complete, span)
            Client.completing(tracked)(sendTo(node, command, asking = false, redirectsLeft = 0, tracked))
          }
        }
      case None       => run(command)
    }

  private[sage] def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out]      = submitPipeline(p).flatMap(TxSupport.collapseStrict(_, p.toOut))
  private[sage] def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R] = submitPipeline(p).map(p.toResults)

  def transaction[A](body: TransactionScope[CIO, String] => CIO[A]): CIO[A] =
    CIO.acquireReleaseWith(acquireScope)(releaseScope)(scope => body(scope))

  // classic subscriptions ride one connection pinned to an arbitrary master (classic PUBLISH broadcasts cluster-wide); the manager re-homes it
  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    CIO.blocking(Client.channelMessages(subscriptions.subscribeChannels(channel +: rest.toVector)))

  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]] =
    CIO.blocking(Client.patternMessages(subscriptions.subscribePatterns(pattern +: rest.toVector)))

  // Shard Channels route per Slot to per-Node Sharded Subscription Connections; resubscription follows ownership on migration/failover
  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    CIO.blocking(Client.channelMessages(subscriptions.subscribeShard(channel +: rest.toVector)))

  def close: CIO[Unit] = CIO.blocking(closeAll())

  // --- routing -------------------------------------------------------------------------------------------------------------------------

  private def dispatch[A](
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    allowReplica: Boolean = true,
    lease: DedicatedPool.Lease = null
  ): Unit =
    if (closed) complete(Failure(NotConnected()))
    else {
      val topology = topologyRef.get()
      if (command.allMasters)
        if (command.aggregate) sendToAllMastersAggregated(topology, command, complete)
        else sendToAllMasters(topology, command, complete)
      else
        topology.route(command) match {
          case Route.ToNode(node, slot) =>
            if (allowReplica && readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command))
              sendRead(command, node, slot, redirectsLeft, complete)
            else sendTo(node, command, asking = false, redirectsLeft, complete, lease)
          case Route.Keyless            =>
            if (allowReplica && readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command))
              sendKeylessRead(topology, command, redirectsLeft, complete)
            else sendToAny(topology, command, redirectsLeft, complete, lease)
          case Route.Unowned(_)         => onUnowned(command, redirectsLeft, complete, lease)
          case Route.CrossSlot(slots)   => complete(Failure(crossSlot(command.name, slots)))
          case Route.Malformed          =>
            complete(Failure(malformedKeys(command.name)))
        }
    }

  // walk the policy's ordered candidates, falling through on connection loss; strict Replica with no live replica exhausts to NotConnected
  private def sendRead[A](command: Command[A], master: Node, slot: Slot, redirectsLeft: Int, complete: Try[A] => Unit): Unit = {
    val replicas = topologyRef.get().shardForSlot(slot).map(_.replicas).getOrElse(Vector.empty)
    val cursor   = replicaCursors.computeIfAbsent(master, _ => new java.util.concurrent.atomic.AtomicInteger())
    tryReadCandidates(command, ReadRouting.candidates(readFrom, master, replicas, cursor.getAndIncrement()), master, redirectsLeft, complete)
  }

  // a keyless eligible read (RANDOMKEY): round-robin over every replica, falling back per policy, so strict Replica never hits a master
  private def sendKeylessRead[A](topology: ClusterTopology, command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    pickNode(topology) match {
      case Some(master) =>
        val replicas = topology.shards.iterator.flatMap(_.replicas).toVector.distinct
        tryReadCandidates(
          command,
          ReadRouting.candidates(readFrom, master, replicas, keylessCursor.getAndIncrement()),
          master,
          redirectsLeft,
          complete
        )
      case None         => complete(Failure(NotConnected()))
    }

  private def tryReadCandidates[A](command: Command[A], candidates: Vector[Node], master: Node, redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    candidates match {
      case node +: rest =>
        val nc =
          try if (node == master) getOrEstablish(node) else replicaPool.getOrEstablish(node)
          catch { case NonFatal(_) => null }
        if (nc == null || !nc.isLive) tryReadCandidates(command, rest, master, redirectsLeft, complete)
        else
          nc.submit[A](
            command,
            asking = false,
            {
              case Success(value) => Events.attributeNode(complete, node); complete(Success(value))
              case Failure(error) => offload(onReadFailure(node, command, error, rest, master, redirectsLeft, complete))
            }
          )
      // strict Replica, all candidates unreachable: refresh so the next read sees the new roster
      case _            => triggerRefresh(); complete(Failure(NotConnected()))
    }

  private def onReadFailure[A](
    node: Node,
    command: Command[A],
    error: Throwable,
    rest: Vector[Node],
    master: Node,
    redirectsLeft: Int,
    complete: Try[A] => Unit
  ): Unit =
    Fault.categorize(error) match {
      case Fault.Redirected(redirect)       =>
        redirect.kind match {
          // strict Replica must not follow ASK onto the importing master (the migrating key is on no replica); MOVED refreshes and re-dispatches
          case RedirectKind.Ask                         =>
            if (readFrom == ReadFrom.Replica) complete(Failure(NotConnected()))
            else onRedirect(node, redirect, command, redirectsLeft, complete)
          case RedirectKind.Moved if redirectsLeft <= 0 =>
            complete(Failure(ServerError("ERR", s"exceeded ${cluster.maxRedirects} cluster redirects for ${command.name}")))
          case RedirectKind.Moved                       => refresh(force = true); offload(dispatch(command, redirectsLeft - 1, complete))
        }
      case Fault.Lost(false)                =>
        if (rest.nonEmpty) tryReadCandidates(command, rest, master, redirectsLeft, complete) else onUnreachable(command, redirectsLeft, complete)
      case Fault.Demoted | Fault.Lost(true) => triggerRefresh(); Events.attributeNode(complete, node); complete(Failure(error))
      case Fault.Fatal                      => Events.attributeNode(complete, node); complete(Failure(error))
    }

  // a broadcast command (SCRIPT LOAD, FUNCTION LOAD, …) runs on every slot-owning master, since a cluster replicates no script/function
  // cache; any node failing fails the command
  private def sendToAllMasters[A](topology: ClusterTopology, command: Command[A], complete: Try[A] => Unit): Unit = {
    val masters = slotOwningMasters(topology)
    if (masters.isEmpty) sendToAny(topology, command, cluster.maxRedirects, complete)
    else {
      val remaining                    = new java.util.concurrent.atomic.AtomicInteger(masters.size)
      val firstError                   = new java.util.concurrent.atomic.AtomicReference[Throwable](null)
      val firstValue                   = new java.util.concurrent.atomic.AtomicReference[Try[A]](null)
      def settle(result: Try[A]): Unit = {
        result match {
          case Success(_) => firstValue.compareAndSet(null, result)
          case Failure(e) => firstError.compareAndSet(null, e)
        }
        if (remaining.decrementAndGet() == 0)
          complete(Option(firstError.get()).map(Failure(_)).getOrElse(firstValue.get()))
      }
      masters.foreach { node =>
        val nc =
          try getOrEstablish(node)
          catch { case NonFatal(_) => null }
        if (nc == null) settle(Failure(NotConnected()))
        else nc.submit[A](command, asking = false, settle)
      }
    }
  }

  // an aggregating broadcast (KEYS): every slot-owning master returns its node-local slice, which we merge as raw frames and decode once,
  // since no single node sees the whole keyspace; any node failing fails the command
  private def sendToAllMastersAggregated[A](topology: ClusterTopology, command: Command[A], complete: Try[A] => Unit): Unit = {
    val masters = slotOwningMasters(topology)
    if (masters.isEmpty) sendToAny(topology, command, cluster.maxRedirects, complete)
    else {
      val raw                                          = command.rawFrame
      val frames                                       = new java.util.concurrent.atomic.AtomicReferenceArray[Frame](masters.size)
      val remaining                                    = new java.util.concurrent.atomic.AtomicInteger(masters.size)
      val firstError                                   = new java.util.concurrent.atomic.AtomicReference[Throwable](null)
      def settle(index: Int, result: Try[Frame]): Unit = {
        result match {
          case Success(frame) => frames.set(index, frame)
          case Failure(e)     => firstError.compareAndSet(null, e)
        }
        if (remaining.decrementAndGet() == 0)
          Option(firstError.get()) match {
            case Some(e) => complete(Failure(e))
            case None    => complete(decodeMerged(command, mergeFrames(frames, masters.size)))
          }
      }
      masters.iterator.zipWithIndex.foreach { case (node, index) =>
        val nc =
          try getOrEstablish(node)
          catch { case NonFatal(_) => null }
        if (nc == null) settle(index, Failure(NotConnected()))
        else nc.submit[Frame](raw, asking = false, result => settle(index, result))
      }
    }
  }

  private def mergeFrames(frames: java.util.concurrent.atomic.AtomicReferenceArray[Frame], size: Int): Frame = {
    val merged = Vector.newBuilder[Frame]
    var i      = 0
    while (i < size) {
      frames.get(i) match {
        case Frame.Array(elements) => merged ++= elements
        case Frame.Set(elements)   => merged ++= elements
        case other                 => merged += other
      }
      i += 1
    }
    Frame.Array(merged.result())
  }

  private def decodeMerged[A](command: Command[A], merged: Frame): Try[A] = Reply.decode(command, merged)

  private def sendToAny[A](
    topology: ClusterTopology,
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease = null
  ): Unit =
    pickNode(topology) match {
      case Some(node) => sendTo(node, command, asking = false, redirectsLeft, complete, lease)
      case None       => complete(Failure(NotConnected()))
    }

  private def sendTo[A](
    node: Node,
    command: Command[A],
    asking: Boolean,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease = null
  ): Unit = {
    val nc =
      try getOrEstablish(node)
      catch { case NonFatal(_) => null }
    if (nc == null) onUnreachable(command, redirectsLeft, complete, lease)
    else
      nc.submit[A](
        command,
        asking,
        {
          case Success(value) => Events.attributeNode(complete, node); complete(Success(value))
          case Failure(error) => offload(onFailure(node, command, error, redirectsLeft, complete, lease))
        },
        lease
      )
  }

  private def onFailure[A](
    node: Node,
    command: Command[A],
    error: Throwable,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease
  ): Unit =
    Fault.categorize(error) match {
      case Fault.Redirected(redirect)       => onRedirect(node, redirect, command, redirectsLeft, complete, lease)
      case Fault.Lost(false)                => onUnreachable(command, redirectsLeft, complete, lease)
      case Fault.Demoted | Fault.Lost(true) => triggerRefresh(); Events.attributeNode(complete, node); complete(Failure(error))
      case Fault.Fatal                      => Events.attributeNode(complete, node); complete(Failure(error))
    }

  private def onRedirect[A](
    from: Node,
    redirect: Redirect,
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease = null
  ): Unit =
    if (redirectsLeft <= 0)
      complete(Failure(ServerError("ERR", s"exceeded ${cluster.maxRedirects} cluster redirects for ${command.name}")))
    else {
      val target = resolve(redirect.target, from)
      redirect.kind match {
        case RedirectKind.Moved => triggerRefresh(); sendTo(target, command, asking = false, redirectsLeft - 1, complete, lease)
        case RedirectKind.Ask   => sendTo(target, command, asking = true, redirectsLeft - 1, complete, lease)
      }
    }

  // the command provably never executed: refresh to adopt the promoted master, then re-route. Jittered backoff paces retries so an in-progress
  // failover is not hammered; redirectsLeft bounds it.
  private def onUnreachable[A](command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit, lease: DedicatedPool.Lease = null): Unit =
    if (redirectsLeft <= 0) complete(Failure(NotConnected()))
    else {
      refresh(force = true)
      val attempt = (cluster.maxRedirects - redirectsLeft).max(0)
      scheduler.after(Backoff.jitteredMillis(reconnect, attempt, scheduler).millis)(dispatch(command, redirectsLeft - 1, complete, lease = lease))
    }

  private def onUnowned[A](command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit, lease: DedicatedPool.Lease): Unit = {
    refresh(force = false)
    val topology = topologyRef.get()
    topology.route(command) match {
      // re-apply the read policy: an eligible read must still go to a replica once the slot resolves, not be pinned to its master
      case Route.ToNode(node, slot)                                                  =>
        if (readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command)) sendRead(command, node, slot, redirectsLeft, complete)
        else sendTo(node, command, asking = false, redirectsLeft, complete, lease)
      // strict Replica must not fall back to a master: refresh and retry (bounded), never sendToAny
      case _ if readFrom == ReadFrom.Replica && ReadRouting.replicaEligible(command) =>
        onUnreachable(command, redirectsLeft, complete, lease)
      // still unowned after refresh: any master, trusting its reply (a MOVED to follow, or CLUSTERDOWN)
      case _                                                                         => sendToAny(topology, command, redirectsLeft, complete, lease)
    }
  }

  // an empty redirect host means "the node I just talked to" (e.g. `MOVED 3999 :6381`)
  private def resolve(target: Node, from: Node): Node = if (target.host.isEmpty) Node(from.host, target.port) else target

  private def pickNode(topology: ClusterTopology): Option[Node] =
    masterPool.firstLiveNode.orElse(topology.shards.headOption.map(_.master))

  private def offload(body: => Unit): Unit = scheduler.after(Duration.Zero)(body)

  private def crossSlot(name: String, slots: Set[Slot]): CrossSlot =
    CrossSlot(s"$name: keys span ${slots.size} slots; a single command must touch exactly one")

  private def malformedKeys(name: String): IllegalArgumentException =
    new IllegalArgumentException(s"$name: declared key positions fall outside its arguments")

  // a transaction never follows a redirect, so the caller's retry depends on the topology actually refreshing — bypass the throttle window
  // (single-flight still collapses concurrent refreshes); ordinary commands re-route, so they use the throttled triggerRefresh
  private def forceRefresh(): Unit = offload(refresh(force = true))

  // --- pipelines (split per node, batch each, merge in submission order) ----------------------------------------------------------------

  private def submitPipeline[Out, R](p: Pipeline[Out, R]): CIO[Vector[Either[SageException, Any]]] =
    if (p.commands.isEmpty)
      CIO.value(Vector.empty)
    // a blocking command is a programmer error: fail the whole effect up front, never a single position
    else if (p.commands.exists(_.isBlocking))
      CIO.fail(new IllegalArgumentException("a Pipeline cannot carry blocking commands; run them individually on the client"))
    // a Pipeline batches per node, so an all-masters command (SCRIPT LOAD, FUNCTION LOAD, KEYS, …) would touch one node only and either
    // break a later key-routed EVALSHA/FCALL or return a partial keyspace; fail up front rather than partially apply it
    else if (p.commands.exists(_.allMasters))
      CIO.fail(
        new IllegalArgumentException(
          "a Pipeline cannot carry an all-masters command (e.g. SCRIPT LOAD, FUNCTION LOAD, KEYS); run it individually on the client"
        )
      )
    else
      CIO.async { complete =>
        offload(runPipeline(p, complete, Events.deferSpans(events, p.commands)))
      }

  // a position a stale topology can't resolve falls back to per-command dispatch; the collector completes once every position lands terminally
  private def runPipeline[Out, R](
    p: Pipeline[Out, R],
    complete: Try[Vector[Either[SageException, Any]]] => Unit,
    deferred: Vector[() => CommandSpan]
  ): Unit = {
    val plan = topologyRef.get().split(p)
    // a malformed command is a programmer error: fail the whole effect before any span is started, never as a per-position result
    plan.rejected.iterator.collectFirst { case (index, Rejected.Malformed) => index } match {
      case Some(index) => complete(Failure(malformedKeys(p.commands(index).name)))
      case None        => dispatchPipeline(p, complete, deferred, plan)
    }
  }

  private def dispatchPipeline[Out, R](
    p: Pipeline[Out, R],
    complete: Try[Vector[Either[SageException, Any]]] => Unit,
    deferred: Vector[() => CommandSpan],
    plan: SplitPlan
  ): Unit = {
    val n                         = p.commands.length
    val collector                 = new TxSupport.IndexedCollector[Either[SageException, Any]](n, complete)
    val emits                     = Vector.tabulate(n) { i =>
      val span = if (deferred.isEmpty) CommandSpan.noop else Events.startDeferred(deferred(i))
      Events.trackCommand[Any](events, p.commands(i), (result: Try[Any]) => collector.set(i, TxSupport.toEither(result)), span)
    }
    // all-or-nothing: reroutes honor the same choice so a slot is never split across master and replica
    val useReplica                = readFrom != ReadFrom.Master && p.commands.forall(ReadRouting.replicaEligible)
    def reroute(index: Int): Unit = offload(dispatch(p.commands(index), cluster.maxRedirects, emits(index), allowReplica = useReplica))

    plan.rejected.foreach {
      case (index, Rejected.CrossSlot(slots)) => emits(index)(Failure(crossSlot(p.commands(index).name, slots)))
      case (index, Rejected.Unowned(_))       => reroute(index) // dispatch refreshes then re-routes
      case (_, Rejected.Malformed)            => ()             // unreachable: the guard above returned
    }
    // keyless positions ride along on the first node group's batch; with no keyed group, dispatch routes each to any node
    if (plan.perNode.isEmpty) plan.keyless.foreach(reroute)
    plan.perNode.zipWithIndex.foreach { case (NodeGroup(node, positions), groupIndex) =>
      // sorted: keep each node's batch in submission order even when keyless positions are folded into the first group
      sendBatch(node, if (groupIndex == 0) (positions ++ plan.keyless).sorted else positions, p, emits, reroute, useReplica)
    }
  }

  // first live read candidate for a shard under the policy, with the node it landed on; (master, null) when none
  private def readConn(master: Node): (Node, NodeClient) = {
    val replicas = topologyRef.get().shards.collectFirst { case s if s.master == master => s.replicas }.getOrElse(Vector.empty)
    val cursor   = replicaCursors.computeIfAbsent(master, _ => new java.util.concurrent.atomic.AtomicInteger())
    val it       = ReadRouting.candidates(readFrom, master, replicas, cursor.getAndIncrement()).iterator
    while (it.hasNext) {
      val node = it.next()
      val nc   =
        try if (node == master) getOrEstablish(node) else replicaPool.getOrEstablish(node)
        catch { case NonFatal(_) => null }
      if (nc != null && nc.isLive) return (node, nc)
    }
    (master, null)
  }

  private def sendBatch[Out, R](
    node: Node,
    indices: Vector[Int],
    p: Pipeline[Out, R],
    emits: Vector[Try[Any] => Unit],
    reroute: Int => Unit,
    useReplica: Boolean
  ): Unit = {
    // the node the batch lands on (a replica when useReplica), so completions attribute the serving node
    val (target, nc)                               =
      if (useReplica) readConn(node)
      else
        (
          node,
          try getOrEstablish(node)
          catch { case NonFatal(_) => null }
        )
    def settle(index: Int, result: Try[Any]): Unit = { Events.attributeNode(emits(index), target); emits(index)(result) }
    val callbacks: Vector[Try[Any] => Unit]        = indices.map { index => (result: Try[Any]) =>
      result match {
        case Success(_)     => settle(index, result)
        case Failure(error) =>
          Fault.categorize(error) match {
            // ASK keeps the slot's owner, so re-routing by topology bounces off the exporting node and burns a redirect; follow it straight
            // to the importing node with ASKING. MOVED and connection loss re-route normally.
            case Fault.Redirected(redirect)       =>
              redirect.kind match {
                // strict Replica must not follow ASK onto the importing master (mirrors the single-read path at onReadFailure)
                case RedirectKind.Ask if useReplica && readFrom == ReadFrom.Replica => settle(index, Failure(NotConnected()))
                case RedirectKind.Ask                                               =>
                  onRedirect(target, redirect, p.commands(index), cluster.maxRedirects, emits(index))
                case RedirectKind.Moved                                             => reroute(index)
              }
            case Fault.Lost(false)                => reroute(index)
            case Fault.Demoted | Fault.Lost(true) => triggerRefresh(); settle(index, result)
            case Fault.Fatal                      => settle(index, result)
          }
      }
    }
    // node unreachable, or not connected when the batch reached it: nothing was sent, so re-route every position individually
    if (nc == null || !nc.submitAll(indices.map(p.commands), callbacks)) indices.foreach(reroute)
  }

  // --- transactions (one leased connection, pinned lazily to the first key's slot) ------------------------------------------------------

  private def acquireScope: CIO[ClusterTxScope] =
    if (closed) CIO.fail(NotConnected()) else CIO.value(new ClusterTxScope)

  private def releaseScope(scope: ClusterTxScope): CIO[Unit] = CIO.blocking(scope.release())

  /**
    * A cluster Transaction scope. It holds no connection until the first key is touched, then leases one Dedicated Connection on that
    * slot's node and pins to the slot; every later key must hash to the pin or fail [[CrossSlot]]. Keyless commands ride the pinned
    * connection (or, before any key, an arbitrary master). Redirects and losses are never followed — they surface and refresh the topology
    * in the background — so the caller retries the whole block, as it already must for a `WATCH` abort.
    */
  final private class ClusterTxScope extends TransactionScope[CIO, String] {

    private val lock                      = new ReentrantLock()
    private var released                  = false
    private var nodeClient: NodeClient    = null
    private var conn: DedicatedConnection = null
    private var pinnedNode: Node          = null
    private var pinnedSlot: Option[Slot]  = None
    private val armed                     = new AtomicBoolean(false)

    def watch[K: KeyCodec](key: K, rest: K*): CIO[Unit] = {
      val command = Connection.watch(key, rest*)
      CIO.async[Unit] { complete =>
        val tracked = Events.trackSpan(events, command, complete)
        offload(withConn(command, tracked) { c => armed.set(true); c.submit(command, faulting(tracked)) })
      }
    }

    def run[A](command: Command[A]): CIO[A] =
      if (command.isBlocking)
        CIO.fail(new IllegalArgumentException("a Transaction cannot run blocking commands; run them individually on the client"))
      else
        CIO.async[A] { complete =>
          val tracked = Events.trackSpan(events, command, complete)
          offload(withConn(command, tracked)(c => c.submit(command, faulting(tracked))))
        }

    def discard: CIO[Unit] =
      CIO.async[Unit] { complete =>
        offload {
          lock.lock()
          try
            if (released) complete(Failure(TxSupport.scopeReleasedError))
            else if (conn == null) complete(Success(())) // nothing leased, nothing watched
            else Client.completing(complete) { armed.set(false); conn.submit(Connection.unwatch, faulting(complete)) }
          finally lock.unlock()
        }
      }

    // a transaction never follows a redirect (that would break MULTI/EXEC atomicity), but on any ownership/connection fault it refreshes in the
    // background so the caller's retry re-pins to the new owner instead of looping on the stale node; a data error refreshes nothing
    private def refreshOnFault(error: Throwable): Unit =
      Fault.categorize(error) match {
        case Fault.Fatal => ()
        case _           => forceRefresh()
      }

    private def faulting[A](complete: Try[A] => Unit): Try[A] => Unit = {
      case failure @ Failure(error) => refreshOnFault(error); complete(failure)
      case success                  => complete(success)
    }

    // EXEC replies arrive as a Success carrying raw frames, so an ownership fault is an error *frame*, invisible to `faulting`; scan the top
    // level and the EXEC array and refresh on any non-terminal one so the caller's retry re-pins
    private def refreshOnExecFault(frames: Vector[Frame]): Unit = {
      val nested = frames.lastOption match { case Some(Frame.Array(elems)) => elems.iterator; case _ => Iterator.empty[Frame] }
      val fault  = (frames.iterator ++ nested).flatMap(TxSupport.errorOf).exists(m => Fault.categorize(ServerError.of(m)) != Fault.Fatal)
      if (fault) forceRefresh()
    }

    private[sage] def exec[Out, R](p: Pipeline[Out, R]): CIO[Option[Out]] =
      runExec(p).flatMap {
        case None          => CIO.value(None)
        case Some(results) => TxSupport.collapseStrict(results, p.toOut).map(Some(_))
      }

    private[sage] def execAttempt[Out, R](p: Pipeline[Out, R]): CIO[Option[R]] =
      runExec(p).map(_.map(p.toResults))

    private def runExec[Out, R](p: Pipeline[Out, R]): CIO[Option[Vector[Either[SageException, Any]]]] =
      if (isReleased)
        CIO.fail(TxSupport.scopeReleasedError)
      else if (p.commands.isEmpty && !armed.get)
        CIO.value(Some(Vector.empty))
      else if (p.commands.exists(_.isBlocking))
        CIO.fail(new IllegalArgumentException("a Transaction cannot carry blocking commands; run them individually on the client"))
      else
        CIO
          .async[Vector[Frame]] { complete =>
            val tracked = Events.trackSpan(events, Connection.multi, complete)
            offload(submitExec(p, tracked))
          }
          .flatMap { frames =>
            armed.set(false) // EXEC clears WATCH/MULTI state server-side whether it committed or aborted
            refreshOnExecFault(frames)
            TxSupport.interpretExec(p.commands, frames)
          }

    // validates the whole pipeline's slots against the pin *before* sending MULTI, so a cross-slot transaction is rejected with nothing on the wire
    private def submitExec[Out, R](p: Pipeline[Out, R], complete: Try[Vector[Frame]] => Unit): Unit = {
      lock.lock()
      try
        if (released) complete(Failure(TxSupport.scopeReleasedError))
        else
          pipelineSlot(p).flatMap(ensureConn) match {
            case Left(error) => refreshOnFault(error); complete(Failure(error))
            case Right(_)    => Client.completing(complete)(conn.submitRaw(Connection.multi +: p.commands :+ Connection.exec, faulting(complete)))
          }
      finally lock.unlock()
    }

    private def withConn[A](command: Command[?], complete: Try[A] => Unit)(use: DedicatedConnection => Unit): Unit = {
      lock.lock()
      try
        if (released) complete(Failure(TxSupport.scopeReleasedError))
        else
          commandSlot(command).flatMap(ensureConn) match {
            case Left(error) => refreshOnFault(error); complete(Failure(error))
            case Right(_)    => Client.completing(complete)(use(conn))
          }
      finally lock.unlock()
    }

    // called under `lock`; the blocking acquire is safe here — the finalizer never runs concurrently with a live block. With a slot: pin its
    // owner on the first key, then require every later key to match; a keyless-acquired pin adopts the slot only if its node already owns it.
    private def ensureConn(slot: Option[Slot]): Either[Throwable, Unit] =
      slot match {
        case Some(s) if conn == null =>
          nodeForSlotRefreshing(s) match {
            case Some(node) => acquireOn(node).map(_ => pinnedSlot = Some(s))
            case None       => Left(NotConnected())
          }
        case Some(s)                 =>
          pinnedSlot match {
            case Some(ps) if ps == s => Right(())
            case Some(ps)            =>
              Left(CrossSlot(s"transaction touches slot ${s.value} but is pinned to slot ${ps.value}; MULTI/EXEC requires a single slot"))
            case None                =>
              if (topologyRef.get().nodeForSlot(s).contains(pinnedNode)) { pinnedSlot = Some(s); Right(()) }
              else Left(CrossSlot(s"transaction touches slot ${s.value} on a node other than its pinned one; MULTI/EXEC requires a single slot"))
          }
        case None if conn != null    => Right(())
        case None                    =>
          pickNode(topologyRef.get()) match {
            case Some(node) => acquireOn(node)
            case None       => Left(NotConnected())
          }
      }

    private def nodeForSlotRefreshing(slot: Slot): Option[Node] =
      topologyRef.get().nodeForSlot(slot).orElse { refresh(force = true); topologyRef.get().nodeForSlot(slot) }

    private def acquireOn(node: Node): Either[Throwable, Unit] =
      try {
        val nc = getOrEstablish(node)
        conn = nc.acquireForTransaction()
        nodeClient = nc
        pinnedNode = node
        Right(())
      } catch {
        case error: SageException => Left(error)
        case NonFatal(_)          => Left(ConnectionLost(mayHaveExecuted = false))
      }

    private def commandSlot(command: Command[?]): Either[Throwable, Option[Slot]] =
      if (command.hasMalformedKeys)
        Left(malformedKeys(command.name))
      else if (command.keyIndices.isEmpty)
        Right(None)
      else {
        val keyIndices = command.keyIndices
        val first      = Slot.of(command.args(keyIndices.head))
        var i          = 1
        var crossed    = false
        while (i < keyIndices.length && !crossed) {
          if (Slot.of(command.args(keyIndices(i))) != first) crossed = true
          i += 1
        }
        if (crossed) Left(crossSlot(command.name, keyIndices.iterator.map(index => Slot.of(command.args(index))).toSet))
        else Right(Some(first))
      }

    private def pipelineSlot[Out, R](p: Pipeline[Out, R]): Either[Throwable, Option[Slot]] = {
      var acc = Option.empty[Slot]
      val it  = p.commands.iterator
      while (it.hasNext)
        commandSlot(it.next()) match {
          case Left(error)       => return Left(error)
          case Right(None)       => ()
          case Right(Some(slot)) =>
            acc match {
              case None       => acc = Some(slot)
              case Some(prev) =>
                if (prev != slot) return Left(CrossSlot("transaction keys span multiple slots; MULTI/EXEC requires a single slot"))
            }
        }
      Right(acc)
    }

    private def isReleased: Boolean = {
      lock.lock()
      try released
      finally lock.unlock()
    }

    // seal against further ops and release the pinned connection (recycle if clean, discard if armed or mid-command); a scope that never
    // touched a key leased nothing
    private[internal] def release(): Unit = {
      lock.lock()
      val (nc, c, reusable) =
        try { released = true; (nodeClient, conn, conn != null && conn.isHealthy && conn.isQuiescent && !armed.get) }
        finally lock.unlock()
      if (nc != null) nc.releaseTransaction(c, reusable)
    }
  }

  private def getOrEstablish(node: Node): NodeClient = masterPool.getOrEstablish(node)

  // --- topology refresh (single-flight, throttled) -------------------------------------------------------------------------------------

  private def triggerRefresh(): Unit = offload(refresh(force = false))

  private def refresh(force: Boolean): Unit =
    refreshThrottle(force)(querySlots(refreshCandidates()).foreach { case (from, shards) => adopt(from, shards) })

  private def refreshCandidates(): Vector[Node] = (masterPool.candidatesByLiveness ++ seeds).distinct

  private def querySlots(candidates: Vector[Node]): Option[(Node, Vector[Shard])] =
    candidates.iterator.flatMap(trySlots).nextOption()

  private def trySlots(node: Node): Option[(Node, Vector[Shard])] =
    try querySlotsVia(getOrEstablish(node)).map(node -> _)
    catch { case NonFatal(_) => None }

  private def querySlotsVia(nc: NodeClient): Option[Vector[Shard]] = {
    val latch   = new CountDownLatch(1)
    val outcome = new AtomicReference[Try[Vector[Shard]]]()
    nc.submit[Vector[Shard]](Cluster.slots, asking = false, result => { outcome.set(result); latch.countDown() })
    if (!latch.await(connectTimeout.toMillis, TimeUnit.MILLISECONDS)) None
    else
      outcome.get() match {
        case Success(shards) => Some(shards)
        case Failure(_)      => None
      }
  }

  // prunes bundles for masters it no longer lists, so a vanished node's reconnect loop cannot leak; an empty announce-IP from CLUSTER SLOTS
  // means "the node I queried", so substitute `from` as redirects do
  private def adopt(from: Node, shards: Vector[Shard]): Unit = {
    val resolved     = shards.map(shard => shard.copy(master = resolve(shard.master, from), replicas = shard.replicas.map(resolve(_, from))))
    val oldTopology  = topologyRef.get()
    val previous     = if (events.emitsEvents) slotOwningMasters(oldTopology).toSet else Set.empty[Node]
    val newTopology  = ClusterTopology.from(resolved)
    topologyRef.set(newTopology)
    // skip the empty -> populated bootstrap transition: discovering the topology at connect is not a change
    if (events.emitsEvents && previous.nonEmpty) {
      val current = slotOwningMasters(newTopology)
      if (current.toSet != previous) events.emit(SageEvent.TopologyChanged(current))
    }
    val masters      = resolved.map(_.master).toSet
    masterPool.retain(masters.contains)
    // prune replica connections and their cursors for replicas the new topology no longer lists, mirroring the master prune
    val replicaNodes = resolved.iterator.flatMap(_.replicas).toSet
    replicaPool.retain(replicaNodes.contains)
    replicaCursors.keySet.removeIf(node => !masters.contains(node))
    // re-home shard subscriptions only when slot ownership changed; else a forced refresh mid-failover loops (refresh -> adopt -> reconcile ->
    // refresh) at RTT. A classic subscription follows the cluster bus, so it re-homes only when its pinned master's socket drops, never here.
    if (!newTopology.sameOwnership(oldTopology)) subscriptions.onTopologyChanged()
  }

  private def closeAll(): Unit = {
    closed = true
    masterPool.close()
    subscriptions.close()
    replicaPool.close()
    events.close()
  }
}

private[client] object ClusterLive {

  def connect(
    config: SageConfig,
    seeds: Vector[Node],
    cluster: ClusterConfig,
    scheduler: Scheduler,
    translate: Throwable => Throwable
  ): CIO[Client[CIO, String]] =
    CIO.blocking[Client[CIO, String]] {
      // cluster validation forces database 0, so this adds no SELECT
      val bootstrap                                               = Bootstrap.commands(config.auth, config.database, config.clientName)
      val factory: Node => MultiplexedConnection.TransportFactory = node => {
        val upgrade = Tls.buildUpgrade(config.tls, node.host, node.port)
        (onFrame, onClosed) => SocketTransport.connect(node.host, node.port, config.connectTimeout, upgrade, onFrame, onClosed)
      }
      val events                                                  = Events(config.listeners, config.tracer)
      val live                                                    = new ClusterLive(
        factory,
        scheduler,
        bootstrap,
        config.reconnect,
        config.watchdog,
        config.connectTimeout,
        config.closeTimeout,
        config.dedicatedPool,
        cluster,
        config.pubsub.bufferSize,
        seeds,
        config.readFrom,
        events
      )
      // translate discovery's handshake/TLS failures here rather than via mapError, which the per-backend CIO alias does not reconcile through
      // `Client`'s invariant type parameter
      try { live.bootstrapTopology(); live }
      catch { case NonFatal(error) => events.close(); throw translate(error) }
    }
}

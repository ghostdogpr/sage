package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Message, PatternMessage, SageEvent, SageException}
import sage.SageException.{ConnectionLost, CrossSlot, NotConnected, ServerError}
import sage.client.{BackoffConfig, ClusterConfig, DedicatedPoolConfig, ReadFrom, SageConfig, WatchdogConfig}
import sage.cluster.{ClusterTopology, Node, NodeGroup, Redirect, RedirectKind, Rejected, Route, Shard, Slot}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Cluster, Command, Connection, Pipeline}
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
) extends Client[CIO] {

  private val topologyRef = new AtomicReference[ClusterTopology](ClusterTopology.from(Vector.empty))

  // replica connections are read-only (READONLY at setup, so a replica serves reads for its master's slots instead of answering MOVED) and
  // carry no Dedicated Pool use — blocking reads stay on the master. A separate pool from the master registry, which drives redirects.
  private val replicaPool    = new NodePool(
    nodeFactory,
    scheduler,
    bootstrap :+ Connection.readonly,
    reconnect,
    watchdog,
    connectTimeout,
    closeTimeout,
    dedicatedPool,
    events
  )
  // per-master round-robin cursor over its shard's replicas, so successive eligible reads spread across them
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

  private val nodesLock        = new ReentrantLock()
  private val established      = mutable.HashMap.empty[Node, NodeClient]
  private val pendingEstablish = mutable.HashMap.empty[Node, ClusterLive.Establish]
  // set once by close; routing and node establishment refuse afterwards, so close is terminal like the standalone client's
  @volatile private var closed = false

  private val refreshThrottle = new RefreshThrottle(scheduler, cluster.minRefreshInterval.toMillis)

  private inline def lockedNodes[A](inline body: A): A = {
    nodesLock.lock()
    try body
    finally nodesLock.unlock()
  }

  // discovers the topology from the seeds; if none answers, throws the last failure so the first connect surfaces a handshake or TLS error
  // the same way a standalone connect would, rather than swallowing it
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

  def run[A](command: Command[A]): CIO[A] =
    CIO.async[A](complete => offload(dispatch(command, cluster.maxRedirects, Events.trackCommand(events, command, complete))))

  // client-side caching in cluster mode (per-node tracking through redirects/failover) is a follow-up; for now a cached read runs without
  // caching so the same call stays portable between standalone and cluster. The cacheability guard still applies, matching standalone. It
  // always targets the master (allowReplica = false): caching is anchored to the master's tracking stream, so it never honors ReadFrom.
  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command))
    else
      CIO.async[A](complete => offload(dispatch(command, cluster.maxRedirects, Events.trackCommand(events, command, complete), allowReplica = false)))

  // a full SCAN must sweep every slot-owning master: SCAN cursors are node-local, so one arbitrary master would silently miss the rest. The
  // node order is fixed from the current snapshot when the walk begins (a reshard mid-scan can still miss or duplicate keys, as on any client).
  def scanTargets: CIO[Vector[ScanTarget]] =
    CIO.blocking {
      val masters = slotOwningMasters(topologyRef.get())
      if (masters.isEmpty) Vector(ScanTarget.any) else masters.map(node => ScanTarget(Some(node)))
    }

  private def slotOwningMasters(topology: ClusterTopology): Vector[Node] =
    topology.shards.collect { case shard if shard.slots.nonEmpty => shard.master }.distinct

  // pinned, redirect-free send: a keyless SCAN page must resume on the same node its cursor came from, so an unreachable node fails the walk
  // rather than rerouting to an arbitrary master (which would resume a node-local cursor on the wrong keyspace)
  def runOn[A](target: ScanTarget, command: Command[A]): CIO[A] =
    target.node match {
      case Some(node) =>
        CIO.async[A](complete => offload(sendTo(node, command, asking = false, redirectsLeft = 0, Events.trackCommand(events, command, complete))))
      case None       => run(command)
    }

  def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out]      = submitPipeline(p).flatMap(TxSupport.collapseStrict(_, p.toOut))
  def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R] = submitPipeline(p).map(p.toResults)

  def transaction[A](body: TransactionScope[CIO] => CIO[A]): CIO[A] =
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

  private def dispatch[A](command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit, allowReplica: Boolean = true): Unit =
    if (closed) complete(Failure(NotConnected()))
    else {
      val topology = topologyRef.get()
      if (command.allMasters) sendToAllMasters(topology, command, complete)
      else
        topology.route(command) match {
          case Route.ToNode(node, slot) =>
            if (allowReplica && readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command))
              sendRead(command, node, slot, redirectsLeft, complete)
            else sendTo(node, command, asking = false, redirectsLeft, complete)
          case Route.Keyless            =>
            if (allowReplica && readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command))
              sendKeylessRead(topology, command, redirectsLeft, complete)
            else sendToAny(topology, command, redirectsLeft, complete)
          case Route.Unowned(_)         => onUnowned(command, redirectsLeft, complete)
          case Route.CrossSlot(slots)   => complete(Failure(crossSlot(command.name, slots)))
          case Route.Malformed          =>
            complete(Failure(malformedKeys(command.name)))
        }
    }

  // an eligible read under a non-Master policy: walk the policy's ordered candidates (master vs this shard's replicas), submitting on the
  // first that establishes live and falling through the rest on a connection loss. The master uses the redirect-driven master registry; a
  // replica uses the read-only replica pool. Strict Replica with no live replica exhausts to NotConnected.
  private def sendRead[A](command: Command[A], master: Node, slot: Slot, redirectsLeft: Int, complete: Try[A] => Unit): Unit = {
    val replicas = topologyRef.get().shardForSlot(slot).map(_.replicas).getOrElse(Vector.empty)
    val cursor   = replicaCursors.computeIfAbsent(master, _ => new java.util.concurrent.atomic.AtomicInteger())
    tryReadCandidates(command, ReadRouting.candidates(readFrom, master, replicas, cursor.getAndIncrement()), master, redirectsLeft, complete)
  }

  // a keyless eligible read (KEYS, RANDOMKEY): round-robin over every replica, falling back per policy, so strict Replica never hits a master
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
    classify(error) match {
      case ClusterLive.Disposition.Reroute             =>
        error match {
          case e: ServerError =>
            Redirect.parse(e.getMessage).get match {
              // ASK hands off to the importing master: *Preferred may follow it with ASKING, but strict Replica must not touch a master, so
              // it fails (the migrating key is not on any replica); MOVED refreshes, then re-dispatch re-applies the policy
              case ask if ask.kind == RedirectKind.Ask =>
                if (readFrom == ReadFrom.Replica) complete(Failure(NotConnected()))
                else onRedirect(node, ask, command, redirectsLeft, complete)
              case _ if redirectsLeft <= 0             =>
                complete(Failure(ServerError("ERR", s"exceeded ${cluster.maxRedirects} cluster redirects for ${command.name}")))
              case _                                   => refresh(force = true); offload(dispatch(command, redirectsLeft - 1, complete))
            }
          // connection loss: try the next candidate, else refresh and re-dispatch
          case _              =>
            if (rest.nonEmpty) tryReadCandidates(command, rest, master, redirectsLeft, complete) else onUnreachable(command, redirectsLeft, complete)
        }
      case ClusterLive.Disposition.RefreshThenTerminal => triggerRefresh(); Events.attributeNode(complete, node); complete(Failure(error))
      case ClusterLive.Disposition.Terminal            => Events.attributeNode(complete, node); complete(Failure(error))
    }

  // a broadcast command (SCRIPT LOAD, FUNCTION LOAD, …) runs on every slot-owning master, since a cluster replicates no script/function
  // cache; any node failing fails the command, and the deterministic replies (SHA, library name, or OK) make agreement automatic
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

  // sends to a live node if one is connected, else any master in the topology; used for keyless commands and as the still-unowned fallback
  private def sendToAny[A](topology: ClusterTopology, command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    pickNode(topology) match {
      case Some(node) => sendTo(node, command, asking = false, redirectsLeft, complete)
      case None       => complete(Failure(NotConnected()))
    }

  private def sendTo[A](node: Node, command: Command[A], asking: Boolean, redirectsLeft: Int, complete: Try[A] => Unit): Unit = {
    val nc =
      try getOrEstablish(node)
      catch { case NonFatal(_) => null }
    if (nc == null) onUnreachable(command, redirectsLeft, complete)
    else
      nc.submit[A](
        command,
        asking,
        {
          case Success(value) => Events.attributeNode(complete, node); complete(Success(value))
          case Failure(error) => offload(onFailure(node, command, error, redirectsLeft, complete))
        }
      )
  }

  private def onFailure[A](node: Node, command: Command[A], error: Throwable, redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    classify(error) match {
      case ClusterLive.Disposition.Reroute             =>
        error match {
          // classify only reroutes a ServerError when it parses as a redirect, so `.get` is safe here
          case e: ServerError => onRedirect(node, Redirect.parse(e.getMessage).get, command, redirectsLeft, complete)
          case _              => onUnreachable(command, redirectsLeft, complete)
        }
      case ClusterLive.Disposition.RefreshThenTerminal =>
        triggerRefresh(); Events.attributeNode(complete, node); complete(Failure(error))
      case ClusterLive.Disposition.Terminal            => Events.attributeNode(complete, node); complete(Failure(error))
    }

  // The single failure taxonomy a routed command can meet, shared by single-command [[onFailure]] and a Pipeline batch's per-position
  // callbacks so the two cannot drift: a redirect or a provably-unexecuted loss is re-routed; a demoted master (`READONLY`) or a
  // may-have-executed loss refreshes the topology but still fails its own command; anything else fails fast in place.
  private def classify(error: Throwable): ClusterLive.Disposition =
    error match {
      case e: ServerError        =>
        if (Redirect.parse(e.getMessage).isDefined) ClusterLive.Disposition.Reroute
        else if (e.code == "READONLY") ClusterLive.Disposition.RefreshThenTerminal
        else ClusterLive.Disposition.Terminal
      case NotConnected()        => ClusterLive.Disposition.Reroute
      case ConnectionLost(false) => ClusterLive.Disposition.Reroute
      case ConnectionLost(true)  => ClusterLive.Disposition.RefreshThenTerminal
      case _                     => ClusterLive.Disposition.Terminal
    }

  private def onRedirect[A](from: Node, redirect: Redirect, command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    if (redirectsLeft <= 0)
      complete(Failure(ServerError("ERR", s"exceeded ${cluster.maxRedirects} cluster redirects for ${command.name}")))
    else {
      val target = resolve(redirect.target, from)
      redirect.kind match {
        case RedirectKind.Moved => triggerRefresh(); sendTo(target, command, asking = false, redirectsLeft - 1, complete)
        case RedirectKind.Ask   => sendTo(target, command, asking = true, redirectsLeft - 1, complete)
      }
    }

  // a routed node was unreachable and the command provably never executed: refresh to adopt the promoted master, then re-route. A growing
  // jittered backoff (and a fresh establish's connect timeout) paces retries so an in-progress failover is not hammered; redirectsLeft bounds it.
  private def onUnreachable[A](command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    if (redirectsLeft <= 0) complete(Failure(NotConnected()))
    else {
      refresh(force = true)
      val attempt = (cluster.maxRedirects - redirectsLeft).max(0)
      scheduler.after(Backoff.jitteredMillis(reconnect, attempt, scheduler).millis)(dispatch(command, redirectsLeft - 1, complete))
    }

  private def onUnowned[A](command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit = {
    refresh(force = false)
    val topology = topologyRef.get()
    topology.route(command) match {
      // re-apply the read policy: an eligible read must still go to a replica once the slot resolves, not be pinned to its master
      case Route.ToNode(node, slot)                                                  =>
        if (readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command)) sendRead(command, node, slot, redirectsLeft, complete)
        else sendTo(node, command, asking = false, redirectsLeft, complete)
      // strict Replica must not fall back to a master: refresh and retry (bounded), never sendToAny
      case _ if readFrom == ReadFrom.Replica && ReadRouting.replicaEligible(command) =>
        onUnreachable(command, redirectsLeft, complete)
      // still unowned after refresh: any master, trusting its reply (a MOVED to follow, or CLUSTERDOWN)
      case _                                                                         => sendToAny(topology, command, redirectsLeft, complete)
    }
  }

  // an empty redirect host means "the node I just talked to" (e.g. `MOVED 3999 :6381`)
  private def resolve(target: Node, from: Node): Node = if (target.host.isEmpty) Node(from.host, target.port) else target

  private def pickNode(topology: ClusterTopology): Option[Node] =
    lockedNodes(established.collectFirst { case (node, nc) if nc.isLive => node })
      .orElse(topology.shards.headOption.map(_.master))

  private def offload(body: => Unit): Unit = scheduler.after(Duration.Zero)(body)

  private def crossSlot(name: String, slots: Set[Slot]): CrossSlot =
    CrossSlot(s"$name: keys span ${slots.size} slots; a single command must touch exactly one")

  private def malformedKeys(name: String): IllegalArgumentException =
    new IllegalArgumentException(s"$name: declared key positions fall outside its arguments")

  // a transaction never follows a redirect, so the caller's retry depends on the topology actually refreshing — bypass the throttle window
  // (the single-flight guard still collapses concurrent refreshes). Ordinary commands use the throttled triggerRefresh, since they re-route.
  private def forceRefresh(): Unit = offload(refresh(force = true))

  // --- pipelines (split per node, batch each, merge in submission order) ----------------------------------------------------------------

  private def submitPipeline[Out, R](p: Pipeline[Out, R]): CIO[Vector[Either[SageException, Any]]] =
    if (p.commands.isEmpty)
      CIO.value(Vector.empty)
    // a blocking command is a programmer error: it fails the whole effect up front, never a single position. Malformed keys are the same
    // kind of error but classified by split, so runPipeline fails on them (the single source, shared with single-command dispatch).
    else if (p.commands.exists(_.isBlocking))
      CIO.fail(new IllegalArgumentException("a Pipeline cannot carry blocking commands; run them individually on the client"))
    // a Pipeline batches per node, so it cannot fan a command out to every master; an all-masters command (SCRIPT LOAD, FUNCTION LOAD, …)
    // would silently load one node only and break a later key-routed EVALSHA/FCALL. Fail up front rather than partially apply it.
    else if (p.commands.exists(_.allMasters))
      CIO.fail(
        new IllegalArgumentException(
          "a Pipeline cannot carry an all-masters command (e.g. SCRIPT LOAD, FUNCTION LOAD); run it individually on the client"
        )
      )
    else
      CIO.async(complete => offload(runPipeline(p, complete)))

  // offloaded (getOrEstablish blocks): split the pipeline, send one batch per node, and scatter each reply into its submission-order slot.
  // A per-command CrossSlot fails only its own slot; a position a stale topology can't resolve (redirect, unreachable node, Unowned) falls
  // back to per-command dispatch. The countdown completes the effect once every position has landed terminally — once, never on a redirect.
  private def runPipeline[Out, R](p: Pipeline[Out, R], complete: Try[Vector[Either[SageException, Any]]] => Unit): Unit = {
    val n                         = p.commands.length
    val collector                 = new TxSupport.IndexedCollector[Either[SageException, Any]](n, complete)
    // one tracking callback per position: the terminal completion for that command, emitting CommandCompleted with the node the routing
    // layer attributes onto it (set in sendBatch for the batch path, or by dispatch->sendTo for a rerouted position)
    val emits                     =
      Vector.tabulate(n)(i => Events.trackCommand[Any](events, p.commands(i), (result: Try[Any]) => collector.set(i, TxSupport.toEither(result))))
    // all-or-nothing: a fully replica-eligible pipeline batches on replicas, else on masters; reroutes honor the same choice so a slot is
    // never split across master and replica
    val useReplica                = readFrom != ReadFrom.Master && p.commands.forall(ReadRouting.replicaEligible)
    def reroute(index: Int): Unit = offload(dispatch(p.commands(index), cluster.maxRedirects, emits(index), allowReplica = useReplica))

    val plan = topologyRef.get().split(p)
    // a malformed command is a programmer error: split is the single source of that classification (the standalone client has no slots and
    // doesn't check), so fail the whole effect before anything reaches the wire, never as a per-position result
    plan.rejected.iterator.collectFirst { case (index, Rejected.Malformed) => index } match {
      case Some(index) => complete(Failure(malformedKeys(p.commands(index).name))); return
      case None        => ()
    }
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

  // first live read candidate for a shard under the policy, with the node it landed on (for attribution); (master, null) when none
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
          classify(error) match {
            case ClusterLive.Disposition.Reroute             =>
              error match {
                // ASK keeps the slot's owner, so re-routing by topology bounces off the exporting node again and burns a redirect; follow it
                // straight to the importing node with ASKING (as single-command dispatch does). MOVED and connection loss re-route normally.
                case e: ServerError =>
                  val redirect = Redirect.parse(e.getMessage).get // classify only reroutes a ServerError that parses as a redirect
                  redirect.kind match {
                    // strict Replica must not follow ASK onto the importing master (mirrors the single-read path at onReadFailure)
                    case RedirectKind.Ask if useReplica && readFrom == ReadFrom.Replica => settle(index, Failure(NotConnected()))
                    case RedirectKind.Ask                                               =>
                      onRedirect(target, redirect, p.commands(index), cluster.maxRedirects, emits(index))
                    case RedirectKind.Moved                                             => reroute(index)
                  }
                case _              => reroute(index)
              }
            case ClusterLive.Disposition.RefreshThenTerminal => triggerRefresh(); settle(index, result)
            case ClusterLive.Disposition.Terminal            => settle(index, result)
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
  final private class ClusterTxScope extends TransactionScope[CIO] {

    private val lock                      = new ReentrantLock()
    private var released                  = false
    private var nodeClient: NodeClient    = null
    private var conn: DedicatedConnection = null
    private var pinnedNode: Node          = null
    private var pinnedSlot: Option[Slot]  = None
    private val armed                     = new AtomicBoolean(false)

    def watch[K: KeyCodec](key: K, rest: K*): CIO[Unit] = {
      val command = Connection.watch(key, rest*)
      CIO.async[Unit](complete => offload(withConn(command, complete) { c => armed.set(true); c.submit(command, faulting(complete)) }))
    }

    def run[A](command: Command[A]): CIO[A] =
      if (command.isBlocking)
        CIO.fail(new IllegalArgumentException("a Transaction cannot run blocking commands; run them individually on the client"))
      else
        CIO.async[A](complete => offload(withConn(command, complete)(c => c.submit(command, faulting(complete)))))

    def discard: CIO[Unit] =
      CIO.async[Unit] { complete =>
        offload {
          lock.lock()
          try
            if (released) complete(Failure(TxSupport.scopeReleasedError))
            else if (conn == null) complete(Success(())) // nothing leased, nothing watched
            else { armed.set(false); conn.submit(Connection.unwatch, faulting(complete)) }
          finally lock.unlock()
        }
      }

    // A transaction never follows a redirect (that would break MULTI/EXEC atomicity), but on any ownership or connection fault — a MOVED/ASK,
    // a READONLY from a demoted master, a lost connection, or a failed acquire — it refreshes the topology in the background so the caller's
    // retry re-pins to the new owner. Without it a retry would loop on the same stale node. A data error (WRONGTYPE, decode) refreshes nothing.
    private def refreshOnFault(error: Throwable): Unit =
      classify(error) match {
        case ClusterLive.Disposition.Terminal => ()
        case _                                => forceRefresh()
      }

    private def faulting[A](complete: Try[A] => Unit): Try[A] => Unit = {
      case failure @ Failure(error) => refreshOnFault(error); complete(failure)
      case success                  => complete(success)
    }

    // EXEC replies arrive as a Success carrying raw frames, so an ownership fault (MOVED/ASK at queue time, READONLY, or one nested in the
    // EXEC array at exec time) is an error *frame*, invisible to `faulting`. Scan those frames — top level and the EXEC array — and refresh
    // the topology on any non-terminal one so the caller's retry re-pins, mirroring `refreshOnFault` for the decoded-reply path.
    private def refreshOnExecFault(frames: Vector[Frame]): Unit = {
      val nested = frames.lastOption match { case Some(Frame.Array(elems)) => elems.iterator; case _ => Iterator.empty[Frame] }
      val fault  = (frames.iterator ++ nested).flatMap(TxSupport.errorOf).exists(m => classify(ServerError.of(m)) != ClusterLive.Disposition.Terminal)
      if (fault) forceRefresh()
    }

    def exec[Out, R](p: Pipeline[Out, R]): CIO[Option[Out]] =
      runExec(p).flatMap {
        case None          => CIO.value(None)
        case Some(results) => TxSupport.collapseStrict(results, p.toOut).map(Some(_))
      }

    def execAttempt[Out, R](p: Pipeline[Out, R]): CIO[Option[R]] =
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
          .async[Vector[Frame]](complete => offload(submitExec(p, complete)))
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
            case Right(_)    => conn.submitRaw(Connection.multi +: p.commands :+ Connection.exec, faulting(complete))
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
            case Right(_)    => use(conn)
          }
      finally lock.unlock()
    }

    // lazily leases and pins the connection (called under `lock`; the blocking acquire is safe here — the finalizer never runs concurrently
    // with a live block). With a slot: pin its owner on the first key, then require every later key to match; a keyless-acquired pin adopts
    // the slot only if its node already owns it. Keyless before any key leases an arbitrary master.
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
        val slots = command.keyIndices.iterator.map(index => Slot.of(command.args(index))).toSet
        if (slots.sizeIs > 1) Left(crossSlot(command.name, slots))
        else Right(Some(slots.head))
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
      lock.lock();
      try released
      finally lock.unlock()
    }

    // run once by the lease finalizer: seal against further ops and release the pinned connection (recycle if clean, discard if armed or
    // mid-command). A scope that never touched a key leased nothing, so there is nothing to release.
    private[internal] def release(): Unit = {
      lock.lock()
      val (nc, c, reusable) =
        try { released = true; (nodeClient, conn, conn != null && conn.isHealthy && conn.isQuiescent && !armed.get) }
        finally lock.unlock()
      if (nc != null) nc.releaseTransaction(c, reusable)
    }
  }

  // --- node registry (single-flight establish) -----------------------------------------------------------------------------------------

  private def getOrEstablish(node: Node): NodeClient = {
    var existing: NodeClient          = null
    var waitOn: ClusterLive.Establish = null
    var mine: ClusterLive.Establish   = null
    lockedNodes {
      if (closed) throw NotConnected()
      established.get(node) match {
        case Some(nc) => existing = nc
        case None     =>
          pendingEstablish.get(node) match {
            case Some(p) => waitOn = p
            case None    => mine = new ClusterLive.Establish; pendingEstablish.put(node, mine)
          }
      }
    }
    if (existing != null) existing
    else if (waitOn != null) waitOn.get()
    else
      try {
        val nc    =
          NodeClient.connect(
            nodeFactory(node),
            scheduler,
            bootstrap,
            reconnect,
            watchdog,
            connectTimeout,
            closeTimeout,
            dedicatedPool,
            Some(node),
            events
          )
        // re-check under the lock: a close that landed during the blocking connect must not re-publish this bundle into a closed client
        val stale = lockedNodes { pendingEstablish.remove(node); if (closed) true else { established.put(node, nc); false } }
        if (stale) { nc.close(); throw NotConnected() }
        mine.succeed(nc)
        nc
      } catch {
        case error: Throwable =>
          lockedNodes(pendingEstablish.remove(node))
          mine.fail(error)
          throw error
      }
  }

  // --- topology refresh (single-flight, throttled) -------------------------------------------------------------------------------------

  private def triggerRefresh(): Unit = offload(refresh(force = false))

  private def refresh(force: Boolean): Unit =
    refreshThrottle(force)(querySlots(refreshCandidates()).foreach { case (from, shards) => adopt(from, shards) })

  private def refreshCandidates(): Vector[Node] = {
    val (live, others) = lockedNodes(established.toVector).partition(_._2.isLive)
    (live.map(_._1) ++ others.map(_._1) ++ seeds).distinct
  }

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

  // installs a fresh topology and prunes bundles for masters it no longer lists, so a vanished node's reconnect loop cannot leak. An empty
  // announce-IP from CLUSTER SLOTS means "the node I queried", so substitute `from` the same way redirects do
  private def adopt(from: Node, shards: Vector[Shard]): Unit = {
    val resolved     = shards.map(shard => shard.copy(master = resolve(shard.master, from), replicas = shard.replicas.map(resolve(_, from))))
    val previous     = if (events.enabled) slotOwningMasters(topologyRef.get()).toSet else Set.empty[Node]
    val newTopology  = ClusterTopology.from(resolved)
    topologyRef.set(newTopology)
    // skip the empty -> populated bootstrap transition: discovering the topology at connect is not a change. A real failover/reshard
    // always replaces a non-empty master set, so `previous.nonEmpty` only suppresses the startup event.
    if (events.enabled && previous.nonEmpty) {
      val current = slotOwningMasters(newTopology)
      if (current.toSet != previous) events.emit(SageEvent.TopologyChanged(current))
    }
    val masters      = resolved.map(_.master).toSet
    val gone         = lockedNodes {
      val absent = established.keysIterator.filterNot(masters.contains).toVector
      absent.flatMap(node => established.remove(node))
    }
    gone.foreach(nc => offload(nc.close()))
    // prune replica connections (and their round-robin cursors) for replicas the new topology no longer lists, mirroring the master prune
    val replicaNodes = resolved.iterator.flatMap(_.replicas).toSet
    replicaPool.retain(replicaNodes.contains)
    replicaCursors.keySet.removeIf(node => !masters.contains(node))
    // re-home Shard Channel subscriptions onto the new slot owners; a classic subscription follows the cluster bus, so it only re-homes when
    // its pinned master's socket actually drops (handled by the connection's onTerminated), not on every topology change
    subscriptions.onTopologyChanged()
  }

  private def closeAll(): Unit = {
    val all = lockedNodes { closed = true; val snapshot = established.values.toVector; established.clear(); snapshot }
    subscriptions.close()
    replicaPool.close()
    all.foreach(_.close())
    events.close()
  }
}

private[client] object ClusterLive {

  // the disposition of a routed command's failure: re-route from scratch, refresh the topology but still fail this command, or fail in place
  private enum Disposition {
    case Reroute, RefreshThenTerminal, Terminal
  }

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

  def connect(
    config: SageConfig,
    seeds: Vector[Node],
    cluster: ClusterConfig,
    scheduler: Scheduler,
    translate: Throwable => Throwable
  ): CIO[Client[CIO]] =
    CIO.blocking[Client[CIO]] {
      // same setup as standalone (identification, clientName); cluster validation forces database 0, so this adds no SELECT
      val bootstrap                                               = Client.bootstrapCommands(config.auth, config.database, config.clientName)
      val factory: Node => MultiplexedConnection.TransportFactory = node => {
        val upgrade = Tls.buildUpgrade(config.tls, node.host, node.port)
        (onFrame, onClosed) => SocketTransport.connect(node.host, node.port, config.connectTimeout, upgrade, onFrame, onClosed)
      }
      val events                                                  = Events(config.listeners)
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
      // discovery's handshake/TLS failures are translated here (a NOPROTO/SSL surfaces like a standalone connect) rather than via mapError,
      // which the per-backend CIO alias does not reconcile through `Client`'s invariant type parameter
      try { live.bootstrapTopology(); live }
      catch { case NonFatal(error) => events.close(); throw translate(error) }
    }
}

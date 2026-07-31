package sage.client.internal

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{CommandSpan, Message, Outcome, PatternMessage, SageEvent, SageException}
import sage.SageException.{ConnectionFailed, ConnectionLost, InvalidArgument, NotConnected, TimedOut}
import sage.client.{MasterReplicaConfig, ReadFrom, SageConfig}
import sage.cluster.Node
import sage.codec.ValueCodec
import sage.commands.{Command, Connection, Pipeline, Role, Server}
import sage.ratelimit.Decision

/**
  * The master-replica runtime: a non-cluster deployment of one master and its replicas, discovered from seeds by asking each its `ROLE`.
  * Writes, blocking reads, transactions, and `cached` reads go to the master; ordinary read-only commands route to replicas per the
  * [[ReadFrom]] policy (round-robin, with the policy's fallback). The same `Client` type as standalone and cluster; only the topology selects
  * it.
  *
  * Roles refresh on events (a reconnect-driven command loss, a `READONLY` from the presumed master, a read that can reach no candidate, or a
  * replica-preferred read/pipeline when no replica is known), throttled by `minRefreshInterval`; a timer only comes into it when
  * `topologyRefreshInterval` opts into the background poll. A write that meets a demoted master fails fast (kicking off a re-discovery) so the
  * caller's retry lands on the freshly-discovered master, mirroring the cluster runtime's `READONLY` disposition.
  */
final private[client] class MasterReplicaLive(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  bootstrap: Vector[Command[?]],
  config: SageConfig,
  seeds: Vector[Node],
  masterReplica: MasterReplicaConfig,
  events: Events = Events.disabled
) extends Client[CIO, String] {

  private val readFrom       = config.readFrom
  private val cachingEnabled = config.clientCache.enabled

  // only the master multiplexed connection caches: cached reads run on the master, replicas and dedicated connections never serve them
  private val masterPool  = pool(caching = true)
  private val replicaPool = pool(caching = false)

  private def pool(caching: Boolean): NodePool = {
    val cached        = caching && cachingEnabled
    val poolBootstrap = if (cached) bootstrap :+ Connection.clientTrackingOnOptin else bootstrap
    val cacheMaxBytes = if (cached) config.clientCache.maxBytes else 0L
    new NodePool(
      nodeFactory,
      scheduler,
      poolBootstrap,
      config.reconnect,
      config.watchdog,
      config.connectTimeout,
      config.closeTimeout,
      config.dedicatedPool,
      cacheMaxBytes,
      events,
      dedicatedBootstrap = Some(bootstrap)
    )
  }

  private val masterNodeRef    = new AtomicReference[Node](null)
  private val replicasRef      = new AtomicReference[Vector[Node]](Vector.empty)
  private val reads            = new ReadRouting(masterPool, replicaPool, scheduler, readFrom, () => triggerRefresh())
  @volatile private var closed = false

  private val subLock                                         = new ReentrantLock()
  @volatile private var subscriptions: SubscriptionConnection = null

  private val refreshThrottle = new RefreshThrottle(scheduler, masterReplica.minRefreshInterval.toMillis)

  // --- discovery -----------------------------------------------------------------------------------------------------------------------

  // several supplied endpoints are the topology itself: keep their addresses and discover only their roles; a lone seed retains the original
  // discovery behavior, following the addresses in ROLE
  private val pinnedToSeeds = seeds.sizeIs > 1

  private[client] def bootstrapRoles(): Unit =
    resolveTopology(seeds) match {
      case Right(topology) =>
        installTopology(topology)
        startRefreshPoll()
      case Left(error)     =>
        closeAll()
        throw error
    }

  private def resolveTopology(discoveredCandidates: => Vector[Node]): Either[Throwable, MasterReplicaLive.ResolvedTopology] =
    if (pinnedToSeeds) resolvePinned()
    else resolveDiscovered(discoveredCandidates)

  // classifies every supplied endpoint by its own ROLE; an endpoint that cannot be opened is omitted, but reported because the successful
  // topology resolution otherwise hides its failure from the caller
  private def resolvePinned(): Either[Throwable, MasterReplicaLive.ResolvedTopology] = {
    var lastError: Throwable = NotConnected()
    val roles                = seeds.flatMap { seed =>
      try probeRole(seed).map(seed -> _)
      catch {
        case NonFatal(error) =>
          lastError = error
          None
      }
    }
    roles.collectFirst { case (node, _: Role.Master) => node } match {
      case Some(master)          =>
        Right(MasterReplicaLive.ResolvedTopology(master, roles.collect { case (node, role) if role.isConnectedReplica => node }))
      case None if roles.isEmpty => Left(lastError)
      case None                  => Left(ConnectionFailed("no supplied endpoint reports the master role"))
    }
  }

  // contacts seeds until one answers ROLE, resolving the master and its advertised replicas; this is the original single-seed discovery path
  private def resolveDiscovered(candidates: Vector[Node]): Either[Throwable, MasterReplicaLive.ResolvedTopology] = {
    var lastError: Throwable = NotConnected()
    val it                   = candidates.iterator
    while (it.hasNext) {
      val seed = it.next()
      try
        resolveFrom(seed) match {
          case Some(topology) => return Right(topology)
          case None           => ()
        }
      catch { case NonFatal(error) => lastError = error }
    }
    Left(lastError)
  }

  // probes a node's ROLE; a master answers with its replica list, a replica points at its master (followed once), a sentinel is skipped
  private def resolveFrom(node: Node): Option[MasterReplicaLive.ResolvedTopology] =
    probeRole(node).flatMap {
      case Role.Master(_, replicas)       => Some(MasterReplicaLive.ResolvedTopology(node, replicas.map(r => Node(r.host, r.port))))
      case Role.Replica(host, port, _, _) =>
        val master = Node(host, port)
        probeRole(master).collect { case Role.Master(_, replicas) =>
          MasterReplicaLive.ResolvedTopology(master, replicas.map(r => Node(r.host, r.port)))
        }
      case _: Role.Sentinel               => None
    }

  // a throwaway connection (must not leave a pooled one behind): a connect/handshake failure propagates so bootstrapRoles surfaces it like a
  // standalone connect; a node that handshakes but doesn't answer ROLE yields None
  private def probeRole(node: Node): Option[Role] = {
    val nc =
      try
        NodeClient.connect(
          nodeFactory(node),
          scheduler,
          bootstrap,
          config.reconnect,
          config.watchdog,
          config.connectTimeout,
          config.closeTimeout,
          config.dedicatedPool,
          node = node,
          events = Events.disabled
        )
      catch {
        case NonFatal(error) =>
          reportProbeFailure(node, error)
          throw error
      }
    try
      Bootstrap.awaitReply[Role](config.connectTimeout.toMillis)(callback => nc.submit(Server.role, asking = false, callback)) match {
        case Some(Success(role))  => Some(role)
        case Some(Failure(error)) =>
          reportProbeFailure(node, error)
          None
        case None                 =>
          reportProbeFailure(node, TimedOut(s"ROLE timed out after ${config.connectTimeout.toMillis}ms"))
          None
      }
    finally nc.close()
  }

  private def reportProbeFailure(node: Node, error: Throwable): Unit =
    events.emit(SageEvent.Connection.ConnectFailed(Some(node), error))

  private def triggerRefresh(): Unit = refreshThrottle.trigger(rediscover())

  private def startRefreshPoll(): Unit = refreshThrottle.startPolling(masterReplica.topologyRefreshInterval)(triggerRefresh())

  // forced, but single-flight may collapse this onto an in-flight refresh, so a stale masterNodeRef self-corrects on the next reconnect
  private def refreshRolesBeforeRehome(): Unit = refreshThrottle(force = true)(rediscover())

  // a re-discovery queued before close must not probe ROLE on a connection the close cannot reach
  private def rediscover(): Unit =
    if (!closed) resolveTopology((Option(masterNodeRef.get()).toVector ++ replicasRef.get() ++ seeds).distinct).foreach(installTopology)

  private def installTopology(topology: MasterReplicaLive.ResolvedTopology): Unit = {
    masterNodeRef.set(topology.master)
    replicasRef.set(topology.replicas)
    replicaPool.retain(topology.replicas.toSet.contains)
    masterPool.retain(_ == topology.master)
    reads.retain(_ == topology.master)
  }

  // --- routing -------------------------------------------------------------------------------------------------------------------------

  def run[A](command: Command[A]): CIO[A] = {
    def body(lease: DedicatedPool.Lease): CIO[A] =
      CIO.async[A] { complete =>
        val tracked = Events.trackCommand(events, command, complete)
        Client.completing(tracked) {
          if (readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command)) sendRead(command, tracked)
          else sendMaster(command, tracked, lease)
        }
      }
    Client.withLeaseIfBlocking(command)(body)
  }

  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command))
    else if (!cachingEnabled)
      CIO.async[A] { complete =>
        val tracked = Events.trackCommand(events, command, complete)
        Client.completing(tracked)(sendMaster(command, tracked))
      }
    else
      CIO.async[A] { complete =>
        val deferred = Events.deferSpan(events, command)
        Client.completing(complete)(sendMasterCached(command, ttl.toMillis, complete, deferred))
      }

  private def sendMaster[A](command: Command[A], complete: Try[A] => Unit, lease: DedicatedPool.Lease = null): Unit =
    onMaster(complete)((nc, _, cb) => nc.submit[A](command, asking = false, cb, lease))

  private def sendMasterCached[A](command: Command[A], ttlMillis: Long, complete: Try[A] => Unit, deferred: () => CommandSpan): Unit =
    // a short-circuit (master down) never reaches cachedSubmit, so settle a Failed span the deferred factory would otherwise never start
    onMaster(complete, onDown = () => Events.settleSpan(Events.startDeferred(deferred), Outcome.Failed(NotConnected()))) { (nc, _, cb) =>
      nc.cachedSubmit[A](command, ttlMillis, cb, deferred)
    }

  // run `submit` on the master, completing with node attribution; an ownership fault (a demoted master) kicks a re-discovery.
  // `onDown` fires on any short-circuit that never reaches `submit`.
  private def onMaster[A](complete: Try[A] => Unit, onDown: () => Unit = () => ())(submit: (NodeClient, Node, Try[A] => Unit) => Unit): Unit = {
    if (closed) {
      onDown()
      complete(Failure(NotConnected()))
      return
    }
    val node     = masterNodeRef.get()
    val existing = masterPool.existing(node)
    if (existing != null) submitMaster(existing, node, complete, submit)
    else
      scheduler.offload {
        val nc = masterPool.getOrEstablishOrNull(node)
        if (nc == null) {
          triggerRefresh()
          onDown()
          complete(Failure(NotConnected()))
        } else submitMaster(nc, node, complete, submit)
      }
  }

  private def submitMaster[A](nc: NodeClient, node: Node, complete: Try[A] => Unit, submit: (NodeClient, Node, Try[A] => Unit) => Unit): Unit =
    submit(
      nc,
      node,
      {
        case s @ Success(_) =>
          Events.attributeNode(complete, node)
          complete(s)
        case f @ Failure(e) =>
          if (isOwnershipFault(e)) triggerRefresh()
          Events.attributeNode(complete, node)
          complete(f)
      }
    )

  private def sendRead[A](command: Command[A], complete: Try[A] => Unit): Unit = {
    if (closed) {
      complete(Failure(NotConnected()))
      return
    }
    val master = masterNodeRef.get()
    walkRead(command, reads.candidatesFor(master, replicasRef.get()), master, complete)
  }

  private def walkRead[A](command: Command[A], candidates: Vector[Node], master: Node, complete: Try[A] => Unit): Unit =
    reads.walk(command, candidates, master, complete)((node, error, rest) =>
      onReadFault(node, node == master, error, command, rest, master, complete)
    )

  private def onReadFault[A](
    node: Node,
    isMaster: Boolean,
    error: Throwable,
    command: Command[A],
    rest: Vector[Node],
    master: Node,
    complete: Try[A] => Unit
  ): Unit = {
    if (isMaster && isOwnershipFault(error)) triggerRefresh()
    def fail(): Unit = {
      Events.attributeNode(complete, node)
      complete(Failure(error))
    }
    if (servesNoRead(error))
      if (rest.nonEmpty) walkRead(command, rest, master, complete)
      else {
        triggerRefresh()
        fail()
      }
    else fail()
  }

  private def isOwnershipFault(error: Throwable): Boolean = Fault.categorize(error) match {
    case Fault.Demoted | Fault.Lost(_) => true
    case _                             => false
  }

  // this node cannot answer the read, which says nothing about the read itself
  private def servesNoRead(error: Throwable): Boolean = Fault.categorize(error) match {
    case Fault.Lost(_) => true
    case fault         => fault.selfClearing
  }

  // --- pipelines -----------------------------------------------------------------------------------------------------------------------

  private[sage] def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out]      = submitPipeline(p).flatMap(TxSupport.collapseStrict(_, p.toOut))
  private[sage] def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R] = submitPipeline(p).map(p.toResults)

  private def submitPipeline[Out, R](p: Pipeline[Out, R]): CIO[Vector[Either[SageException, Any]]] =
    if (p.commands.isEmpty) CIO.value(Vector.empty)
    else if (p.commands.exists(_.isBlocking))
      CIO.fail(InvalidArgument("a Pipeline cannot carry blocking commands; run them individually on the client"))
    else
      CIO.async { complete =>
        val spans                                              = Events.startSpans(events, p.commands)
        // all-or-nothing: a fully replica-eligible pipeline batches on a replica, else the master, never split
        val useReplica                                         = readFrom != ReadFrom.Master && p.commands.forall(ReadRouting.replicaEligible)
        def submitOn(picked: Option[(Node, NodeClient)]): Unit = {
          // no reachable node fires no wire fault, so re-discover here or a stale replica set / down master strands the pipeline forever
          if (picked.isEmpty) triggerRefresh()
          val submit = picked match {
            case Some((_, nc)) => nc.submitAll
            case None          => (_: Vector[Command[?]], _: Vector[Try[Any] => Unit]) => false
          }
          Client.submitBatchOnOne(events, p.commands, spans, submit, complete, picked.map(_._1))
        }
        val master                                             = masterNodeRef.get()
        if (useReplica) reads.pickOne(reads.candidatesFor(master, replicasRef.get()), master)(submitOn)
        else {
          val existing = masterPool.existing(master)
          if (existing != null) submitOn(Some((master, existing)))
          else
            scheduler.offload {
              val nc = masterPool.getOrEstablishOrNull(master)
              submitOn(Option(nc).map(master -> _))
            }
        }
      }

  // --- transactions (always on the master) ---------------------------------------------------------------------------------------------

  def transaction[A](body: TransactionScope[CIO, String] => CIO[A]): CIO[A] =
    CIO.acquireReleaseWith(acquireScope)(releaseScope)(lease => CIO.unit.flatMap(_ => body(lease.scope)))

  private def refreshOnTxFault(error: Throwable): Unit = if (isOwnershipFault(error)) triggerRefresh()

  private def acquireScope: CIO[MasterReplicaLive.TxLease] =
    CIO.blocking {
      val nc =
        try masterPool.getOrEstablish(masterNodeRef.get())
        catch {
          case e: SageException =>
            triggerRefresh()
            throw e
          case NonFatal(_)      =>
            triggerRefresh()
            throw ConnectionLost(mayHaveExecuted = false)
        }
      try new MasterReplicaLive.TxLease(new Client.TxScope(nc.acquireForTransaction(), refreshOnTxFault, events), nc)
      catch {
        case e: TimedOut      => throw e
        case e: SageException =>
          triggerRefresh()
          throw e
        case NonFatal(_)      =>
          triggerRefresh()
          throw ConnectionLost(mayHaveExecuted = false)
      }
    }

  private def releaseScope(lease: MasterReplicaLive.TxLease): CIO[Unit] =
    CIO.blocking(lease.nc.releaseTransaction(lease.scope.conn, lease.scope.sealAndReusable()))

  // --- pub/sub (on the master) ---------------------------------------------------------------------------------------------------------

  private def subs(): SubscriptionConnection = {
    var s = subscriptions
    if (s == null) {
      subLock.lock()
      try {
        if (subscriptions == null) {
          // resolves the current master per (re)connect, not once, so onReconnect's refresh re-homes the subscription across a failover
          val rehomingFactory: MultiplexedConnection.TransportFactory =
            (onFrame, onClosed) => nodeFactory(masterNodeRef.get())(onFrame, onClosed)
          subscriptions = new SubscriptionConnection(
            rehomingFactory,
            bootstrap,
            scheduler,
            config.reconnect,
            config.watchdog,
            config.connectTimeout.toMillis,
            config.pubsub.bufferSize,
            // the subscription opens its own socket, so gate on a resolved master, not a live pooled connection a pub/sub-only client never creates
            () => !closed && masterNodeRef.get() != null,
            onReconnect = () => refreshRolesBeforeRehome(),
            events = events
          )
        }
        s = subscriptions
      } finally subLock.unlock()
    }
    s
  }

  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    CIO.blocking(Client.channelMessages(subs().subscribeChannels(channel +: rest.toVector)))

  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]] =
    CIO.blocking(Client.patternMessages(subs().subscribePatterns(pattern +: rest.toVector)))

  // a non-cluster server has no slots, so a Shard Channel rides the one Subscription Connection, exactly as standalone treats it
  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    CIO.blocking(Client.channelMessages(subs().subscribeShard(channel +: rest.toVector)))

  // --- scan / lifecycle ----------------------------------------------------------------------------------------------------------------

  def scanTargets: CIO[Vector[ScanTarget]]                      = CIO.value(Vector(ScanTarget.any))
  def runOn[A](target: ScanTarget, command: Command[A]): CIO[A] = run(command)

  private[sage] def rateLimitAcquire[RK](executor: RateLimitExecutor[RK], subject: RK, cost: Long, peek: Boolean): CIO[Decision] =
    executor.evalSha(this, subject, cost, peek)

  def close: CIO[Unit] = CIO.blocking(closeAll())

  private def closeAll(): Unit = {
    closed = true
    refreshThrottle.stopPolling()
    val s = subscriptions
    if (s != null) s.close()
    masterPool.close()
    replicaPool.close()
    events.close()
  }
}

private[client] object MasterReplicaLive {

  final private[client] case class ResolvedTopology(master: Node, replicas: Vector[Node])

  final private[client] class TxLease(val scope: Client.TxScope, val nc: NodeClient)

  def connect(
    config: SageConfig,
    seeds: Vector[Node],
    masterReplica: MasterReplicaConfig,
    scheduler: Scheduler,
    translate: Throwable => Throwable
  ): CIO[Client[CIO, String]] =
    CIO.blocking[Client[CIO, String]] {
      val bootstrap                                               = Bootstrap.commands(config.auth, config.database, config.clientName)
      val factory: Node => MultiplexedConnection.TransportFactory = node => {
        val upgrade = Tls.buildUpgrade(config.tls, node.host, node.port)
        (onFrame, onClosed) => SocketTransport.connect(node.host, node.port, config.connectTimeout, upgrade, onFrame, onClosed)
      }
      val events                                                  = Events(config.listeners, config.tracer)
      val live                                                    =
        new MasterReplicaLive(factory, scheduler, bootstrap, config, seeds, masterReplica, events)
      try {
        live.bootstrapRoles()
        live
      } catch {
        case NonFatal(error) =>
          events.close()
          throw translate(error)
      }
    }
}

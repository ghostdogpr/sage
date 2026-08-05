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
  * The runtime for a non-cluster deployment with one master and its replicas. It discovers their roles by sending `ROLE` to the seed nodes.
  * Writes, blocking reads, transactions, and `cached` reads go to the master. Other read-only commands use replicas according to the
  * [[ReadFrom]] policy, including its fallback behavior. Standalone, master-replica, and cluster deployments use the same `Client` type; the
  * configured topology chooses the runtime.
  *
  * The runtime refreshes roles after a command is lost during reconnection, a presumed master returns `READONLY`, a read cannot reach any
  * candidate, or a replica-preferred read or pipeline has no known replica. `minRefreshInterval` limits how often these refreshes run.
  * `topologyRefreshInterval` can also enable periodic refreshes. A write sent to a demoted master fails immediately and starts role discovery.
  * The caller can then retry against the newly discovered master, as it can after a cluster `READONLY` response.
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

  // When several endpoints are supplied, keep those addresses and use discovery only to determine their roles. With one seed, use the
  // addresses returned by ROLE.
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

  // request ROLE from every supplied endpoint. Omit endpoints that cannot be reached, but report their connection failures through events.
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

  // contact candidates until one answers ROLE, then use its advertised master and replica addresses
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

  // use an existing live connection for ROLE when possible. Otherwise, open a temporary connection and close it after the probe.
  private def probeRole(node: Node): Option[Role] = {
    val pooled = pooledFor(node)
    if (pooled != null) {
      val reply = askRole(pooled)
      if (!lostConnection(reply)) return interpretRole(node, reply)
    }
    val nc     = connectForProbe(node)
    try interpretRole(node, askRole(nc))
    finally nc.close()
  }

  private def pooledFor(node: Node): NodeClient = {
    val master = masterPool.existing(node)
    val nc     = if (master != null) master else replicaPool.existing(node)
    if (nc != null && nc.isLive) nc else null
  }

  private def askRole(nc: NodeClient): Option[Try[Role]] =
    Bootstrap.awaitReply[Role](config.connectTimeout.toMillis)(callback => nc.submit(Server.role, asking = false, callback))

  private def interpretRole(node: Node, reply: Option[Try[Role]]): Option[Role] =
    reply match {
      case Some(Success(role))  => Some(role)
      case Some(Failure(error)) =>
        reportProbeFailure(node, error)
        None
      case None                 =>
        reportProbeFailure(node, TimedOut(s"ROLE timed out after ${config.connectTimeout.toMillis}ms"))
        None
    }

  private def lostConnection(reply: Option[Try[Role]]): Boolean =
    reply match {
      case Some(Failure(error)) =>
        Fault.categorize(error) match {
          case Fault.Lost(_) => true
          case _             => false
        }
      case _                    => false
    }

  private def connectForProbe(node: Node): NodeClient = {
    // a refresh can outlive the start of close, and a closed client must not open a new socket
    if (closed) throw NotConnected()
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
  }

  private def reportProbeFailure(node: Node, error: Throwable): Unit =
    events.emit(SageEvent.Connection.ConnectFailed(Some(node), error))

  private val rediscoverWork: () => Unit = () => rediscover()

  private def triggerRefresh(): Unit = refreshThrottle.trigger(rediscoverWork)

  private def startRefreshPoll(): Unit = refreshThrottle.startPolling(masterReplica.topologyRefreshInterval)(triggerRefresh())

  // request an immediate refresh. If another refresh is active, wait for it; a later reconnect requests discovery again if the master changed.
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
    // if no master is available, cachedSubmit is not called. Complete its deferred span here.
    onMaster(complete, onDown = () => Events.settleSpan(Events.startDeferred(deferred), Outcome.Failed(NotConnected()))) { (nc, _, cb) =>
      nc.cachedSubmit[A](command, ttlMillis, cb, deferred)
    }

  // Submit on the master and add its node to the result. Start role discovery if the server is no longer the master. Call onDown when the
  // operation ends before submit is called.
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
    reads.walk(command, candidates, master, complete)((node, error, rest) => onReadFault(ReadRoute(node, master, rest), error, command, complete))

  private def onReadFault[A](
    route: ReadRoute,
    error: Throwable,
    command: Command[A],
    complete: Try[A] => Unit
  ): Unit =
    handleReadFaults(route, Vector(error), RetryExecution.Inline)(
      remaining => walkRead(command, remaining, route.master, complete),
      () => {
        Events.attributeNode(complete, route.node)
        complete(Failure(error))
      }
    )

  final private case class ReadRoute(node: Node, master: Node, remaining: Vector[Node])

  private enum RetryExecution {
    case Inline, Offloaded
  }

  private def handleReadFaults(route: ReadRoute, errors: Vector[Throwable], retryExecution: RetryExecution)(
    retry: Vector[Node] => Unit,
    settle: () => Unit
  ): Unit = {
    val ownershipFault = route.node == route.master && errors.exists(isOwnershipFault)
    if (ownershipFault) triggerRefresh()
    if (errors.exists(servesNoRead)) {
      def continue(): Unit =
        if (route.remaining.nonEmpty) retry(route.remaining)
        else {
          // an ownership fault already requested the same throttled refresh above
          if (!ownershipFault) triggerRefresh()
          settle()
        }
      retryExecution match {
        case RetryExecution.Inline    => continue()
        case RetryExecution.Offloaded => scheduler.offload(continue())
      }
    } else settle()
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
        val spans           = Events.startSpans(events, p.commands)
        // route the whole pipeline to a replica only when every command is eligible. Otherwise, route the whole pipeline to the master.
        val useReplica      = readFrom != ReadFrom.Master && p.commands.forall(ReadRouting.replicaEligible)
        val master          = masterNodeRef.get()
        val refreshOnUnsent = () => triggerRefresh()
        if (useReplica) {
          val batch                                              = new Client.TrackedBatch(events, p.commands, spans, complete)
          def failUnsent(): Unit                                 =
            // without a submission, a connection error cannot trigger role discovery. Refresh roles before failing the batch.
            batch.failUnsent(refreshOnUnsent)
          def submitOn(picked: Option[ReadRouting.Picked]): Unit =
            picked match {
              case Some(ReadRouting.Picked(node, nc, rest)) =>
                val route     = ReadRoute(node, master, rest)
                val attempt   = new TxSupport.IndexedCollector[Try[Any]](
                  p.commands.length,
                  results =>
                    handleReadFaults(route, results.collect { case Failure(error) => error }, RetryExecution.Offloaded)(
                      remaining => reads.pickOne(remaining, master)(submitOn),
                      () => batch.settleAll(node, results)
                    )
                )
                val callbacks = Vector.tabulate(p.commands.length)(i => (result: Try[Any]) => attempt.set(i, result))
                // the selected connection died before reserving the batch; retry the whole batch on the remaining candidates
                if (!nc.submitAll(p.commands, callbacks))
                  if (rest.nonEmpty) reads.pickOne(rest, master)(submitOn)
                  else failUnsent()
              case None                                     => failUnsent()
            }
          reads.pickOne(reads.candidatesFor(master, replicasRef.get()), master)(submitOn)
        } else {
          def submitOn(picked: Option[(Node, NodeClient)]): Unit = {
            val submit = picked match {
              case Some((_, nc)) => nc.submitAll
              case None          => (_: Vector[Command[?]], _: Vector[Try[Any] => Unit]) => false
            }
            Client.submitBatchOnOne(
              events,
              p.commands,
              spans,
              submit,
              complete,
              onUnsent = refreshOnUnsent,
              node = picked.map(_._1)
            )
          }
          val existing                                           = masterPool.existing(master)
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
          // resolve the master for every connection attempt so subscriptions move to the promoted master after failover.
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
            // subscriptions use a separate socket. Wait for master discovery before opening it; a pooled connection is not required.
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

  // master-replica mode uses one subscription connection for all shard channels, as standalone mode does.
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

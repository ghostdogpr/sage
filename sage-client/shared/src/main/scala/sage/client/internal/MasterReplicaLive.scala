package sage.client.internal

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{CommandSpan, Message, Outcome, PatternMessage, SageEvent, SageException}
import sage.SageException.{ConnectionFailed, ConnectionLost, InvalidArgument, NotConnected, TimedOut}
import sage.client.{ReadFrom, SageConfig}
import sage.cluster.Node
import sage.codec.ValueCodec
import sage.commands.{Command, Connection, Pipeline, ReplicaNode, Role, Server}
import sage.ratelimit.Decision

/**
  * The master-replica runtime: a non-cluster deployment of one master and its replicas, discovered from seeds by asking each its `ROLE`.
  * Writes, blocking reads, transactions, and `cached` reads go to the master; ordinary read-only commands route to replicas per the
  * [[ReadFrom]] policy (round-robin, with the policy's fallback). The same `Client` type as standalone and cluster; only the topology selects
  * it.
  *
  * Roles refresh on events — a reconnect-driven command loss, a `READONLY` from the presumed master, or a read that can reach no candidate —
  * throttled by `minRefreshInterval`, never on a timer. The one exception is a degraded topology: while a node has been dropped as unreachable,
  * reads schedule a re-probe on a widening window, since nothing else would ever put it back. A write that meets a demoted master fails fast (kicking off a
  * re-discovery) so the caller's retry lands on the freshly-discovered master, mirroring the cluster runtime's `READONLY` disposition.
  */
final private[client] class MasterReplicaLive(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  bootstrap: Vector[Command[?]],
  config: SageConfig,
  seeds: Vector[Node],
  minRefreshInterval: FiniteDuration,
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
  private val cursor           = new AtomicInteger()
  @volatile private var closed = false

  private val subLock                                         = new ReentrantLock()
  @volatile private var subscriptions: SubscriptionConnection = null

  private val refreshThrottle    = new RefreshThrottle(scheduler, minRefreshInterval.toMillis)
  private val degradedProbeDue   = new AtomicLong(0L)
  private val degradedAttempt    = new AtomicInteger()
  @volatile private var degraded = false
  private val reportedDown       = ConcurrentHashMap.newKeySet[Node]()

  private def offload(body: => Unit): Unit = scheduler.after(Duration.Zero)(body)

  // --- discovery -----------------------------------------------------------------------------------------------------------------------

  /**
    * Several seeds are the topology itself, each classified by its own `ROLE`, and no other address is ever dialled. A lone seed cannot
    * enumerate a deployment, so it discovers from the `ROLE` reply instead.
    */
  private val pinnedToSeeds = seeds.sizeIs > 1

  private[client] def bootstrapRoles(): Unit =
    resolve(seeds) match {
      case Right(roles) => masterNodeRef.set(roles.master); replicasRef.set(roles.replicas)
      case Left(error)  => closeAll(); throw error
    }

  /**
    * `discoveryCandidates` is consulted only when discovering, where a refresh widens the walk to the nodes it already knows.
    */
  private def resolve(discoveryCandidates: Vector[Node]): Either[Throwable, MasterReplicaLive.Roles] =
    if (pinnedToSeeds) resolvePinned() else resolveDiscovered(discoveryCandidates)

  /**
    * Classifies each supplied endpoint by its own `ROLE`, keeping the endpoint itself as the address. Unreachable endpoints are dropped, since a
    * partially available deployment must still connect, as is a replica whose replication stream is not caught up.
    */
  private def resolvePinned(): Either[Throwable, MasterReplicaLive.Roles] = {
    val (roles, lastError) = probeSeeds()
    setDegraded(roles.sizeIs < seeds.size)
    roles.collectFirst { case (node, _: Role.Master) => node } match {
      case Some(master)          => Right(MasterReplicaLive.Roles(master, roles.collect(MasterReplicaLive.onlineReplica)))
      case None if roles.isEmpty => Left(lastError.getOrElse(NotConnected()))
      case None                  =>
        val cause = lastError.fold("")(error => s", last failure: ${error.getMessage}")
        Left(
          ConnectionFailed(
            s"no supplied endpoint reports the master role (probed ${roles.size} of ${seeds.size}$cause): expected briefly during a " +
              "failover, otherwise the master endpoint is missing from the seeds"
          )
        )
    }
  }

  /**
    * Contacts candidates until one resolves a master, taking the replica addresses from its `ROLE` reply. Reports the connect failure that
    * stopped it, so a handshake/TLS error surfaces as it would on a standalone connect, and distinguishes that from a node that answered but
    * named no usable master.
    */
  private def resolveDiscovered(candidates: Vector[Node]): Either[Throwable, MasterReplicaLive.Roles] = {
    var lastError = Option.empty[Throwable]
    var answered  = false
    var resolved  = Option.empty[MasterReplicaLive.Roles]
    val walk      = candidates.iterator
    while (resolved.isEmpty && walk.hasNext) {
      val node = walk.next()
      try {
        val role = probeRole(node)
        if (role.isDefined) { answered = true; markUp(node) }
        resolved = role.flatMap(rolesFrom(node, _))
      } catch { case NonFatal(error) => lastError = Some(error); reportDown(node, error) }
    }
    resolved.toRight(
      lastError.getOrElse(
        if (answered)
          ConnectionFailed(
            "a candidate answered but named no usable master: it reported the sentinel role, or the master it names did not answer as a master"
          )
        else NotConnected()
      )
    )
  }

  private def probeSeeds(): (Vector[(Node, Role)], Option[Throwable]) = {
    val failure = new AtomicReference[Throwable](null)
    val roles   = probeConcurrently(seeds) { seed =>
      try { val role = probeRole(seed); if (role.isDefined) markUp(seed); role }
      catch { case NonFatal(error) => val _ = failure.compareAndSet(null, error); reportDown(seed, error); None }
    }
    (roles, Option(failure.get()))
  }

  // a replica's master is followed once, never transitively
  private def rolesFrom(node: Node, role: Role): Option[MasterReplicaLive.Roles] =
    role match {
      case Role.Master(_, replicas)       => Some(MasterReplicaLive.Roles(node, reachable(replicas)))
      case Role.Replica(host, port, _, _) =>
        val master = Node(host, port)
        try probeRole(master).collect { case Role.Master(_, replicas) => MasterReplicaLive.Roles(master, reachable(replicas)) }
        catch { case NonFatal(error) => reportDown(master, error); None }
      case _: Role.Sentinel               => None
    }

  /**
    * Drops advertised replica addresses that cannot be reached, so no read is routed at an address discovery already found dead. A node holding
    * a live pooled connection counts as reachable without a probe.
    */
  private def reachable(advertised: Vector[ReplicaNode]): Vector[Node] = {
    val nodes           = advertised.map(replica => Node(replica.host, replica.port))
    val (live, unknown) = nodes.partition(node => replicaPool.existingLive(node).isDefined)
    val answered        = probeConcurrently(unknown)(node => if (connects(node)) Some(()) else None).map(_._1).toSet
    val kept            = nodes.filter(node => live.contains(node) || answered.contains(node))
    setDegraded(kept.sizeIs < nodes.size)
    kept
  }

  private def connects(node: Node): Boolean =
    try { connectProbe(node).close(); markUp(node); true }
    catch { case NonFatal(error) => reportDown(node, error); false }

  /**
    * Probes every node at once under one shared deadline, so unreachable addresses cost one connect timeout rather than one apiece. A probe that
    * has not answered by the deadline counts as unreachable, and still closes its own connection when it returns.
    */
  private def probeConcurrently[A](nodes: Vector[Node])(probe: Node => Option[A]): Vector[(Node, A)] =
    if (nodes.sizeIs <= 1) nodes.flatMap(node => probe(node).map(node -> _))
    else {
      val answers = new ConcurrentHashMap[Node, A]()
      val latch   = new CountDownLatch(nodes.size)
      nodes.foreach { node =>
        val _ = Thread
          .ofVirtual()
          .name("sage-probe")
          .start { () =>
            try
              probe(node).foreach { answer =>
                val _ = answers.put(node, answer)
              }
            finally latch.countDown()
          }
      }
      // each probe bounds its own connect and its ROLE wait by connectTimeout, so twice that covers the slowest legitimate answer
      val _       = latch.await(config.connectTimeout.toMillis * 2, TimeUnit.MILLISECONDS)
      nodes.flatMap(node => Option(answers.get(node)).map(node -> _))
    }

  /**
    * A node that fails to establish after discovery leaves the roster; the degraded re-probe is what puts it back.
    */
  private def dropReplica(node: Node): Unit =
    if (replicasRef.get().contains(node)) {
      val _ = replicasRef.updateAndGet(_.filterNot(_ == node))
      setDegraded(true)
    }

  private def setDegraded(value: Boolean): Unit = {
    degraded = value
    if (!value) { degradedAttempt.set(0); degradedProbeDue.set(0L) }
  }

  /**
    * Only a refresh re-admits a dropped node, and a replica-preferring read that falls back to the master fires no other trigger. Consecutive
    * failed re-probes widen the window.
    */
  private def refreshWhileDegraded(): Unit =
    if (degraded) {
      val now = scheduler.nowMillis
      val due = degradedProbeDue.get()
      if (now >= due && degradedProbeDue.compareAndSet(due, now + degradedWindowMillis)) {
        val _ = degradedAttempt.incrementAndGet()
        triggerRefresh()
      }
    }

  private def degradedWindowMillis: Long =
    minRefreshInterval.toMillis * (1L << math.min(degradedAttempt.get(), MasterReplicaLive.maxDegradedBackoffShift))

  private def reportDown(node: Node, error: Throwable): Unit =
    if (reportedDown.add(node)) events.emit(SageEvent.Connection.ConnectFailed(Some(node), error))

  private def markUp(node: Node): Unit = { val _ = reportedDown.remove(node) }

  // a throwaway connection (must not leave a pooled one behind): a connect/handshake failure propagates so bootstrapRoles surfaces it like a
  // standalone connect; a node that handshakes but doesn't answer ROLE yields None
  private def probeRole(node: Node): Option[Role] = {
    val nc = connectProbe(node)
    try {
      val latch   = new CountDownLatch(1)
      val outcome = new AtomicReference[Try[Role]]()
      nc.submit[Role](Server.role, asking = false, result => { outcome.set(result); latch.countDown() })
      if (!latch.await(config.connectTimeout.toMillis, TimeUnit.MILLISECONDS)) None
      else outcome.get() match { case Success(role) => Some(role); case Failure(_) => None }
    } finally nc.close()
  }

  private def connectProbe(node: Node): NodeClient =
    NodeClient.connect(
      nodeFactory(node),
      scheduler,
      bootstrap,
      config.reconnect,
      config.watchdog,
      config.connectTimeout,
      config.closeTimeout,
      config.dedicatedPool,
      node = Some(node),
      events = events
    )

  private def triggerRefresh(): Unit = offload(refreshThrottle(force = false)(rediscover()))

  // forced, but single-flight may collapse this onto an in-flight refresh, so a stale masterNodeRef self-corrects on the next reconnect
  private def refreshRolesBeforeRehome(): Unit = refreshThrottle(force = true)(rediscover())

  private def rediscover(): Unit =
    resolve((Option(masterNodeRef.get()).toVector ++ replicasRef.get() ++ seeds).distinct).foreach { roles =>
      masterNodeRef.set(roles.master)
      replicasRef.set(roles.replicas)
      replicaPool.retain(roles.replicas.toSet.contains)
      masterPool.retain(_ == roles.master)
    }

  // --- routing -------------------------------------------------------------------------------------------------------------------------

  def run[A](command: Command[A]): CIO[A] = {
    def body(lease: DedicatedPool.Lease): CIO[A] =
      CIO.async[A] { complete =>
        val span    = Events.startSpan(events, command)
        val tracked = Events.trackCommand(events, command, complete, span)
        Client.completing(tracked) {
          if (readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command)) sendRead(command, tracked)
          else sendMaster(command, tracked, lease)
        }
      }
    if (!command.isBlocking) body(null)
    // acquire a fresh lease per execution: a lease is single-shot (cancel is terminal), so a captured one would make a re-run of this value hang
    else CIO.acquireReleaseWith(CIO.defer(new DedicatedPool.Lease))(lease => CIO.blocking(lease.cancel()))(body)
  }

  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command))
    else if (!cachingEnabled)
      CIO.async[A] { complete =>
        val span    = Events.startSpan(events, command)
        val tracked = Events.trackCommand(events, command, complete, span)
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
    if (closed) { onDown(); complete(Failure(NotConnected())); return }
    val node     = masterNodeRef.get()
    val existing = masterPool.existing(node)
    if (existing != null) submitMaster(existing, node, complete, submit)
    else
      offload {
        val nc =
          try masterPool.getOrEstablish(node)
          catch { case NonFatal(_) => null }
        if (nc == null) { triggerRefresh(); onDown(); complete(Failure(NotConnected())) }
        else submitMaster(nc, node, complete, submit)
      }
  }

  private def submitMaster[A](nc: NodeClient, node: Node, complete: Try[A] => Unit, submit: (NodeClient, Node, Try[A] => Unit) => Unit): Unit =
    submit(
      nc,
      node,
      {
        case s @ Success(_) => Events.attributeNode(complete, node); complete(s)
        case f @ Failure(e) => if (isOwnershipFault(e)) triggerRefresh(); Events.attributeNode(complete, node); complete(f)
      }
    )

  private def sendRead[A](command: Command[A], complete: Try[A] => Unit): Unit = {
    if (closed) { complete(Failure(NotConnected())); return }
    refreshWhileDegraded()
    val master     = masterNodeRef.get()
    val candidates = ReadRouting.candidates(readFrom, master, replicasRef.get(), cursor.getAndIncrement())
    tryRead(command, candidates, master, complete)
  }

  private def tryRead[A](command: Command[A], candidates: Vector[Node], master: Node, complete: Try[A] => Unit): Unit =
    candidates match {
      case node +: rest =>
        val isMaster = node == master
        val pool     = if (isMaster) masterPool else replicaPool
        val existing = pool.existing(node)
        if (existing != null)
          if (existing.isLive) submitRead(existing, node, isMaster, command, rest, master, complete)
          else tryRead(command, rest, master, complete)
        else
          offload {
            val nc =
              try pool.getOrEstablish(node)
              catch { case NonFatal(_) => null }
            if (nc == null || !nc.isLive) { if (!isMaster) dropReplica(node); tryRead(command, rest, master, complete) }
            else submitRead(nc, node, isMaster, command, rest, master, complete)
          }
      // nothing reached the wire, so no fault event fires: re-discover here or a strict-Replica read stays stuck on a stale replica set
      case _            => triggerRefresh(); complete(Failure(NotConnected()))
    }

  private def submitRead[A](
    nc: NodeClient,
    node: Node,
    isMaster: Boolean,
    command: Command[A],
    rest: Vector[Node],
    master: Node,
    complete: Try[A] => Unit
  ): Unit =
    nc.submit[A](
      command,
      asking = false,
      {
        case s @ Success(_) => Events.attributeNode(complete, node); complete(s)
        case Failure(error) => offload(onReadFault(node, isMaster, error, command, rest, master, complete))
      }
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
    if (isConnLoss(error)) {
      if (rest.nonEmpty) tryRead(command, rest, master, complete)
      else { triggerRefresh(); Events.attributeNode(complete, node); complete(Failure(error)) }
    } else { Events.attributeNode(complete, node); complete(Failure(error)) }
  }

  private def isOwnershipFault(error: Throwable): Boolean = Fault.categorize(error) match {
    case Fault.Demoted | Fault.Lost(_) => true
    case _                             => false
  }

  private def isConnLoss(error: Throwable): Boolean = Fault.categorize(error) match {
    case Fault.Lost(_) => true
    case _             => false
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
        val spans                                      = Events.startSpans(events, p.commands)
        // all-or-nothing: a fully replica-eligible pipeline batches on a replica, else the master, never split
        val useReplica                                 = readFrom != ReadFrom.Master && p.commands.forall(ReadRouting.replicaEligible)
        def submitOn(node: Node, nc: NodeClient): Unit = {
          // no reachable node fires no wire fault, so re-discover here or a stale replica set / down master strands the pipeline forever
          if (nc == null) triggerRefresh()
          val submit     = if (nc == null) (_: Vector[Command[?]], _: Vector[Try[Any] => Unit]) => false else nc.submitAll
          // None when nothing reached the wire, so the batch is not attributed to a node it never landed on
          val attributed = if (nc == null) None else Some(node)
          Client.submitBatchOnOne(events, p.commands, spans, submit, complete, attributed)
        }
        val master                                     = masterNodeRef.get()
        if (useReplica) {
          refreshWhileDegraded()
          val candidates = ReadRouting.candidates(readFrom, master, replicasRef.get(), cursor.getAndIncrement())
          liveEstablished(candidates, master) match {
            case Some((node, nc)) => submitOn(node, nc)
            case None             => offload { val (node, nc) = establishRead(candidates, master); submitOn(node, nc) }
          }
        } else {
          val existing = masterPool.existing(master)
          if (existing != null) submitOn(master, existing)
          else
            offload {
              val nc =
                try masterPool.getOrEstablish(master)
                catch { case NonFatal(_) => null }
              submitOn(master, nc)
            }
        }
      }

  // lock-free walk: the first live established candidate, Some((null, null)) when all are established but none live, and None when an
  // unestablished candidate is met first — only then must the caller offload to establish
  private def liveEstablished(candidates: Vector[Node], master: Node): Option[(Node, NodeClient)] = {
    val it = candidates.iterator
    while (it.hasNext) {
      val node = it.next()
      val pool = if (node == master) masterPool else replicaPool
      val nc   = pool.existing(node)
      if (nc == null) return None
      if (nc.isLive) return Some((node, nc))
    }
    Some((null, null))
  }

  // the first live read candidate, establishing as needed, with the node it landed on; (null, null) when none
  private def establishRead(candidates: Vector[Node], master: Node): (Node, NodeClient) = {
    val it = candidates.iterator
    while (it.hasNext) {
      val node = it.next()
      val pool = if (node == master) masterPool else replicaPool
      val nc   =
        try pool.getOrEstablish(node)
        catch { case NonFatal(_) => null }
      if (nc != null && nc.isLive) return (node, nc)
      if (node != master) dropReplica(node)
    }
    (null, null)
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
          case e: SageException => triggerRefresh(); throw e
          case NonFatal(_)      => triggerRefresh(); throw ConnectionLost(mayHaveExecuted = false)
        }
      try new MasterReplicaLive.TxLease(new Client.TxScope(nc.acquireForTransaction(), refreshOnTxFault, events), nc)
      catch {
        case e: TimedOut      => throw e
        case e: SageException => triggerRefresh(); throw e
        case NonFatal(_)      => triggerRefresh(); throw ConnectionLost(mayHaveExecuted = false)
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
    val s = subscriptions
    if (s != null) s.close()
    masterPool.close()
    replicaPool.close()
    events.close()
  }
}

private[client] object MasterReplicaLive {

  final private[client] case class Roles(master: Node, replicas: Vector[Node])

  // the degraded re-probe window doubles out from minRefreshInterval, at most this many times
  private val maxDegradedBackoffShift = 5

  // ROLE's replication states are none/connect/connecting/sync/connected; only the last is caught up and safe to read from
  private val onlineReplica: PartialFunction[(Node, Role), Node] = { case (node, replica: Role.Replica) if replica.state == "connected" => node }

  final private[client] class TxLease(val scope: Client.TxScope, val nc: NodeClient)

  def connect(
    config: SageConfig,
    seeds: Vector[Node],
    masterReplica: sage.client.MasterReplicaConfig,
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
        new MasterReplicaLive(factory, scheduler, bootstrap, config, seeds, masterReplica.minRefreshInterval, events)
      try { live.bootstrapRoles(); live }
      catch { case NonFatal(error) => events.close(); throw translate(error) }
    }
}

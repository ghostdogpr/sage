package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Message, PatternMessage, SageException}
import sage.SageException.{ConnectionLost, NotConnected, ServerError, TimedOut}
import sage.client.{ReadFrom, SageConfig}
import sage.cluster.Node
import sage.codec.ValueCodec
import sage.commands.{Command, Pipeline, Role, Server}

/**
  * The master-replica runtime: a non-cluster deployment of one master and its replicas, discovered from seeds by asking each its `ROLE`.
  * Writes, blocking reads, transactions, and `cached` reads go to the master; ordinary read-only commands route to replicas per the
  * [[ReadFrom]] policy (round-robin, with the policy's fallback). The same `Client` type as standalone and cluster; only the topology selects
  * it.
  *
  * Roles refresh only on events — a reconnect-driven command loss, a `READONLY` from the presumed master, or a read that can reach no
  * candidate — throttled by `minRefreshInterval`, never on a timer. A write that meets a demoted master fails fast (kicking off a
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
) extends Client[CIO] {

  private val readFrom = config.readFrom

  private val masterPool  = pool()
  private val replicaPool = pool() // non-cluster replicas serve reads without READONLY, so the same plain bootstrap as the master

  private def pool(): NodePool =
    new NodePool(
      nodeFactory,
      scheduler,
      bootstrap,
      config.reconnect,
      config.watchdog,
      config.connectTimeout,
      config.closeTimeout,
      config.dedicatedPool,
      events
    )

  private val masterNodeRef    = new AtomicReference[Node](null)
  private val replicasRef      = new AtomicReference[Vector[Node]](Vector.empty)
  private val cursor           = new AtomicInteger()
  @volatile private var closed = false

  // lazily created on the first subscription, pinned to the master discovered then; classic pub/sub re-homing across a master failover is a
  // follow-up (the connection re-issues its subscriptions on reconnect to the same address, which a stable endpoint repoints)
  private val subLock                                         = new ReentrantLock()
  @volatile private var subscriptions: SubscriptionConnection = null

  private val refreshThrottle = new RefreshThrottle(scheduler, minRefreshInterval.toMillis)

  private def offload(body: => Unit): Unit = scheduler.after(Duration.Zero)(body)

  // --- discovery -----------------------------------------------------------------------------------------------------------------------

  // contacts seeds (and currently-known nodes) until one answers ROLE, resolving the master and its replicas; throws the last failure if
  // none answers, so the first connect surfaces a handshake/TLS error like a standalone connect would
  private[client] def bootstrapRoles(): Unit = {
    var lastError: Throwable = NotConnected()
    val candidates           = seeds.iterator
    var done                 = false
    while (!done && candidates.hasNext) {
      val seed = candidates.next()
      try
        resolveFrom(seed) match {
          case Some((master, replicas)) => masterNodeRef.set(master); replicasRef.set(replicas); done = true
          case None                     => ()
        }
      catch { case NonFatal(error) => lastError = error }
    }
    if (!done) { closeAll(); throw lastError }
  }

  // probes a node's ROLE; a master answers with its replica list, a replica points at its master (followed once), a sentinel is skipped
  private def resolveFrom(node: Node): Option[(Node, Vector[Node])] =
    probeRole(node).flatMap {
      case Role.Master(_, replicas)       => Some(node -> replicas.map(r => Node(r.host, r.port)))
      case Role.Replica(host, port, _, _) =>
        val master = Node(host, port)
        probeRole(master).collect { case Role.Master(_, replicas) => master -> replicas.map(r => Node(r.host, r.port)) }
      case _: Role.Sentinel               => None
    }

  // a throwaway connection (must not leave a pooled one behind): a connect/handshake failure propagates so bootstrapRoles surfaces it like a
  // standalone connect; a node that handshakes but doesn't answer ROLE yields None
  private def probeRole(node: Node): Option[Role] = {
    val nc = NodeClient.connect(
      nodeFactory(node),
      scheduler,
      bootstrap,
      config.reconnect,
      config.watchdog,
      config.connectTimeout,
      config.closeTimeout,
      config.dedicatedPool,
      Some(node),
      events
    )
    try {
      val latch   = new CountDownLatch(1)
      val outcome = new AtomicReference[Try[Role]]()
      nc.submit[Role](Server.role, asking = false, result => { outcome.set(result); latch.countDown() })
      if (!latch.await(config.connectTimeout.toMillis, TimeUnit.MILLISECONDS)) None
      else outcome.get() match { case Success(role) => Some(role); case Failure(_) => None }
    } finally nc.close()
  }

  private def triggerRefresh(): Unit = offload(refreshThrottle(force = false)(rediscover()))

  // adopt the new master/replicas and prune pools for nodes that vanished
  private def rediscover(): Unit = {
    val candidates = (Option(masterNodeRef.get()).toVector ++ replicasRef.get() ++ seeds).distinct
    candidates.iterator
      .flatMap(n =>
        try resolveFrom(n)
        catch { case NonFatal(_) => None }
      )
      .nextOption()
      .foreach { case (master, replicas) =>
        masterNodeRef.set(master)
        replicasRef.set(replicas)
        replicaPool.retain(replicas.toSet.contains)
        masterPool.retain(_ == master)
      }
  }

  // --- routing -------------------------------------------------------------------------------------------------------------------------

  def run[A](command: Command[A]): CIO[A] =
    CIO.async[A] { complete =>
      val tracked = Events.trackCommand(events, command, complete)
      offload(if (readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command)) sendRead(command, tracked) else sendMaster(command, tracked))
    }

  // cached reads always go to the master (caching is anchored to its tracking stream); cluster-style, the cache itself is a follow-up, so
  // the read runs uncached but stays on the master so the call is portable
  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command))
    else CIO.async[A](complete => offload(sendMaster(command, Events.trackCommand(events, command, complete))))

  private def sendMaster[A](command: Command[A], complete: Try[A] => Unit): Unit = {
    if (closed) { complete(Failure(NotConnected())); return }
    val node = masterNodeRef.get()
    val nc   =
      try masterPool.getOrEstablish(node)
      catch { case NonFatal(_) => null }
    if (nc == null) { triggerRefresh(); complete(Failure(NotConnected())) }
    else
      nc.submit[A](
        command,
        asking = false,
        {
          case s @ Success(_) => Events.attributeNode(complete, node); complete(s)
          case f @ Failure(e) => if (isOwnershipFault(e)) triggerRefresh(); Events.attributeNode(complete, node); complete(f)
        }
      )
  }

  private def sendRead[A](command: Command[A], complete: Try[A] => Unit): Unit = {
    if (closed) { complete(Failure(NotConnected())); return }
    val master     = masterNodeRef.get()
    val candidates = ReadRouting.candidates(readFrom, master, replicasRef.get(), cursor.getAndIncrement())
    tryRead(command, candidates, master, complete)
  }

  private def tryRead[A](command: Command[A], candidates: Vector[Node], master: Node, complete: Try[A] => Unit): Unit =
    candidates match {
      case node +: rest =>
        val isMaster = node == master
        val nc       =
          try if (isMaster) masterPool.getOrEstablish(node) else replicaPool.getOrEstablish(node)
          catch { case NonFatal(_) => null }
        if (nc == null || !nc.isLive) tryRead(command, rest, master, complete)
        else
          nc.submit[A](
            command,
            asking = false,
            {
              case s @ Success(_) => Events.attributeNode(complete, node); complete(s)
              case Failure(error) => offload(onReadFault(node, isMaster, error, command, rest, master, complete))
            }
          )
      // nothing reached the wire, so no fault event fires: re-discover here or a strict-Replica read stays stuck on a stale replica set
      case _            => triggerRefresh(); complete(Failure(NotConnected()))
    }

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
    } else { Events.attributeNode(complete, node); complete(Failure(error)) } // data error: terminal in place
  }

  private def isOwnershipFault(error: Throwable): Boolean = error match {
    case e: ServerError    => e.code == "READONLY"
    case NotConnected()    => true
    case ConnectionLost(_) => true
    case _                 => false
  }

  private def isConnLoss(error: Throwable): Boolean = error match {
    case NotConnected()    => true
    case ConnectionLost(_) => true
    case _                 => false
  }

  // --- pipelines -----------------------------------------------------------------------------------------------------------------------

  def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out]      = submitPipeline(p).flatMap(TxSupport.collapseStrict(_, p.toOut))
  def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R] = submitPipeline(p).map(p.toResults)

  private def submitPipeline[Out, R](p: Pipeline[Out, R]): CIO[Vector[Either[SageException, Any]]] =
    if (p.commands.isEmpty) CIO.value(Vector.empty)
    else if (p.commands.exists(_.isBlocking))
      CIO.fail(new IllegalArgumentException("a Pipeline cannot carry blocking commands; run them individually on the client"))
    else
      CIO.async { complete =>
        offload {
          // all-or-nothing: a fully replica-eligible pipeline batches on a replica, else on the master
          val useReplica = readFrom != ReadFrom.Master && p.commands.forall(ReadRouting.replicaEligible)
          val nc         = if (useReplica) readConn() else masterConn()
          // no reachable node fires no wire fault, so re-discover here or a stale replica set / down master strands the pipeline forever
          if (nc == null) triggerRefresh()
          // submitBatchOnOne either way, so a not-connected pipeline still reports a failed completion per position
          val submit     = if (nc == null) (_: Vector[Command[?]], _: Vector[Try[Any] => Unit]) => false else nc.submitAll
          Client.submitBatchOnOne(events, p.commands, submit, complete)
        }
      }

  private def masterConn(): NodeClient =
    try masterPool.getOrEstablish(masterNodeRef.get())
    catch { case NonFatal(_) => null }

  // the first live read candidate under the policy, master or replica (used for a replica-eligible pipeline batch); null when none
  private def readConn(): NodeClient = {
    val master            = masterNodeRef.get()
    val it                = ReadRouting.candidates(readFrom, master, replicasRef.get(), cursor.getAndIncrement()).iterator
    var found: NodeClient = null
    while (found == null && it.hasNext) {
      val node = it.next()
      val nc   =
        try if (node == master) masterPool.getOrEstablish(node) else replicaPool.getOrEstablish(node)
        catch { case NonFatal(_) => null }
      if (nc != null && nc.isLive) found = nc
    }
    found
  }

  // --- transactions (always on the master) ---------------------------------------------------------------------------------------------

  def transaction[A](body: TransactionScope[CIO] => CIO[A]): CIO[A] =
    CIO.acquireReleaseWith(acquireScope)(releaseScope)(lease => body(lease.scope))

  private def acquireScope: CIO[MasterReplicaLive.TxLease] =
    CIO.blocking {
      val nc =
        try masterPool.getOrEstablish(masterNodeRef.get())
        catch { case e: NotConnected => throw e; case NonFatal(_) => throw ConnectionLost(mayHaveExecuted = false) }
      try new MasterReplicaLive.TxLease(new Client.TxScope(nc.acquireForTransaction()), nc)
      catch {
        case e: NotConnected => throw e
        case e: TimedOut     => throw e
        case NonFatal(_)     => throw ConnectionLost(mayHaveExecuted = false)
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
          val master = masterNodeRef.get()
          subscriptions = new SubscriptionConnection(
            nodeFactory(master),
            bootstrap,
            scheduler,
            config.reconnect,
            config.watchdog,
            config.connectTimeout.toMillis,
            config.pubsub.bufferSize,
            () => masterPool.existingLive(masterNodeRef.get()).isDefined
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

  final private[client] class TxLease(val scope: Client.TxScope, val nc: NodeClient)

  def connect(
    config: SageConfig,
    seeds: Vector[Node],
    masterReplica: sage.client.MasterReplicaConfig,
    scheduler: Scheduler,
    translate: Throwable => Throwable
  ): CIO[Client[CIO]] =
    CIO.blocking[Client[CIO]] {
      val bootstrap                                               = Client.bootstrapCommands(config.auth, config.database, config.clientName)
      val factory: Node => MultiplexedConnection.TransportFactory = node => {
        val upgrade = Tls.buildUpgrade(config.tls, node.host, node.port)
        (onFrame, onClosed) => SocketTransport.connect(node.host, node.port, config.connectTimeout, upgrade, onFrame, onClosed)
      }
      val events                                                  = Events(config.listeners)
      val live                                                    =
        new MasterReplicaLive(factory, scheduler, bootstrap, config, seeds, masterReplica.minRefreshInterval, events)
      try { live.bootstrapRoles(); live }
      catch { case NonFatal(error) => events.close(); throw translate(error) }
    }
}

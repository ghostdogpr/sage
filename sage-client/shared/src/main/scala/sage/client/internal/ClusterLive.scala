package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.SageException.{ConnectionLost, CrossSlot, NotConnected, ServerError}
import sage.client.{BackoffConfig, ClusterConfig, DedicatedPoolConfig, SageConfig, WatchdogConfig}
import sage.cluster.{ClusterTopology, Node, Redirect, RedirectKind, Route, Shard}
import sage.codec.ValueCodec
import sage.commands.{Cluster, Command, Connection, Pipeline}

/**
  * The cluster runtime: one [[NodeClient]] bundle per master, a refreshable [[ClusterTopology]], and the routing/redirect/failover state
  * machine that executes the pure engine's [[Route]] classifications. The same `Client` type as standalone; only configuration selects it.
  * Pipelines and Transactions are rejected in cluster mode.
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
  seeds: Vector[Node]
) extends Client[CIO] {

  private val topologyRef = new AtomicReference[ClusterTopology](ClusterTopology.from(Vector.empty))

  private val nodesLock        = new ReentrantLock()
  private val established      = mutable.HashMap.empty[Node, NodeClient]
  private val pendingEstablish = mutable.HashMap.empty[Node, ClusterLive.Establish]
  // set once by close; routing and node establishment refuse afterwards, so close is terminal like the standalone client's
  @volatile private var closed = false

  private val refreshLock   = new ReentrantLock()
  private val refreshDone   = refreshLock.newCondition()
  private var refreshing    = false
  private val minRefreshMs  = cluster.minRefreshInterval.toMillis
  // bootstrap's CLUSTER SLOTS does not consume the throttle window: seed so the first redirect/READONLY-driven refresh runs immediately,
  // and only refreshes within minRefreshInterval of one another are throttled (Long.MinValue here would overflow the elapsed subtraction)
  private var lastRefreshMs = scheduler.nowMillis - minRefreshMs

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
    CIO.async[A](complete => offload(dispatch(command, cluster.maxRedirects, complete)))

  def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out]               = CIO.fail(unsupported("Pipelines"))
  def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R]          = CIO.fail(unsupported("Pipelines"))
  def transaction[A](body: TransactionScope[CIO] => CIO[A]): CIO[A] = CIO.fail(unsupported("Transactions"))

  // classic and sharded pub/sub in cluster mode arrive with the cluster pub/sub work; standalone subscriptions build the machinery first
  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    CIO.fail(unsupported("Subscriptions"))

  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]] =
    CIO.fail(unsupported("Subscriptions"))

  def close: CIO[Unit] = CIO.blocking(closeAll())

  // a deliberate programmer-facing limitation, outside the sealed hierarchy like the other client guards
  private def unsupported(what: String): UnsupportedOperationException =
    new UnsupportedOperationException(s"$what are not yet supported in cluster mode")

  // --- routing -------------------------------------------------------------------------------------------------------------------------

  private def dispatch[A](command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    if (closed) complete(Failure(NotConnected()))
    else {
      val topology = topologyRef.get()
      topology.route(command) match {
        case Route.ToNode(node, _)  => sendTo(node, command, asking = false, redirectsLeft, complete)
        case Route.Keyless          => sendToAny(topology, command, redirectsLeft, complete)
        case Route.Unowned(_)       => onUnowned(command, redirectsLeft, complete)
        case Route.CrossSlot(slots) =>
          complete(Failure(CrossSlot(s"${command.name}: keys span ${slots.size} slots; a single command must touch exactly one")))
        case Route.Malformed        =>
          complete(Failure(new IllegalArgumentException(s"${command.name}: declared key positions fall outside its arguments")))
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
          case Success(value) => complete(Success(value))
          case Failure(error) => offload(onFailure(node, command, error, redirectsLeft, complete))
        }
      )
  }

  private def onFailure[A](node: Node, command: Command[A], error: Throwable, redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    error match {
      case ServerError(message)  =>
        Redirect.parse(message) match {
          case Some(redirect) => onRedirect(node, redirect, command, redirectsLeft, complete)
          case None           =>
            if (message.startsWith("READONLY")) triggerRefresh() // a demoted master: adopt the new owner for subsequent commands
            complete(Failure(error)) // fail fast, no retry
        }
      case NotConnected()        => onUnreachable(command, redirectsLeft, complete)
      case ConnectionLost(false) => onUnreachable(command, redirectsLeft, complete) // never reached the wire: safe to retry
      case ConnectionLost(true)  => triggerRefresh(); complete(Failure(error))      // may have executed: fail fast
      case other                 => complete(Failure(other))
    }

  private def onRedirect[A](from: Node, redirect: Redirect, command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    if (redirectsLeft <= 0)
      complete(Failure(ServerError(s"exceeded ${cluster.maxRedirects} cluster redirects for ${command.name}")))
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
      case Route.ToNode(node, _) => sendTo(node, command, asking = false, redirectsLeft, complete)
      // still unowned after refresh: send to an arbitrary master and trust the server's reply (a MOVED to follow, or CLUSTERDOWN)
      case _                     => sendToAny(topology, command, redirectsLeft, complete)
    }
  }

  // an empty redirect host means "the node I just talked to" (e.g. `MOVED 3999 :6381`)
  private def resolve(target: Node, from: Node): Node = if (target.host.isEmpty) Node(from.host, target.port) else target

  private def pickNode(topology: ClusterTopology): Option[Node] =
    lockedNodes(established.collectFirst { case (node, nc) if nc.isLive => node })
      .orElse(topology.shards.headOption.map(_.master))

  private def offload(body: => Unit): Unit = scheduler.after(Duration.Zero)(body)

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
        val nc    = NodeClient.connect(nodeFactory(node), scheduler, bootstrap, reconnect, watchdog, connectTimeout, closeTimeout, dedicatedPool)
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

  private def refresh(force: Boolean): Unit = {
    refreshLock.lock()
    val iRefresh =
      try
        if (refreshing) { while (refreshing) refreshDone.awaitUninterruptibly(); false }
        else if (!force && scheduler.nowMillis - lastRefreshMs < minRefreshMs) false
        else { refreshing = true; true }
      finally refreshLock.unlock()

    if (iRefresh)
      try querySlots(refreshCandidates()).foreach { case (from, shards) => adopt(from, shards) }
      finally {
        refreshLock.lock()
        try { refreshing = false; lastRefreshMs = scheduler.nowMillis; refreshDone.signalAll() }
        finally refreshLock.unlock()
      }
  }

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
    val resolved = shards.map(shard => shard.copy(master = resolve(shard.master, from), replicas = shard.replicas.map(resolve(_, from))))
    topologyRef.set(ClusterTopology.from(resolved))
    val masters  = resolved.map(_.master).toSet
    val gone     = lockedNodes {
      val absent = established.keysIterator.filterNot(masters.contains).toVector
      absent.flatMap(node => established.remove(node))
    }
    gone.foreach(nc => offload(nc.close()))
  }

  private def closeAll(): Unit = {
    val all = lockedNodes { closed = true; val snapshot = established.values.toVector; established.clear(); snapshot }
    all.foreach(_.close())
  }
}

private[client] object ClusterLive {

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
      val bootstrap                                               = Vector(Connection.hello(config.auth.map(a => a.username -> a.password)))
      val factory: Node => MultiplexedConnection.TransportFactory = node => {
        val upgrade = Tls.buildUpgrade(config.tls, node.host, node.port)
        (onFrame, onClosed) => SocketTransport.connect(node.host, node.port, config.connectTimeout, upgrade, onFrame, onClosed)
      }
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
        seeds
      )
      // discovery's handshake/TLS failures are translated here (a NOPROTO/SSL surfaces like a standalone connect) rather than via mapError,
      // which the per-backend CIO alias does not reconcile through `Client`'s invariant type parameter
      try { live.bootstrapTopology(); live }
      catch { case NonFatal(error) => throw translate(error) }
    }
}

package sage.client.internal

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import RoutedClient.DispatchMode
import kyo.compat.*

import sage.{SageEvent, SageException}
import sage.SageException.{ConnectionFailed, ConnectionLost, NotConnected, TimedOut}
import sage.client.{MasterReplicaConfig, SageConfig}
import sage.cluster.Node
import sage.commands.{Command, Pipeline, Role, Server}

/**
  * The runtime for a non-cluster deployment with one master and its replicas. It discovers their roles by sending `ROLE` to the seed nodes.
  * Writes, blocking reads, transactions, and `cached` reads go to the master. Other read-only commands use replicas according to the
  * [[sage.client.ReadFrom]] policy, including its fallback behavior. Standalone, master-replica, and cluster deployments use the same `Client` type; the
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
  config: SageConfig,
  seeds: Vector[Node],
  masterReplica: MasterReplicaConfig,
  events: Events = Events.disabled
) extends RoutedClient(
    nodeFactory,
    scheduler,
    MultiplexedConnection.NodeRole.Replica,
    config,
    masterReplica.minRefreshInterval,
    masterReplica.topologyRefreshInterval,
    events
  ) {

  // null until discover installs the first topology
  private val topologyRef = new AtomicReference[MasterReplicaLive.ResolvedTopology](null)

  // resolve the master for every connection attempt so subscriptions move to the promoted master after failover.
  private val subscribedOn = new SubscriptionConnection.Following(nodeFactory, () => Option(topologyRef.get()).map(_.master))

  // master-replica mode uses one subscription connection for all shard channels, as standalone mode does.
  private val subscriptions = new SubscriptionConnection(
    subscribedOn.factory,
    scheduler,
    config,
    // subscriptions use a separate socket. Wait for master discovery before opening it; a pooled connection is not required.
    () => !closed && topologyRef.get() != null,
    // request an immediate refresh. If another refresh is active, wait for it; a later reconnect requests discovery again if the master changed.
    SubscriptionConnection.OnLoss.Reconnect(() => refreshThrottle(force = true), events, () => subscribedOn.node)
  )

  // --- discovery -----------------------------------------------------------------------------------------------------------------------

  // When several endpoints are supplied, keep those addresses and use discovery only to determine their roles. With one seed, use the
  // addresses returned by ROLE.
  private val pinnedToSeeds = seeds.sizeIs > 1

  protected def discover(): Either[Throwable, Unit] = resolveTopology(seeds).map(installTopology)

  private def resolveTopology(discoveredCandidates: => Vector[Node]): Either[Throwable, MasterReplicaLive.ResolvedTopology] =
    if (pinnedToSeeds) resolvePinned()
    else resolveDiscovered(discoveredCandidates)

  // request ROLE from every supplied endpoint. Omit endpoints that cannot be reached, but report their connection failures through events.
  private def resolvePinned(): Either[Throwable, MasterReplicaLive.ResolvedTopology] = {
    val probed = seeds.map(seed => seed -> probeRole(seed))
    val roles  = probed.collect { case (node, Success(role)) => node -> role }
    roles.collectFirst { case (node, _: Role.Master) => node } match {
      case Some(master)          =>
        Right(MasterReplicaLive.ResolvedTopology(master, roles.collect { case (node, role) if role.isConnectedReplica => node }))
      case None if roles.isEmpty => Left(probed.collect { case (_, Failure(error)) => error }.lastOption.getOrElse(NotConnected()))
      case None                  => Left(ConnectionFailed("no supplied endpoint reports the master role"))
    }
  }

  // contact candidates until one answers ROLE, then use its advertised master and replica addresses
  @tailrec private def resolveDiscovered(
    candidates: Vector[Node],
    lastError: Throwable = NotConnected()
  ): Either[Throwable, MasterReplicaLive.ResolvedTopology] =
    candidates match {
      case seed +: rest =>
        resolveFrom(seed) match {
          case Success(Some(topology)) => Right(topology)
          case Success(None)           => resolveDiscovered(rest, lastError)
          case Failure(error)          => resolveDiscovered(rest, error)
        }
      case _            => Left(lastError)
    }

  // probes a node's ROLE; a master answers with its replica list, a replica points at its master (followed once), a sentinel is skipped
  private def resolveFrom(node: Node): Try[Option[MasterReplicaLive.ResolvedTopology]] =
    probeRole(node).flatMap {
      case Role.Master(_, replicas)       => Success(Some(MasterReplicaLive.ResolvedTopology(node, replicas.map(r => Node(r.host, r.port)))))
      case Role.Replica(host, port, _, _) =>
        val master = Node(host, port)
        probeRole(master).map {
          case Role.Master(_, replicas) => Some(MasterReplicaLive.ResolvedTopology(master, replicas.map(r => Node(r.host, r.port))))
          case _                        => None
        }
      case _: Role.Sentinel               => Success(None)
    }

  // use an existing live connection for ROLE when possible. Otherwise, open a temporary connection and close it after the probe.
  private def probeRole(node: Node): Try[Role] = {
    val pooled = pooledFor(node).map(askRole).filterNot(lostConnection)
    // a refresh can outlive the start of close, and a closed client must not open a new socket
    if (pooled.isEmpty && closed) Failure(NotConnected())
    else {
      val reply = pooled.getOrElse(
        Try(new MultiplexedConnection(nodeFactory(node), scheduler, config, MultiplexedConnection.NodeRole.Replica, Some(node)).start())
          .flatMap(nc =>
            try askRole(nc)
            finally nc.close()
          )
      )
      reply.failed.foreach(reportProbeFailure(node, _))
      reply
    }
  }

  private def pooledFor(node: Node): Option[MultiplexedConnection] =
    Option(masterPool.existing(node)).orElse(Option(replicaPool.existing(node))).filter(_.isLive)

  private def askRole(nc: MultiplexedConnection): Try[Role] =
    Bootstrap.awaitReply[Role](config.connectTimeout.toMillis, TimedOut(s"ROLE timed out after ${config.connectTimeout.toMillis}ms"))(
      nc.submit(Server.role, _)
    )

  private def lostConnection(reply: Try[Role]): Boolean =
    reply.failed.toOption.map(Fault.categorize).exists {
      case Fault.Lost(_) => true
      case _             => false
    }

  private def reportProbeFailure(node: Node, error: Throwable): Unit =
    events.emit(SageEvent.Connection.ConnectFailed(Some(node), error))

  protected def rediscover(): Unit =
    resolveTopology((Option(topologyRef.get()).toVector.flatMap(t => t.master +: t.replicas) ++ seeds).distinct).foreach(installTopology)

  private def installTopology(topology: MasterReplicaLive.ResolvedTopology): Unit = {
    topologyRef.set(topology)
    replicaPool.retain(topology.replicas.toSet.contains)
    masterPool.retain(_ == topology.master)
    reads.retain(_ == topology.master)
    // a demoted master still receives PUBLISH through replication, so only a node outside the topology loses the subscription
    subscribedOn.retain(node => node == topology.master || topology.replicas.contains(node))
  }

  // --- routing -------------------------------------------------------------------------------------------------------------------------

  // Submit to the master and add its node to the result. Start role discovery if the server is no longer the master.
  protected def route[A](command: Command[A], complete: Try[A] => Unit, lease: DedicatedPool.Lease, mode: DispatchMode): Unit =
    if (closed) complete(Failure(NotConnected()))
    else if (mode == DispatchMode.ReplicaRead) {
      val topology = topologyRef.get()
      walkRead(command, reads.candidatesFor(topology.master, topology.replicas), topology.master, complete)
    } else {
      val node = topologyRef.get().master
      masterPool.withClient(node) {
        triggerRefresh()
        complete(Failure(NotConnected()))
      } { nc =>
        submitOn(
          nc,
          node,
          command,
          lease,
          mode,
          asking = false,
          result => {
            result match {
              case Failure(e) if isOwnershipFault(e) => triggerRefresh()
              case _                                 => ()
            }
            Events.completeAt(complete, node)(result)
          }
        )
      }
    }

  protected def replicaCount(master: Node): Int = topologyRef.get().replicas.size

  private def walkRead[A](command: Command[A], candidates: Vector[Node], master: Node, complete: Try[A] => Unit): Unit =
    reads.walk(command, candidates, master, complete)((node, error, rest) => onReadFault(ReadRoute(node, master, rest), error, command, complete))

  private def onReadFault[A](
    route: ReadRoute,
    error: Throwable,
    command: Command[A],
    complete: Try[A] => Unit
  ): Unit =
    handleReadFaults(route, Vector(error))(
      remaining => walkRead(command, remaining, route.master, complete),
      () => Events.completeAt(complete, route.node)(Failure(error))
    )

  final private case class ReadRoute(node: Node, master: Node, remaining: Vector[Node])

  private def handleReadFaults(route: ReadRoute, errors: Vector[Throwable])(
    retry: Vector[Node] => Unit,
    settle: () => Unit
  ): Unit = {
    val ownershipFault = route.node == route.master && errors.exists(isOwnershipFault)
    if (ownershipFault) triggerRefresh()
    if (errors.exists(servesNoRead))
      if (route.remaining.nonEmpty) retry(route.remaining)
      else {
        // an ownership fault already requested the same throttled refresh above
        if (!ownershipFault) triggerRefresh()
        settle()
      }
    else settle()
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

  protected def submitPipeline[R](p: Pipeline[R]): CIO[Vector[Either[SageException, Any]]] =
    CIO.async { complete =>
      val spans              = Events.startSpans(events, p.commands)
      val topology           = topologyRef.get()
      val master             = topology.master
      val batch              = new Client.TrackedBatch(events, p.commands, spans, complete)
      // without a submission, a connection error cannot trigger role discovery. Refresh roles before failing the batch.
      def failUnsent(): Unit = {
        triggerRefresh()
        batch.failUnsent()
      }
      if (pipelineMode(p.commands) == DispatchMode.ReplicaRead) {
        def submitOn(picked: Option[ReadRouting.Picked]): Unit =
          picked match {
            case Some(ReadRouting.Picked(node, nc, rest)) =>
              val route     = ReadRoute(node, master, rest)
              val attempt   = new TxSupport.IndexedCollector[Try[Any]](
                p.commands.length,
                results =>
                  handleReadFaults(route, results.collect { case Failure(error) => error })(
                    remaining => scheduler.offload(reads.pickOne(remaining, master)(submitOn)),
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
        reads.pickOne(reads.candidatesFor(master, topology.replicas), master)(submitOn)
      } else
        masterPool.withClient(master)(failUnsent())(nc => if (!nc.submitAll(p.commands, batch.callbacks(Some(master)))) failUnsent())
    }

  // --- transactions (always on the master) ---------------------------------------------------------------------------------------------

  protected def openTransaction: CIO[LiveTransactionScope] =
    CIO.blocking {
      try {
        val nc = masterPool.getOrEstablish(topologyRef.get().master)
        new Client.TxScope(nc.pool.acquireForTransaction(), nc.pool.releaseTransaction, p => if (p == RefreshPolicy.Forced) triggerRefresh(), events)
      } catch {
        case e: TimedOut      => throw e
        case e: SageException =>
          triggerRefresh()
          throw e
        case NonFatal(_)      =>
          triggerRefresh()
          throw ConnectionLost(mayHaveExecuted = false)
      }
    }

  // --- pub/sub (on the master) ---------------------------------------------------------------------------------------------------------

  protected def pubsub: SubscriptionConnection.PubSub = subscriptions
}

private[client] object MasterReplicaLive {

  final private[client] case class ResolvedTopology(master: Node, replicas: Vector[Node])
}

package sage.client.internal

import java.util.Locale
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import RoutedClient.DispatchMode
import RoutedClient.DispatchMode.*
import kyo.compat.*

import sage.{Bytes, CommandSpan, SageEvent, SageException}
import sage.SageException.{ConnectionLost, CrossSlot, DecodeError, InvalidArgument, NotConnected, ServerError, TimedOut, UnsupportedServer}
import sage.client.{ClusterConfig, ReadFrom, SageConfig}
import sage.cluster.{ClusterTopology, Node, NodeGroup, Redirect, RedirectKind, Route, Shard, Slot, SlotRange, SplitPlan}
import sage.codec.KeyCodec
import sage.commands.{Cluster, Command, Pipeline, Reply}
import sage.protocol.Frame

/**
  * Implements the cluster client with one [[MultiplexedConnection]] per master and a [[ClusterTopology]] that can be refreshed. The topology identifies
  * the node for each command, and this class handles connections, redirects, and failover. Configuration selects this implementation without
  * changing the `Client` type.
  *
  * A pipeline is grouped by node with [[ClusterTopology.split]]. Each group is sent as one batch, and results are restored to submission
  * order. Commands whose node is unknown use normal [[dispatch]]. A transaction uses one dedicated connection. Keyed commands select its
  * slot, and a later command for another slot fails with [[CrossSlot]].
  *
  * Dispatch to an existing connection runs on the caller's thread because lookup and submission do not block. When a connection is being
  * opened or the topology is refreshed, work runs on a separate virtual thread. Reply callbacks also move any blocking continuation off the
  * reply thread.
  *
  * Redirects, ownership or connection failures, unowned slots, and lost subscriptions can refresh the topology. `minRefreshInterval` limits
  * these refreshes. Set `topologyRefreshInterval` to add periodic background refreshes.
  */
final private[client] class ClusterLive(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  config: SageConfig,
  cluster: ClusterConfig,
  seeds: Vector[Node],
  events: Events = Events.disabled
) extends RoutedClient(
    nodeFactory,
    scheduler,
    // replica connections remain separate from the master registry used for command routing and redirects
    MultiplexedConnection.NodeRole.ClusterReplica,
    config,
    cluster.minRefreshInterval,
    cluster.topologyRefreshInterval,
    events
  ) {

  private val topologyRef = new AtomicReference[ClusterTopology](ClusterTopology.from(Vector.empty))

  private val keylessCursor = new java.util.concurrent.atomic.AtomicInteger()

  private val subscriptions = new ClusterSubscriptions(
    nodeFactory,
    scheduler,
    config,
    () => topologyRef.get(),
    () => refreshThrottle(force = true),
    () => pickNode(topologyRef.get()),
    events
  )

  // if every seed fails, report the final connection or handshake error to the caller.
  protected def discover(): Either[Throwable, Unit] =
    seeds.foldLeft[Either[Throwable, Unit]](Left(NotConnected()))((found, node) =>
      if (found.isRight) found else Try(querySlotsVia(node).map(adopt)).toEither.flatten
    )

  protected def route[A](command: Command[A], complete: Try[A] => Unit, lease: DedicatedPool.Lease, mode: DispatchMode): Unit =
    dispatch(Request(command, complete, lease, mode), cluster.maxRedirects)

  protected def replicaCount(master: Node): Int = topologyRef.get().replicasForMaster(master).size

  // SCAN cursors are node-local. A full scan visits every master that owns slots. Resharding during the scan can still miss or duplicate keys.
  override def scanTargets: CIO[Vector[ScanTarget]] =
    CIO.blocking {
      val masters = topologyRef.get().masters
      if (masters.isEmpty) Vector(this) else masters.map(pinnedTo)
    }

  // Resume a SCAN page on the node that issued its cursor. If that node is unavailable, fail the scan because another master would interpret
  // the node-local cursor against a different keyspace. redirectsLeft = 0 disables rerouting.
  private def pinnedTo(node: Node): ScanTarget =
    new ScanTarget {
      def run[A](command: Command[A]): CIO[A] =
        Client.withLeaseIfBlocking(command) { lease =>
          tracked(command)(t => sendTo(node, Request(command, t, lease, readMode(command)), asking = false, redirectsLeft = 0))
        }
    }

  // Classic subscriptions share a connection to an arbitrary master because PUBLISH broadcasts across the cluster. Each shard channel uses a
  // sharded subscription connection for its slot's node, and the subscription follows ownership changes.
  protected def pubsub: SubscriptionConnection.PubSub = subscriptions

  // --- routing -------------------------------------------------------------------------------------------------------------------------

  final private case class Request[A](command: Command[A], complete: Try[A] => Unit, lease: DedicatedPool.Lease, mode: DispatchMode)

  private def dispatch[A](req: Request[A], redirectsLeft: Int): Unit =
    if (closed) req.complete(Failure(NotConnected()))
    else {
      val topology = topologyRef.get()
      if (req.command.allMasters) broadcast(topology, req, redirectsLeft)
      else
        topology.route(req.command) match {
          case Route.ToNode(shard, _) => sendOwned(req, shard, redirectsLeft)
          case Route.Keyless          =>
            if (req.mode == ReplicaRead) sendKeylessRead(topology, req, redirectsLeft)
            else sendToAny(topology, req, redirectsLeft)
          case Route.Unowned(_)       => scheduler.offload(onUnowned(req, redirectsLeft))
          case Route.CrossSlot        =>
            multiSlotPolicy(req.command) match {
              case Some(policy) => scatterMultiSlot(req, policy, redirectsLeft)
              case None         => req.complete(Failure(crossSlot(req.command)))
            }
          case Route.Malformed        =>
            req.complete(Failure(malformedKeys(req.command.name)))
        }
    }

  private def sendOwned[A](req: Request[A], shard: Shard, redirectsLeft: Int): Unit =
    if (req.mode == ReplicaRead) walkRead(req, reads.candidatesFor(shard.master, shard.replicas), shard.master, redirectsLeft)
    else sendTo(shard.master, req, asking = false, redirectsLeft)

  private enum MultiSlotMerge {
    case Positional, Sum, AllSucceeded
  }

  // suffixArgs are shared trailing args (e.g. JSON.MGET's path) re-appended to every per-slot subgroup after its keys
  final private case class MultiSlotPolicy(merge: MultiSlotMerge, argsPerKey: Int, suffixArgs: Int = 0)

  final private case class MultiSlotEntry(resultIndex: Int, argIndex: Int)

  // Split a supported cross-slot command into one command for each slot. Send every group through normal dispatch to apply replica policy,
  // topology refresh, and MOVED or ASK handling. After every group completes, decode the combined result with the original command. MGET
  // restores values to their original positions, integer commands add the per-slot counts, and MSET requires an OK reply from every slot.
  private def scatterMultiSlot[A](req: Request[A], policy: MultiSlotPolicy, redirectsLeft: Int): Unit = {
    val command = req.command
    val bySlot  = mutable.LinkedHashMap.empty[Slot, mutable.ArrayBuffer[MultiSlotEntry]]
    command.keyIndices.iterator.zipWithIndex.foreach { case (argIndex, resultIndex) =>
      val key   = command.args(argIndex)
      val entry = MultiSlotEntry(resultIndex, argIndex)
      val group = bySlot.getOrElseUpdate(Slot.of(key), mutable.ArrayBuffer.empty)
      group += entry
    }
    val groups  = bySlot.valuesIterator.map(_.toVector).toVector

    val collector = gather(command, groups.size, req.complete) { frames =>
      policy.merge match {
        case MultiSlotMerge.Positional   =>
          val values = new Array[Frame](command.keyIndices.size)
          groups.lazyZip(frames).foreach { (group, frame) =>
            frame match {
              case Frame.Array(elements) if elements.size == group.size =>
                group.lazyZip(elements).foreach((entry, value) => values(entry.resultIndex) = value)
              case Frame.Array(elements)                                =>
                throw DecodeError(s"an array of ${group.size} MGET values", s"an array of ${elements.size} values")
              case other                                                => throw DecodeError(s"an array of ${group.size} MGET values", Frame.describe(other))
            }
          }
          Frame.Array(values.toVector)
        case MultiSlotMerge.Sum          =>
          Frame.Integer(frames.foldLeft(0L) {
            case (total, Frame.Integer(value)) => total + value
            case (_, other)                    => throw DecodeError("an integer count", Frame.describe(other))
          })
        case MultiSlotMerge.AllSucceeded =>
          frames.foreach {
            case Frame.SimpleString("OK") => ()
            case other                    => throw DecodeError("simple string 'OK'", Frame.describe(other))
          }
          Frame.SimpleString("OK")
      }
    }

    val raw = command.rawFrame
    groups.iterator.zipWithIndex.foreach { case (group, index) =>
      val args = Vector.newBuilder[Bytes]
      args.sizeHint(group.size * policy.argsPerKey)
      group.foreach { entry =>
        var offset = 0
        while (offset < policy.argsPerKey) {
          args += command.args(entry.argIndex + offset)
          offset += 1
        }
      }
      if (policy.suffixArgs > 0) command.args.takeRight(policy.suffixArgs).foreach(args += _)
      val sub  = raw.copy(
        keyIndices = Vector.tabulate(group.size)(_ * policy.argsPerKey),
        args = args.result()
      )
      dispatch(Request(sub, collector.set(index, _), req.lease, req.mode), redirectsLeft)
    }
  }

  // Validate each recognized command's complete argument shape before splitting. A custom command that merely shares its name must not
  // lose or misassociate non-key arguments during subgroup construction.
  private def multiSlotPolicy(command: Command[?]): Option[MultiSlotPolicy] =
    command.name.toUpperCase(Locale.ROOT) match {
      case "MGET" if hasKeyStride(command, 1)                                => Some(MultiSlotPolicy(MultiSlotMerge.Positional, 1))
      case "DEL" | "EXISTS" | "TOUCH" | "UNLINK" if hasKeyStride(command, 1) => Some(MultiSlotPolicy(MultiSlotMerge.Sum, 1))
      case "MSET" if hasKeyStride(command, 2)                                => Some(MultiSlotPolicy(MultiSlotMerge.AllSucceeded, 2))
      // Keep JSON.MSET as one command because any triplet can fail path validation. Splitting it could apply earlier groups before a later
      // group fails validation.
      case "JSON.MGET" if hasLeadingKeys(command, 1)                         => Some(MultiSlotPolicy(MultiSlotMerge.Positional, 1, suffixArgs = 1))
      case _                                                                 => None
    }

  private def hasKeyStride(command: Command[?], argsPerKey: Int): Boolean =
    command.args.nonEmpty && command.args.size % argsPerKey == 0 &&
      command.keyIndices == Vector.tabulate(command.args.size / argsPerKey)(_ * argsPerKey)

  private def hasLeadingKeys(command: Command[?], suffixArgs: Int): Boolean =
    command.args.size > suffixArgs &&
      command.keyIndices == Vector.tabulate(command.args.size - suffixArgs)(identity)

  // RANDOMKEY has no slot. Try replicas across the cluster in round-robin order, then apply the configured fallback policy.
  // ReadFrom.Replica uses only replica candidates.
  private def sendKeylessRead[A](topology: ClusterTopology, req: Request[A], redirectsLeft: Int): Unit =
    pickNode(topology) match {
      case Some(master) =>
        val replicas = topology.shards.iterator.flatMap(_.replicas).toVector.distinct
        walkRead(req, reads.candidatesFor(master, replicas, keylessCursor.getAndIncrement()), master, redirectsLeft)
      case None         => req.complete(Failure(NotConnected()))
    }

  private def walkRead[A](req: Request[A], candidates: Vector[Node], master: Node, redirectsLeft: Int): Unit =
    reads.walk(req.command, candidates, master, req.complete)((node, error, rest) => onReadFailure(node, req, error, rest, master, redirectsLeft))

  // Lost or Unavailable walks the remaining candidates. MOVED refreshes the topology and re-dispatches so the read keeps its replica policy.
  private def onReadFailure[A](node: Node, req: Request[A], error: Throwable, rest: Vector[Node], master: Node, redirectsLeft: Int): Unit =
    Fault.categorize(error) match {
      case Fault.Lost(_) | Fault.Unavailable(_) if rest.nonEmpty                                  => walkRead(req, rest, master, redirectsLeft)
      case Fault.Redirected(redirect) if redirect.kind == RedirectKind.Moved && redirectsLeft > 0 =>
        refreshThrottle(force = true)
        scheduler.offload(dispatch(req, redirectsLeft - 1))
      case _                                                                                      => onFailure(node, req, error, redirectsLeft)
    }

  // A broadcast command (SCRIPT LOAD, FUNCTION LOAD, …) runs on every slot-owning master, since a cluster replicates no script/function
  // cache; any node failing terminally fails the command. Replies are combined before decoding: KEYS concatenates the keys returned by each
  // node, and WAIT and WAITAOF use the lowest acknowledgement counts returned by any shard.
  private def broadcast[A](topology: ClusterTopology, req: Request[A], redirectsLeft: Int): Unit = {
    val masters = topology.masters
    val command = req.command
    if (masters.isEmpty) sendToAny(topology, req, cluster.maxRedirects)
    else {
      val raw       = command.rawFrame
      val collector = gather(command, masters.size, req.complete)(frames => command.reduceReplies(frames(0), frames.drop(1)))
      masters.iterator.zipWithIndex.foreach { case (node, index) =>
        submitBroadcast(node, raw, redirectsLeft, collector.set(index, _))
      }
    }
  }

  // after every part replies, completes with the first failure by part index or with the decoded combination of all frames
  private def gather[A](command: Command[A], parts: Int, complete: Try[A] => Unit)(combine: Vector[Frame] => Frame) =
    new TxSupport.IndexedCollector[Try[Frame]](
      parts,
      results =>
        complete(
          results
            .collectFirst { case Failure(e) => Failure(e) }
            .getOrElse(Try(combine(results.collect { case Success(f) => f })))
            .flatMap(Reply.decode(command, _))
        )
    )

  // withClient runs the unreachable branch only after offloading the connect, so the retry's blocking refresh never runs on the caller's thread
  private def submitBroadcast[B](node: Node, command: Command[B], attemptsLeft: Int, settle: Try[B] => Unit): Unit =
    masterPool.withClient(node)(retryBroadcast(node, command, NotConnected(), attemptsLeft, settle)) {
      _.submit[B](
        command,
        {
          case Success(value) => settle(Success(value))
          case Failure(error) => scheduler.offload(onBroadcastFailure(node, command, error, attemptsLeft, settle))
        }
      )
    }

  // retry only the node whose connection was lost or whose request was temporarily refused; retrying WAIT starts its timeout again there
  private def onBroadcastFailure[B](node: Node, command: Command[B], error: Throwable, attemptsLeft: Int, settle: Try[B] => Unit): Unit =
    Fault.categorize(error) match {
      case Fault.Lost(false) | Fault.TryAgain | Fault.Unavailable(false) => retryBroadcast(node, command, error, attemptsLeft, settle)
      // a cluster-wide refusal may mean the selected masters are stale; refresh the topology, then return the error
      case Fault.Unavailable(true)                                       =>
        refreshBeforeFailing()
        settle(Failure(error))
      case fault                                                         =>
        if (fault.refreshPolicy != RefreshPolicy.Skip) triggerRefresh()
        settle(Failure(error))
    }

  // retry this node only while it remains a slot-owning master
  private def retryBroadcast[B](node: Node, command: Command[B], error: Throwable, attemptsLeft: Int, settle: Try[B] => Unit): Unit = {
    val refreshFirst = refreshesFirst(error)
    if (attemptsLeft <= 0) {
      if (refreshFirst) refreshBeforeFailing()
      settle(Failure(error))
    } else
      afterBackoff(attemptsLeft) {
        if (refreshFirst) refreshThrottle(force = true)
        if (closed || !topologyRef.get().masters.contains(node)) settle(Failure(error))
        else submitBroadcast(node, command, attemptsLeft - 1, settle)
      }
  }

  private def sendToAny[A](topology: ClusterTopology, req: Request[A], redirectsLeft: Int): Unit =
    pickNode(topology) match {
      case Some(node) => sendTo(node, req, asking = false, redirectsLeft)
      case None       => req.complete(Failure(NotConnected()))
    }

  private def sendTo[A](node: Node, req: Request[A], asking: Boolean, redirectsLeft: Int): Unit =
    masterPool.withClient(node)(onUnreachable(req, redirectsLeft))(submitTo(_, node, req, asking, redirectsLeft))

  private def submitTo[A](nc: MultiplexedConnection, node: Node, req: Request[A], asking: Boolean, redirectsLeft: Int): Unit = {
    val onReply: Try[A] => Unit = {
      case success @ Success(_) => Events.completeAt(req.complete, node)(success)
      case Failure(error)       => scheduler.offload(onFailure(node, req, error, redirectsLeft))
    }
    submitOn(nc, node, req.command, req.lease, req.mode, asking, onReply)
  }

  private def onFailure[A](node: Node, req: Request[A], error: Throwable, redirectsLeft: Int): Unit =
    Fault.categorize(error) match {
      case Fault.Lost(false) => onUnreachable(req, redirectsLeft)
      case fault             =>
        // the node received the command, so an exhausted retry reports it unless a later attempt reaches another node
        Events.attributeNode(req.complete, node)
        fault match {
          case Fault.Redirected(redirect)            => onRedirect(node, redirect, req, error, redirectsLeft)
          case Fault.TryAgain | Fault.Unavailable(_) => onRetryable(req, error, redirectsLeft)
          case Fault.Demoted | Fault.Lost(true)      =>
            triggerRefresh()
            req.complete(Failure(error))
          case _                                     => req.complete(Failure(error))
        }
    }

  private def onRedirect[A](from: Node, redirect: Redirect, req: Request[A], error: Throwable, redirectsLeft: Int): Unit = {
    // a MOVED proves `from` lost the slot; retire its cache even if the retry budget is now exhausted
    if (redirect.kind == RedirectKind.Moved) flushNode(from)
    // ReadFrom.Replica cannot follow ASK because the importing master holds the key during migration
    if (redirect.kind == RedirectKind.Ask && req.mode == ReplicaRead && config.readFrom == ReadFrom.Replica) {
      req.complete(Failure(NotConnected()))
    } else if (redirectsLeft <= 0) {
      if (redirect.kind == RedirectKind.Moved) refreshBeforeFailing()
      // Lock writes retry redirect faults within their lock budget after the topology refresh. Ordinary commands report the redirect limit.
      val failure = req.mode match {
        case Confirmed(_, _) => error
        case _               => ServerError("ERR", s"exceeded ${cluster.maxRedirects} cluster redirects for ${req.command.name}")
      }
      req.complete(Failure(failure))
    } else {
      val target = redirect.target(from)
      redirect.kind match {
        case RedirectKind.Moved =>
          triggerRefresh()
          sendTo(target, req, asking = false, redirectsLeft - 1)
        case RedirectKind.Ask   => sendTo(target, req, asking = true, redirectsLeft - 1)
      }
    }
  }

  // The command was not sent; refresh the topology before routing it again, delay retries with jitter while failover completes, and use
  // redirectsLeft to limit the number of attempts
  private def onUnreachable[A](req: Request[A], redirectsLeft: Int): Unit = onRetryable(req, NotConnected(), redirectsLeft)

  // retry temporary refusals such as TRYAGAIN, LOADING, MASTERDOWN, and CLUSTERDOWN with bounded jitter
  private def onRetryable[A](req: Request[A], error: Throwable, redirectsLeft: Int): Unit = {
    val refreshFirst = refreshesFirst(error)
    if (redirectsLeft <= 0) {
      if (refreshFirst) refreshBeforeFailing()
      req.complete(Failure(error))
    } else {
      if (refreshFirst) refreshThrottle(force = true)
      afterBackoff(redirectsLeft)(dispatch(req, redirectsLeft - 1))
    }
  }

  private def refreshesFirst(error: Throwable): Boolean = Fault.categorize(error).refreshPolicy == RefreshPolicy.Forced

  // increase the jittered delay with each attempt to reduce request load during failover or migration
  private def afterBackoff(attemptsLeft: Int)(retry: => Unit): Unit =
    scheduler.afterBackoff(config.reconnect, (cluster.maxRedirects - attemptsLeft).max(0))(retry)

  // refresh immediately before returning an error on paths where no later retry can trigger another refresh
  private def refreshBeforeFailing(): Unit = refreshThrottle(force = true)

  private def onUnowned[A](req: Request[A], redirectsLeft: Int): Unit = {
    refreshThrottle(force = false)
    val topology = topologyRef.get()
    topology.route(req.command) match {
      // apply the read policy after the slot resolves. Eligible reads still use replica routing.
      case Route.ToNode(shard, _)                                              => sendOwned(req, shard, redirectsLeft)
      // ReadFrom.Replica has no master fallback. Refresh and retry within the configured limit.
      case _ if req.mode == ReplicaRead && config.readFrom == ReadFrom.Replica => onUnreachable(req, redirectsLeft)
      // if the refreshed topology still has no owner, send to any master and handle its MOVED or CLUSTERDOWN reply
      case _                                                                   => sendToAny(topology, req, redirectsLeft)
    }
  }

  private def pickNode(topology: ClusterTopology): Option[Node] =
    masterPool.firstLiveNode.orElse(topology.shards.headOption.map(_.master))

  private def crossSlot(command: Command[?]): CrossSlot = {
    val slots = command.keyIndices.iterator.flatMap(command.args.lift).map(Slot.of).distinct.size
    CrossSlot(s"${command.name}: keys span $slots slots; a single command must touch exactly one")
  }

  private def malformedKeys(name: String): InvalidArgument =
    InvalidArgument(s"$name: declared key positions fall outside its arguments")

  // --- pipelines (split per node, batch each, merge in submission order) ----------------------------------------------------------------

  protected def submitPipeline[R](p: Pipeline[R]): CIO[Vector[Either[SageException, Any]]] =
    // A pipeline batches commands per node, but all-masters commands must run on every master. Reject them before submission because running
    // one on a single node could break a later key-routed EVALSHA or FCALL, or return only part of the keyspace.
    if (p.commands.exists(_.allMasters))
      CIO.fail(
        InvalidArgument("a Pipeline cannot carry an all-masters command (e.g. SCRIPT LOAD, FUNCTION LOAD, KEYS); run it individually on the client")
      )
    else
      CIO.async { complete =>
        runPipeline(p, complete, Events.deferSpans(events, p.commands))
      }

  // use per-command dispatch for positions that the current topology cannot resolve. Complete after every position has succeeded or failed.
  private def runPipeline[R](
    p: Pipeline[R],
    complete: Try[Vector[Either[SageException, Any]]] => Unit,
    deferred: Vector[() => CommandSpan]
  ): Unit = {
    val plan = topologyRef.get().split(p.commands)
    // reject a malformed command before starting spans or submitting any part of the pipeline
    plan.routes.indexOf(Route.Malformed) match {
      case -1    => new PipelineRun(p.commands, plan, deferred, complete).start()
      case index => complete(Failure(malformedKeys(p.commands(index).name)))
    }
  }

  final private class PipelineRun(
    commands: Vector[Command[?]],
    plan: SplitPlan,
    deferred: Vector[() => CommandSpan],
    complete: Try[Vector[Either[SageException, Any]]] => Unit
  ) {
    private val collector =
      new TxSupport.IndexedCollector[Either[SageException, Any]](commands.length, results => complete(Success(results)))
    // reroutes and retries keep the original choice, preventing a slot from being split across a master and replica.
    private val routing   = pipelineMode(commands)
    private val positions = Vector.tabulate(commands.length)(new Position(_))

    final private class Position(index: Int) {
      val command                = commands(index)
      // settling a command releases its latch; a retry waits for the previous command on the same slot to preserve write order
      private val settled        = new CountDownLatch(1)
      // the slot a retry orders on; -1 for a keyless or cross-slot position, which orders against nothing
      val slot: Int              = plan.routes(index) match {
        case Route.ToNode(_, slot) => slot.value
        case Route.Unowned(slot)   => slot.value
        case _                     => -1
      }
      val emit: Try[Any] => Unit = Events.trackCommand[Any](
        events,
        command,
        result => {
          settled.countDown()
          collector.set(index, TxSupport.toEither(result))
        },
        if (deferred.isEmpty) CommandSpan.noop else Events.startDeferred(deferred(index))
      )

      private def awaitTurn(): Unit =
        if (slot >= 0) {
          val previous = positions.lastIndexWhere(_.slot == slot, index - 1)
          if (previous >= 0) positions(previous).settled.await()
        }

      // run rerouting on the scheduler because awaitTurn may block
      def reroute(): Unit = scheduler.offload {
        awaitTurn()
        dispatch(Request(command, emit, null, routing), cluster.maxRedirects)
      }

      def onBatchReply(target: Node): Try[Any] => Unit = {
        case success @ Success(_) => Events.completeAt(emit, target)(success)
        // a fault's disposition can block on CLUSTER SLOTS, whose reply needs this very reader thread
        case Failure(error)       =>
          scheduler.offload {
            awaitTurn()
            Fault.categorize(error) match {
              // MOVED and connection loss use normal routing. ASK keeps the exporting node as the slot owner in the topology, so onFailure
              // sends the command directly to the importing node with ASKING instead of routing it back to the exporter.
              case Fault.Redirected(redirect) if redirect.kind == RedirectKind.Moved => reroute()
              case Fault.Lost(false)                                                 => reroute()
              case _                                                                 =>
                onFailure(target, Request(command, emit, null, routing), error, cluster.maxRedirects)
            }
          }
      }
    }

    def start(): Unit = {
      plan.routes.iterator.zip(positions).foreach {
        case (Route.CrossSlot, position)                       =>
          if (multiSlotPolicy(position.command).nonEmpty) position.reroute()
          else position.emit(Failure(crossSlot(position.command)))
        case (Route.Unowned(_), position)                      => position.reroute() // dispatch refreshes then re-routes
        case (Route.Keyless, position) if plan.perNode.isEmpty => position.reroute()
        case _                                                 => ()
      }
      plan.perNode.foreach { case NodeGroup(shard, indices) => send(shard, indices) }
    }

    // attribute the batch to the node that handles it, which is a replica when the routing allows one
    private def send(shard: Shard, indices: Vector[Int]): Unit =
      if (routing == ReplicaRead)
        reads.pickOne(reads.candidatesFor(shard.master, shard.replicas), shard.master) {
          case Some(picked) => submit(picked.node, picked.client, indices)
          case None         => indices.foreach(positions(_).reroute())
        }
      else masterPool.withClient(shard.master)(indices.foreach(positions(_).reroute()))(submit(shard.master, _, indices))

    // if the node is unavailable before the batch is submitted, route each command in the batch again
    private def submit(target: Node, nc: MultiplexedConnection, indices: Vector[Int]): Unit =
      if (!nc.submitAll(indices.map(positions(_).command), indices.map(positions(_).onBatchReply(target))))
        indices.foreach(positions(_).reroute())
  }

  // --- transactions (one leased connection, optionally pinned to a key's slot) ---------------------------------------------------------

  protected def openTransaction: CIO[LiveTransactionScope] =
    if (closed) CIO.fail(NotConnected()) else CIO.value(new ClusterTxScope)

  // A transaction cannot follow a redirect without breaking MULTI/EXEC atomicity. After an ownership or connection failure, refresh the
  // topology in the background. A later transaction attempt then selects a connection using the updated topology. Data errors do not refresh.
  // A forced refresh skips the throttle so the caller's retry uses the latest ownership information.
  private def refreshFor(policy: RefreshPolicy): Unit =
    policy match {
      case RefreshPolicy.Forced    => scheduler.offload(refreshThrottle(force = true))
      case RefreshPolicy.Throttled => triggerRefresh()
      case RefreshPolicy.Skip      => ()
    }

  /**
    * A cluster transaction scope. It leases a dedicated connection when the first command is submitted. A keyed command pins the transaction
    * to that key's slot. A keyless first command uses an arbitrary master; the first later key is accepted only if that master owns its slot.
    * Later keys must use the same slot or fail with [[CrossSlot]]. The transaction does not follow redirects or reconnect after a connection
    * loss. These failures trigger a background topology refresh, and the caller can retry the full transaction.
    */
  final private class ClusterTxScope extends LiveTransactionScope(events, refreshFor) {

    // guarded by `lock`
    private var leased: Leased = null
    private val acquiring      = new ReentrantLock()

    protected type Target = Either[Throwable, Option[Slot]]

    override def run[A](command: Command[A]): CIO[A] =
      if (command.requiresClusterWideTxResult)
        CIO.fail(
          InvalidArgument(
            s"${command.name} returns a cluster-wide result that a single-node Transaction cannot produce; run it individually on the client"
          )
        )
      else super.run(command)

    override protected def sendMultiExec[R](p: Pipeline[R]): CIO[TxSupport.ExecReplies] =
      if (p.commands.exists(_.requiresClusterWideTxResult))
        CIO.fail(
          InvalidArgument(
            "a Transaction cannot carry a command that returns a cluster-wide result; run it individually on the client"
          )
        )
      else super.sendMultiExec(p)

    // validate every pipeline slot before sending MULTI. Reject a cross-slot transaction before submitting any commands.
    protected def withConn[A](target: Target, complete: Try[A] => Unit)(use: DedicatedConnection => Unit): Unit =
      scheduler.offload(onConn(target, complete)(use))

    // Check the released state and submit while holding `lock` so release cannot race with a submission. Acquire outside the lock so release()
    // can finish while a connection is being opened.
    private def onConn[A](target: Target, complete: Try[A] => Unit)(use: DedicatedConnection => Unit): Unit = {
      val leasing          = target.flatMap(ensureLeased)
      var fault: Throwable = null
      lock.lock()
      try
        if (released) complete(Failure(TxSupport.scopeReleasedError))
        else
          leasing.flatMap(checkPin) match {
            case Left(error) =>
              fault = error
              complete(Failure(error))
            case Right(())   => Client.completing(complete)(use(leased.conn))
          }
      finally lock.unlock()
      if (fault != null) onFault(fault)
    }

    // `acquiring` serializes the first lease so concurrent first commands share one connection; release() never takes it
    private def ensureLeased(slot: Option[Slot]): Either[Throwable, Option[Slot]] = {
      acquiring.lock()
      try
        if (underLock(released || leased != null)) Right(slot)
        else
          acquireConn(slot).map { acquired =>
            if (!underLock { if (!released) leased = acquired; !released })
              acquired.nc.pool.releaseTransaction(acquired.conn, reusable = true)
            slot
          }
      finally acquiring.unlock()
    }

    private inline def underLock[A](inline body: A): A = {
      lock.lock()
      try body
      finally lock.unlock()
    }

    // must hold `lock` with leased != null. If a keyless command acquired the connection, accept the first keyed slot only when its node owns it.
    private def checkPin(slot: Option[Slot]): Either[Throwable, Unit] =
      slot match {
        case None    => Right(())
        case Some(s) =>
          leased.slot match {
            case Some(ps) if ps == s => Right(())
            case Some(ps)            =>
              Left(CrossSlot(s"transaction touches slot ${s.value} but is pinned to slot ${ps.value}; MULTI/EXEC requires a single slot"))
            case None                =>
              if (topologyRef.get().nodeForSlot(s).contains(leased.node)) {
                leased = leased.copy(slot = Some(s))
                Right(())
              } else Left(CrossSlot(s"transaction touches slot ${s.value} on a node other than its pinned one; MULTI/EXEC requires a single slot"))
          }
      }

    // runs outside `lock`: may force a topology refresh, connect, or wait for a pool slot
    private def acquireConn(slot: Option[Slot]): Either[Throwable, Leased] =
      slot.fold(pickNode(topologyRef.get()))(nodeForSlotRefreshing) match {
        case None       => Left(NotConnected())
        case Some(node) =>
          try {
            val nc = masterPool.getOrEstablish(node)
            Right(Leased(nc, nc.pool.acquireForTransaction(), node, slot))
          } catch {
            case error: SageException => Left(error)
            case NonFatal(_)          => Left(ConnectionLost(mayHaveExecuted = false))
          }
      }

    private def nodeForSlotRefreshing(slot: Slot): Option[Node] =
      topologyRef.get().nodeForSlot(slot).orElse {
        refreshThrottle(force = true)
        topologyRef.get().nodeForSlot(slot)
      }

    // select the transaction connection by key slot. Keep the slot even when the topology does not currently identify its owner.
    protected def targetOf(command: Command[?]): Either[Throwable, Option[Slot]] =
      topologyRef.get().route(command) match {
        case Route.Malformed       => Left(malformedKeys(command.name))
        case Route.Keyless         => Right(None)
        case Route.ToNode(_, slot) => Right(Some(slot))
        case Route.Unowned(slot)   => Right(Some(slot))
        case Route.CrossSlot       => Left(crossSlot(command))
      }

    protected def targetOf(commands: Vector[Command[?]]): Either[Throwable, Option[Slot]] =
      commands.foldLeft[Either[Throwable, Option[Slot]]](Right(None)) { (acc, command) =>
        acc.flatMap(pinned =>
          targetOf(command).flatMap {
            case Some(slot) if pinned.exists(_ != slot) =>
              Left(CrossSlot("transaction keys span multiple slots; MULTI/EXEC requires a single slot"))
            case slot                                   => Right(pinned.orElse(slot))
          }
        )
      }

    protected def leasedConn: DedicatedConnection = if (leased == null) null else leased.conn

    // runs after release() sealed the scope, so `leased` no longer changes
    protected def giveBack(conn: DedicatedConnection, reusable: Boolean): Unit = leased.nc.pool.releaseTransaction(conn, reusable)
  }

  // a transaction's leased connection, its node, and the slot it is pinned to (None until a keyed command arrives)
  final private case class Leased(nc: MultiplexedConnection, conn: DedicatedConnection, node: Node, slot: Option[Slot])

  private def flushNode(node: Node): Unit = {
    val nc = masterPool.existing(node)
    if (nc != null) nc.flushCache()
  }

  // --- topology refresh (single-flight, throttled) -------------------------------------------------------------------------------------

  protected def rediscover(): Unit =
    querySlots(refreshCandidates()) match {
      case Some(ranges) => adopt(ranges)
      // if no candidate answers CLUSTER SLOTS, slot ownership is unknown. Clear every client-side cache.
      case None         => masterPool.foreachEstablished(_.flushCache())
    }

  private def refreshCandidates(): Vector[Node] = (masterPool.candidatesByLiveness ++ seeds).distinct

  private def querySlots(candidates: Vector[Node]): Option[Vector[SlotRange]] =
    candidates.iterator.flatMap(trySlots).nextOption()

  private def trySlots(node: Node): Option[Vector[SlotRange]] =
    try querySlotsVia(node).toOption
    catch { case NonFatal(_) => None }

  // treat an empty CLUSTER SLOTS reply as unavailable topology information. A node can return it before joining a formed cluster.
  private def querySlotsVia(node: Node): Either[Throwable, Vector[SlotRange]] = {
    val nc       = masterPool.getOrEstablish(node)
    def timedOut = TimedOut(s"CLUSTER SLOTS on ${node.host}:${node.port} timed out after ${config.connectTimeout.toMillis}ms")
    Bootstrap.awaitReply[Vector[SlotRange]](config.connectTimeout.toMillis, timedOut)(nc.submit(Cluster.slots(node), _)) match {
      case Success(ranges) if ranges.nonEmpty => Right(ranges)
      case Success(_)                         => Left(UnsupportedServer(s"${node.host}:${node.port} owns no slots: it is not part of a formed cluster"))
      case Failure(error: ServerError)        => Left(UnsupportedServer(s"${node.host}:${node.port} rejected CLUSTER SLOTS: ${error.getMessage}"))
      case Failure(error)                     => Left(error)
    }
  }

  // Prune bundles for masters that are no longer listed. This stops reconnect loops for nodes that have left.
  private def adopt(ranges: Vector[SlotRange]): Unit = {
    val oldTopology  = topologyRef.get()
    val previous     = if (events.emitsEvents) oldTopology.masters.toSet else Set.empty[Node]
    val newTopology  = ClusterTopology.from(ranges)
    // retire losing masters' caches before the new topology is published
    newTopology.mastersLosingSlots(oldTopology).foreach(flushNode)
    topologyRef.set(newTopology)
    // skip the empty -> populated bootstrap transition: discovering the topology at connect is not a change
    if (events.emitsEvents && previous.nonEmpty) {
      val current = newTopology.masters
      if (current.toSet != previous) events.emit(SageEvent.TopologyChanged(current))
    }
    val masters      = ranges.map(_.master).toSet
    masterPool.retain(masters.contains)
    // prune replica connections and their cursors for replicas the new topology no longer lists, mirroring the master prune
    val replicaNodes = ranges.iterator.flatMap(_.replicas).toSet
    replicaPool.retain(replicaNodes.contains)
    // replicas also receive PUBLISH, so a master demoted in place keeps the classic subscriptions
    subscriptions.retain(node => masters(node) || replicaNodes(node))
    reads.retain(masters.contains)
    // Reassign shard subscriptions only when slot ownership changes. Doing this for every forced refresh during failover would create a
    // refresh and reconciliation loop.
    if (!newTopology.sameOwnership(oldTopology)) subscriptions.onTopologyChanged()
  }
}

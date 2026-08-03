package sage.client.internal

import java.util.Locale
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Bytes, CommandSpan, Message, PatternMessage, SageEvent, SageException}
import sage.SageException.{ConnectionLost, CrossSlot, DecodeError, InvalidArgument, NotConnected, ServerError, TimedOut, UnsupportedServer}
import sage.client.{BackoffConfig, ClusterConfig, DedicatedPoolConfig, ReadFrom, SageConfig, WatchdogConfig}
import sage.cluster.{ClusterTopology, Node, NodeGroup, Redirect, RedirectKind, Rejected, Route, Shard, Slot, SplitPlan}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{BroadcastReduce, Cluster, Command, Connection, Pipeline, Reply}
import sage.protocol.Frame
import sage.ratelimit.Decision

/**
  * The cluster runtime: one [[NodeClient]] bundle per master, a refreshable [[ClusterTopology]], and the routing/redirect/failover state
  * machine that executes the pure engine's [[Route]] classifications. The same `Client` type as standalone; only configuration selects it.
  *
  * A Pipeline is split per node (the pure [[ClusterTopology.split]]), each group sent as one batch, and the results scattered back into
  * submission order; positions a stale topology can't resolve fall back to per-command [[dispatch]]. A Transaction leases one Dedicated
  * Connection, pinned lazily to the slot of its first key, and rejects any key on another slot with [[CrossSlot]].
  *
  * Dispatch to an already-established node runs inline on the caller: the registry lookup and the submit never block, even mid-reconnect,
  * where the submit fails fast. Only the paths that may establish a connection or refresh the topology hop to an offloaded virtual thread,
  * and a submit's reply callback re-offloads before any blocking continuation (never on the reply thread).
  *
  * The topology refreshes on events (a redirect, an ownership or connection fault, an unowned slot, a subscription drop), throttled by
  * `minRefreshInterval`; a timer only comes into it when `topologyRefreshInterval` opts into the background poll.
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
  events: Events = Events.disabled,
  cachingEnabled: Boolean = false,
  cacheMaxBytes: Long = 0L
) extends Client[CIO, String] {

  private val topologyRef = new AtomicReference[ClusterTopology](ClusterTopology.from(Vector.empty))

  // READONLY at setup, so a replica serves reads for its master's slots instead of answering MOVED; separate from the master registry, which
  // drives redirects
  private val replicaPool   = new NodePool(
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
  private val keylessCursor = new java.util.concurrent.atomic.AtomicInteger()

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
    if (cachingEnabled) bootstrap :+ Connection.clientTrackingOnOptin else bootstrap,
    reconnect,
    watchdog,
    connectTimeout,
    closeTimeout,
    dedicatedPool,
    cacheMaxBytes = if (cachingEnabled) cacheMaxBytes else 0L,
    events = events,
    dedicatedBootstrap = Some(bootstrap)
  )
  private val reads            = new ReadRouting(masterPool, replicaPool, scheduler, readFrom, () => triggerRefresh())
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
        querySlotsVia(node) match {
          case Right(shards) =>
            adopt(node, shards)
            startRefreshPoll()
            return
          case Left(error)   => lastError = error
        }
      catch { case NonFatal(error) => lastError = error }
    }
    closeAll()
    throw lastError
  }

  def run[A](command: Command[A]): CIO[A] = {
    def body(lease: DedicatedPool.Lease): CIO[A] =
      CIO.async[A] { complete =>
        val tracked = Events.trackCommand(events, command, complete)
        Client.completing(tracked)(dispatch(command, cluster.maxRedirects, tracked, lease = lease))
      }
    Client.withLeaseIfBlocking(command)(body)
  }

  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command))
    else if (!cachingEnabled)
      CIO.async[A] { complete =>
        val tracked = Events.trackCommand(events, command, complete)
        Client.completing(tracked)(dispatch(command, cluster.maxRedirects, tracked, allowReplica = false))
      }
    else
      CIO.async[A] { complete =>
        val deferred = Events.deferSpan(events, command)
        Client.completing(complete)(
          dispatch(command, cluster.maxRedirects, complete, allowReplica = false, cacheCtx = Cached(ttl.toMillis, deferred))
        )
      }

  private[sage] def rateLimitAcquire[RK](executor: RateLimitExecutor[RK], subject: RK, cost: Long, peek: Boolean): CIO[Decision] =
    executor.evalSha(this, subject, cost, peek)

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
        def body(lease: DedicatedPool.Lease): CIO[A] =
          CIO.async[A] { complete =>
            val tracked = Events.trackCommand(events, command, complete)
            Client.completing(tracked)(sendTo(node, command, asking = false, redirectsLeft = 0, tracked, lease))
          }
        Client.withLeaseIfBlocking(command)(body)
      case None       => run(command)
    }

  private[sage] def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out]      = submitPipeline(p).flatMap(TxSupport.collapseStrict(_, p.toOut))
  private[sage] def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R] = submitPipeline(p).map(p.toResults)

  def transaction[A](body: TransactionScope[CIO, String] => CIO[A]): CIO[A] =
    CIO.acquireReleaseWith(acquireScope)(releaseScope)(scope => CIO.unit.flatMap(_ => body(scope)))

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

  final private case class Cached(ttlMillis: Long, deferred: () => CommandSpan)

  // a cached read is master-pinned, so it may use a replica only when there is no cache context
  private def replicaAllowed(cacheCtx: Cached): Boolean = cacheCtx == null

  private def dispatch[A](
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    allowReplica: Boolean = true,
    lease: DedicatedPool.Lease = null,
    cacheCtx: Cached = null
  ): Unit =
    if (closed) complete(Failure(NotConnected()))
    else {
      val topology = topologyRef.get()
      if (command.allMasters)
        if (slotOwningMasters(topology).forall(node => masterPool.existing(node) != null))
          broadcast(topology, command, redirectsLeft, complete, masterPool.existing)
        else scheduler.offload(broadcast(topology, command, redirectsLeft, complete, masterPool.getOrEstablishOrNull))
      else
        topology.route(command) match {
          case Route.ToNode(node, slot) => sendOwned(command, node, slot, redirectsLeft, complete, allowReplica, lease, cacheCtx)
          case Route.Keyless            =>
            if (servesFromReplica(command, allowReplica)) sendKeylessRead(topology, command, redirectsLeft, complete)
            else sendToAny(topology, command, redirectsLeft, complete, lease, cacheCtx)
          case Route.Unowned(_)         => scheduler.offload(onUnowned(command, redirectsLeft, complete, lease, cacheCtx))
          case Route.CrossSlot(slots)   =>
            multiSlotPolicy(command) match {
              case Some(policy) => scatterMultiSlot(command, policy, redirectsLeft, complete, allowReplica, cacheCtx)
              case None         => complete(Failure(crossSlot(command.name, slots)))
            }
          case Route.Malformed          =>
            complete(Failure(malformedKeys(command.name)))
        }
    }

  private def sendOwned[A](
    command: Command[A],
    node: Node,
    slot: Slot,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    allowReplica: Boolean,
    lease: DedicatedPool.Lease,
    cacheCtx: Cached
  ): Unit =
    if (servesFromReplica(command, allowReplica)) sendRead(command, node, slot, redirectsLeft, complete)
    else sendTo(node, command, asking = false, redirectsLeft, complete, lease, cacheCtx)

  private def servesFromReplica(command: Command[?], allowReplica: Boolean): Boolean =
    allowReplica && readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command)

  private enum MultiSlotMerge {
    case Positional, Sum, AllSucceeded
  }

  // suffixArgs are shared trailing args (e.g. JSON.MGET's path) re-appended to every per-slot subgroup after its keys
  final private case class MultiSlotPolicy(merge: MultiSlotMerge, argsPerKey: Int, suffixArgs: Int = 0)

  final private case class MultiSlotEntry(resultIndex: Int, argIndex: Int)

  // A supported logical cross-slot command becomes one ordinary command per exact slot, not per owner node. Every subgroup re-enters
  // dispatch so replica policy, topology refresh, and MOVED/ASK handling stay identical to a normal one-slot call. The merge waits for every
  // subgroup and decodes once through the original command: MGET scatters positional frames, integer commands sum per-slot counts, and MSET
  // requires every per-slot reply to succeed with OK.
  private def scatterMultiSlot[A](
    command: Command[A],
    policy: MultiSlotPolicy,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    allowReplica: Boolean,
    cacheCtx: Cached
  ): Unit = {
    val bySlot = mutable.LinkedHashMap.empty[Slot, mutable.ArrayBuffer[MultiSlotEntry]]
    command.keyIndices.iterator.zipWithIndex.foreach { case (argIndex, resultIndex) =>
      val key   = command.args(argIndex)
      val entry = MultiSlotEntry(resultIndex, argIndex)
      val group = bySlot.getOrElseUpdate(Slot.of(key), mutable.ArrayBuffer.empty)
      group += entry
    }
    val groups = bySlot.valuesIterator.map(_.toVector).toVector

    lazy val values = new java.util.concurrent.atomic.AtomicReferenceArray[Frame](command.keyIndices.size)
    lazy val total  = new java.util.concurrent.atomic.AtomicLong(0L)
    val remaining   = new java.util.concurrent.atomic.AtomicInteger(groups.size)
    val firstError  = new java.util.concurrent.atomic.AtomicReference[Throwable](null)

    def settle(group: Vector[MultiSlotEntry], result: Try[Frame]): Unit = {
      (policy.merge, result) match {
        case (MultiSlotMerge.Positional, Success(Frame.Array(elements))) if elements.size == group.size =>
          group.iterator.zip(elements).foreach { case (entry, frame) => values.set(entry.resultIndex, frame) }
        case (MultiSlotMerge.Positional, Success(Frame.Array(elements)))                                =>
          firstError.compareAndSet(
            null,
            DecodeError(s"an array of ${group.size} MGET values", s"an array of ${elements.size} values")
          )
        case (MultiSlotMerge.Positional, Success(other))                                                =>
          firstError.compareAndSet(null, DecodeError(s"an array of ${group.size} MGET values", Frame.describe(other)))
        case (MultiSlotMerge.Sum, Success(Frame.Integer(value)))                                        => total.addAndGet(value)
        case (MultiSlotMerge.Sum, Success(other))                                                       =>
          firstError.compareAndSet(null, DecodeError("an integer count", Frame.describe(other)))
        case (MultiSlotMerge.AllSucceeded, Success(Frame.SimpleString("OK")))                           => ()
        case (MultiSlotMerge.AllSucceeded, Success(other))                                              =>
          firstError.compareAndSet(null, DecodeError("simple string 'OK'", Frame.describe(other)))
        case (_, Failure(error))                                                                        =>
          firstError.compareAndSet(null, error)
      }

      if (remaining.decrementAndGet() == 0)
        Option(firstError.get()) match {
          case Some(error) => complete(Failure(error))
          case None        =>
            val merged = policy.merge match {
              case MultiSlotMerge.Positional   => Frame.Array(Vector.tabulate(command.keyIndices.size)(values.get))
              case MultiSlotMerge.Sum          => Frame.Integer(total.get())
              case MultiSlotMerge.AllSucceeded => Frame.SimpleString("OK")
            }
            complete(Reply.decode(command, merged))
        }
    }

    val raw = command.rawFrame
    groups.foreach { group =>
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
      dispatch(sub, redirectsLeft, result => settle(group, result), allowReplica = allowReplica, cacheCtx = cacheCtx)
    }
  }

  // Validate each recognized command's complete argument shape before splitting so a custom command that merely shares its name can never
  // lose or misassociate non-key arguments during subgroup construction.
  private def multiSlotPolicy(command: Command[?]): Option[MultiSlotPolicy] =
    command.name.toUpperCase(Locale.ROOT) match {
      case "MGET" if hasKeyStride(command, 1)                                => Some(MultiSlotPolicy(MultiSlotMerge.Positional, 1))
      case "DEL" | "EXISTS" | "TOUCH" | "UNLINK" if hasKeyStride(command, 1) => Some(MultiSlotPolicy(MultiSlotMerge.Sum, 1))
      case "MSET" if hasKeyStride(command, 2)                                => Some(MultiSlotPolicy(MultiSlotMerge.AllSucceeded, 2))
      // JSON.MSET is not split: unlike MSET, a triplet can fail path validation, so splitting could partially apply before a later group fails
      case "JSON.MGET" if hasLeadingKeys(command, 1)                         => Some(MultiSlotPolicy(MultiSlotMerge.Positional, 1, suffixArgs = 1))
      case _                                                                 => None
    }

  private def hasKeyStride(command: Command[?], argsPerKey: Int): Boolean =
    command.args.nonEmpty && command.args.size % argsPerKey == 0 &&
      command.keyIndices == Vector.tabulate(command.args.size / argsPerKey)(_ * argsPerKey)

  private def hasLeadingKeys(command: Command[?], suffixArgs: Int): Boolean =
    command.args.size > suffixArgs &&
      command.keyIndices == Vector.tabulate(command.args.size - suffixArgs)(identity)

  private def sendRead[A](command: Command[A], master: Node, slot: Slot, redirectsLeft: Int, complete: Try[A] => Unit): Unit = {
    val replicas = topologyRef.get().shardForSlot(slot).map(_.replicas).getOrElse(Vector.empty)
    walkRead(command, reads.candidatesFor(master, replicas), master, redirectsLeft, complete)
  }

  // a keyless eligible read (RANDOMKEY): round-robin over every replica, falling back per policy, so strict Replica never hits a master
  private def sendKeylessRead[A](topology: ClusterTopology, command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    pickNode(topology) match {
      case Some(master) =>
        val replicas = topology.shards.iterator.flatMap(_.replicas).toVector.distinct
        walkRead(command, reads.candidatesFor(master, replicas, keylessCursor.getAndIncrement()), master, redirectsLeft, complete)
      case None         => complete(Failure(NotConnected()))
    }

  private def walkRead[A](command: Command[A], candidates: Vector[Node], master: Node, redirectsLeft: Int, complete: Try[A] => Unit): Unit =
    reads.walk(command, candidates, master, complete)((node, error, rest) =>
      onReadFailure(node, command, error, rest, master, redirectsLeft, complete)
    )

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
      case Fault.Redirected(redirect)     =>
        redirect.kind match {
          // strict Replica must not follow ASK onto the importing master (the migrating key is on no replica); MOVED refreshes and re-dispatches
          case RedirectKind.Ask                         =>
            if (readFrom == ReadFrom.Replica) complete(Failure(NotConnected()))
            else onRedirect(node, redirect, command, redirectsLeft, complete)
          case RedirectKind.Moved if redirectsLeft <= 0 =>
            refreshBeforeFailing()
            complete(Failure(ServerError("ERR", s"exceeded ${cluster.maxRedirects} cluster redirects for ${command.name}")))
          case RedirectKind.Moved                       =>
            refresh(force = true)
            scheduler.offload(dispatch(command, redirectsLeft - 1, complete))
        }
      // only a loss that provably never ran may re-dispatch, per onUnreachable's contract
      case Fault.Lost(executed)           =>
        if (rest.nonEmpty) walkRead(command, rest, master, redirectsLeft, complete)
        else if (executed) {
          triggerRefresh()
          Events.attributeNode(complete, node)
          complete(Failure(error))
        } else onUnreachable(command, redirectsLeft, complete)
      case Fault.Unavailable(clusterWide) =>
        if (rest.nonEmpty) walkRead(command, rest, master, redirectsLeft, complete)
        else onRetryable(command, error, clusterWide, redirectsLeft, complete)
      case Fault.TryAgain                 => onRetryable(command, error, refreshFirst = false, redirectsLeft, complete)
      case Fault.Demoted                  =>
        triggerRefresh()
        Events.attributeNode(complete, node)
        complete(Failure(error))
      case Fault.Fatal                    =>
        Events.attributeNode(complete, node)
        complete(Failure(error))
    }

  private def broadcast[A](
    topology: ClusterTopology,
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    resolve: Node => NodeClient
  ): Unit =
    command.broadcast match {
      case BroadcastReduce.First      => sendToAllMasters(topology, command, redirectsLeft, complete, resolve)
      case BroadcastReduce.Concat     => broadcastCombine(topology, command, concatFrames, redirectsLeft, complete, resolve)
      case BroadcastReduce.Fold(fold) => broadcastCombine(topology, command, _.reduce(fold), redirectsLeft, complete, resolve)
    }

  // a broadcast command (SCRIPT LOAD, FUNCTION LOAD, …) runs on every slot-owning master, since a cluster replicates no script/function
  // cache; any node failing terminally fails the command
  private def sendToAllMasters[A](
    topology: ClusterTopology,
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    resolve: Node => NodeClient
  ): Unit = {
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
      masters.foreach(node => submitBroadcast(node, command, resolve, redirectsLeft, settle))
    }
  }

  // an all-masters broadcast whose per-node replies fold into one: KEYS concatenates each node's node-local slice (no single node sees the
  // whole keyspace), WAIT/WAITAOF reduce to the weakest shard. Frames are collected raw and combined before a single decode; any node
  // failing terminally fails the command
  private def broadcastCombine[A](
    topology: ClusterTopology,
    command: Command[A],
    combine: Vector[Frame] => Frame,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    resolve: Node => NodeClient
  ): Unit = {
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
            case None    => complete(Try(combine(Vector.tabulate(masters.size)(frames.get))).flatMap(Reply.decode(command, _)))
          }
      }
      masters.iterator.zipWithIndex.foreach { case (node, index) =>
        submitBroadcast(node, raw, resolve, redirectsLeft, result => settle(index, result))
      }
    }
  }

  private def submitBroadcast[B](node: Node, command: Command[B], resolve: Node => NodeClient, attemptsLeft: Int, settle: Try[B] => Unit): Unit = {
    val nc = resolve(node)
    // the dispatch fast path resolves inline on the caller, which the retry's refresh would block
    if (nc == null) scheduler.offload(retryBroadcast(node, command, NotConnected(), refreshFirst = true, attemptsLeft, settle))
    else
      nc.submit[B](
        command,
        asking = false,
        {
          case Success(value) => settle(Success(value))
          case Failure(error) => scheduler.offload(onBroadcastFailure(node, command, error, attemptsLeft, settle))
        }
      )
  }

  // only the node that lost its connection or refused for its own reasons is retried; a WAIT re-serves its own timeout there
  private def onBroadcastFailure[B](node: Node, command: Command[B], error: Throwable, attemptsLeft: Int, settle: Try[B] => Unit): Unit =
    Fault.categorize(error) match {
      case Fault.Lost(false)                         => retryBroadcast(node, command, error, refreshFirst = true, attemptsLeft, settle)
      case Fault.TryAgain | Fault.Unavailable(false) => retryBroadcast(node, command, error, refreshFirst = false, attemptsLeft, settle)
      // a cluster-wide refusal may have moved the masters this fan-out captured, so it is not chased
      case Fault.Unavailable(true)                   =>
        refreshBeforeFailing()
        settle(Failure(error))
      case fault                                     =>
        if (fault.refreshPolicy != RefreshPolicy.Skip) triggerRefresh()
        settle(Failure(error))
    }

  // a node the refreshed topology no longer lists as a slot-owning master makes this broadcast's target set stale, so it fails unchased
  private def retryBroadcast[B](
    node: Node,
    command: Command[B],
    error: Throwable,
    refreshFirst: Boolean,
    attemptsLeft: Int,
    settle: Try[B] => Unit
  ): Unit =
    if (attemptsLeft <= 0) {
      if (refreshFirst) refreshBeforeFailing()
      settle(Failure(error))
    } else
      afterBackoff(attemptsLeft) {
        if (refreshFirst) refresh(force = true)
        if (closed || !slotOwningMasters(topologyRef.get()).contains(node)) settle(Failure(error))
        else submitBroadcast(node, command, masterPool.getOrEstablishOrNull, attemptsLeft - 1, settle)
      }

  private def concatFrames(frames: Vector[Frame]): Frame =
    Frame.Array(frames.flatMap {
      case Frame.Array(elements) => elements
      case Frame.Set(elements)   => elements
      case other                 => Vector(other)
    })

  private def sendToAny[A](
    topology: ClusterTopology,
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease = null,
    cacheCtx: Cached = null
  ): Unit =
    pickNode(topology) match {
      case Some(node) => sendTo(node, command, asking = false, redirectsLeft, complete, lease, cacheCtx)
      case None       => complete(Failure(NotConnected()))
    }

  private def sendTo[A](
    node: Node,
    command: Command[A],
    asking: Boolean,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease,
    cacheCtx: Cached = null
  ): Unit = {
    val existing = masterPool.existing(node)
    if (existing != null) submitTo(existing, node, command, asking, redirectsLeft, complete, lease, cacheCtx)
    else
      scheduler.offload {
        val nc = masterPool.getOrEstablishOrNull(node)
        if (nc == null) onUnreachable(command, redirectsLeft, complete, lease, cacheCtx)
        else submitTo(nc, node, command, asking, redirectsLeft, complete, lease, cacheCtx)
      }
  }

  private def submitTo[A](
    nc: NodeClient,
    node: Node,
    command: Command[A],
    asking: Boolean,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease,
    cacheCtx: Cached
  ): Unit = {
    val onReply: Try[A] => Unit = {
      case Success(value) =>
        Events.attributeNode(complete, node)
        complete(Success(value))
      case Failure(error) => scheduler.offload(onFailure(node, command, error, redirectsLeft, complete, lease, cacheCtx))
    }
    if (cacheCtx != null && !asking) nc.cachedSubmit[A](command, cacheCtx.ttlMillis, onReply, cacheCtx.deferred)
    else nc.submit[A](command, asking, onReply, lease)
  }

  private def onFailure[A](
    node: Node,
    command: Command[A],
    error: Throwable,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease,
    cacheCtx: Cached
  ): Unit =
    Fault.categorize(error) match {
      case Fault.Redirected(redirect)       => onRedirect(node, redirect, command, redirectsLeft, complete, lease, cacheCtx)
      case Fault.Lost(false)                => onUnreachable(command, redirectsLeft, complete, lease, cacheCtx)
      case Fault.TryAgain                   => onRetryable(command, error, refreshFirst = false, redirectsLeft, complete, lease, cacheCtx)
      case Fault.Unavailable(clusterWide)   => onRetryable(command, error, clusterWide, redirectsLeft, complete, lease, cacheCtx)
      case Fault.Demoted | Fault.Lost(true) =>
        triggerRefresh()
        Events.attributeNode(complete, node)
        complete(Failure(error))
      case Fault.Fatal                      =>
        Events.attributeNode(complete, node)
        complete(Failure(error))
    }

  private def onRedirect[A](
    from: Node,
    redirect: Redirect,
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease = null,
    cacheCtx: Cached = null
  ): Unit = {
    // a MOVED proves `from` lost the slot; retire its cache even if the retry budget is now exhausted
    if (redirect.kind == RedirectKind.Moved) flushNode(from)
    if (redirectsLeft <= 0) {
      if (redirect.kind == RedirectKind.Moved) refreshBeforeFailing()
      complete(Failure(ServerError("ERR", s"exceeded ${cluster.maxRedirects} cluster redirects for ${command.name}")))
    } else {
      val target = resolve(redirect.target, from)
      redirect.kind match {
        case RedirectKind.Moved =>
          triggerRefresh()
          sendTo(target, command, asking = false, redirectsLeft - 1, complete, lease, cacheCtx)
        case RedirectKind.Ask   => sendTo(target, command, asking = true, redirectsLeft - 1, complete, lease, cacheCtx)
      }
    }
  }

  // the command provably never executed: refresh to adopt the promoted master, then re-route. Jittered backoff paces retries so an in-progress
  // failover is not hammered; redirectsLeft bounds it.
  private def onUnreachable[A](
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease = null,
    cacheCtx: Cached = null
  ): Unit = onRetryable(command, NotConnected(), refreshFirst = true, redirectsLeft, complete, lease, cacheCtx)

  // a self-clearing refusal (-TRYAGAIN, -LOADING, -MASTERDOWN, -CLUSTERDOWN): retry bounded and jittered
  private def onRetryable[A](
    command: Command[A],
    error: Throwable,
    refreshFirst: Boolean,
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    lease: DedicatedPool.Lease = null,
    cacheCtx: Cached = null
  ): Unit =
    if (redirectsLeft <= 0) {
      if (refreshFirst) refreshBeforeFailing()
      complete(Failure(error))
    } else {
      if (refreshFirst) refresh(force = true)
      afterBackoff(redirectsLeft)(
        dispatch(command, redirectsLeft - 1, complete, allowReplica = replicaAllowed(cacheCtx), lease = lease, cacheCtx = cacheCtx)
      )
    }

  // paces a retry, growing with the attempts already spent, so an in-progress failover or migration is not hammered
  private def afterBackoff(attemptsLeft: Int)(retry: => Unit): Unit =
    scheduler.after(Backoff.jitteredMillis(reconnect, (cluster.maxRedirects - attemptsLeft).max(0), scheduler).millis)(retry)

  // a path that will not retry must adopt a topology, not schedule one a throttle could drop
  private def refreshBeforeFailing(): Unit = refresh(force = true)

  private def onUnowned[A](command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit, lease: DedicatedPool.Lease, cacheCtx: Cached): Unit = {
    refresh(force = false)
    val topology     = topologyRef.get()
    val allowReplica = replicaAllowed(cacheCtx)
    topology.route(command) match {
      // re-apply the read policy: an eligible read must still go to a replica once the slot resolves, not be pinned to its master
      case Route.ToNode(node, slot)                                                                  =>
        sendOwned(command, node, slot, redirectsLeft, complete, allowReplica, lease, cacheCtx)
      // strict Replica must not fall back to a master: refresh and retry (bounded), never sendToAny
      case _ if allowReplica && readFrom == ReadFrom.Replica && ReadRouting.replicaEligible(command) =>
        onUnreachable(command, redirectsLeft, complete, lease, cacheCtx)
      // still unowned after refresh: any master, trusting its reply (a MOVED to follow, or CLUSTERDOWN)
      case _                                                                                         => sendToAny(topology, command, redirectsLeft, complete, lease, cacheCtx)
    }
  }

  // an empty redirect host means "the node I just talked to" (e.g. `MOVED 3999 :6381`)
  private def resolve(target: Node, from: Node): Node = if (target.host.isEmpty) Node(from.host, target.port) else target

  private def pickNode(topology: ClusterTopology): Option[Node] =
    masterPool.firstLiveNode.orElse(topology.shards.headOption.map(_.master))

  private def crossSlot(name: String, slots: Set[Slot]): CrossSlot =
    CrossSlot(s"$name: keys span ${slots.size} slots; a single command must touch exactly one")

  private def malformedKeys(name: String): InvalidArgument =
    InvalidArgument(s"$name: declared key positions fall outside its arguments")

  // a transaction never follows a redirect, so an ownership move must be adopted before the caller can retry: bypass the throttle window
  private def forceRefresh(): Unit = scheduler.offload(refresh(force = true))

  // --- pipelines (split per node, batch each, merge in submission order) ----------------------------------------------------------------

  private def submitPipeline[Out, R](p: Pipeline[Out, R]): CIO[Vector[Either[SageException, Any]]] =
    if (p.commands.isEmpty)
      CIO.value(Vector.empty)
    // a blocking command is a programmer error: fail the whole effect up front, never a single position
    else if (p.commands.exists(_.isBlocking))
      CIO.fail(InvalidArgument("a Pipeline cannot carry blocking commands; run them individually on the client"))
    // a Pipeline batches per node, so an all-masters command (SCRIPT LOAD, FUNCTION LOAD, KEYS, …) would touch one node only and either
    // break a later key-routed EVALSHA/FCALL or return a partial keyspace; fail up front rather than partially apply it
    else if (p.commands.exists(_.allMasters))
      CIO.fail(
        InvalidArgument("a Pipeline cannot carry an all-masters command (e.g. SCRIPT LOAD, FUNCTION LOAD, KEYS); run it individually on the client")
      )
    else
      CIO.async { complete =>
        runPipeline(p, complete, Events.deferSpans(events, p.commands))
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
    val n                           = p.commands.length
    val collector                   =
      new TxSupport.IndexedCollector[Either[SageException, Any]](n, results => complete(Success(results)))
    // a position opens its gate as it settles; a retry waits out the gate of the one before it on its slot, so same-key writes cannot invert
    val gates                       = Vector.fill(n)(new CountDownLatch(1))
    val slotAt                      = p.commands.map(slotOf)
    val emits                       = Vector.tabulate(n) { i =>
      val span                     = if (deferred.isEmpty) CommandSpan.noop else Events.startDeferred(deferred(i))
      val settle: Try[Any] => Unit = result => {
        gates(i).countDown()
        collector.set(i, TxSupport.toEither(result))
      }
      Events.trackCommand[Any](events, p.commands(i), settle, span)
    }
    def awaitTurn(index: Int): Unit = {
      val previous = if (slotAt(index) < 0) -1 else slotAt.lastIndexOf(slotAt(index), index - 1)
      if (previous >= 0) gates(previous).await()
    }
    // all-or-nothing: reroutes honor the same choice so a slot is never split across master and replica
    val useReplica                  = readFrom != ReadFrom.Master && p.commands.forall(ReadRouting.replicaEligible)
    // on the caller thread, so the wait gets a thread of its own; a retry from a reply is already offloaded
    def reroute(index: Int): Unit   = scheduler.offload {
      awaitTurn(index)
      dispatch(p.commands(index), cluster.maxRedirects, emits(index), allowReplica = useReplica)
    }

    plan.rejected.foreach {
      case (index, Rejected.CrossSlot(slots)) =>
        if (multiSlotPolicy(p.commands(index)).nonEmpty) reroute(index)
        else emits(index)(Failure(crossSlot(p.commands(index).name, slots)))
      case (index, Rejected.Unowned(_))       => reroute(index) // dispatch refreshes then re-routes
      case (_, Rejected.Malformed)            => ()             // unreachable: the guard above returned
    }
    // keyless positions ride along on the first node group's batch; with no keyed group, dispatch routes each to any node
    if (plan.perNode.isEmpty) plan.keyless.foreach(reroute)
    plan.perNode.zipWithIndex.foreach { case (NodeGroup(node, positions), groupIndex) =>
      // sorted: keep each node's batch in submission order even when keyless positions are folded into the first group
      sendBatch(node, if (groupIndex == 0) (positions ++ plan.keyless).sorted else positions, p, emits, reroute, awaitTurn, useReplica)
    }
  }

  // the slot a retry orders on; -1 for a keyless or cross-slot position, which orders against nothing
  private def slotOf(command: Command[?]): Int =
    topologyRef.get().route(command) match {
      case Route.ToNode(_, slot) => slot.value
      case Route.Unowned(slot)   => slot.value
      case _                     => -1
    }

  private def sendBatch[Out, R](
    node: Node,
    indices: Vector[Int],
    p: Pipeline[Out, R],
    emits: Vector[Try[Any] => Unit],
    reroute: Int => Unit,
    awaitTurn: Int => Unit,
    useReplica: Boolean
  ): Unit =
    // the batch attributes to the node it lands on (a replica when useReplica)
    if (useReplica) {
      val replicas = topologyRef.get().shards.collectFirst { case s if s.master == node => s.replicas }.getOrElse(Vector.empty)
      reads.pickOne(reads.candidatesFor(node, replicas), node) {
        case Some(picked) => submitBatch(picked.node, picked.client, indices, p, emits, reroute, awaitTurn, useReplica)
        case None         => indices.foreach(reroute)
      }
    } else {
      val existing = masterPool.existing(node)
      if (existing != null) submitBatch(node, existing, indices, p, emits, reroute, awaitTurn, useReplica)
      else
        scheduler.offload {
          val nc = masterPool.getOrEstablishOrNull(node)
          submitBatch(node, nc, indices, p, emits, reroute, awaitTurn, useReplica)
        }
    }

  private def submitBatch[Out, R](
    target: Node,
    nc: NodeClient,
    indices: Vector[Int],
    p: Pipeline[Out, R],
    emits: Vector[Try[Any] => Unit],
    reroute: Int => Unit,
    awaitTurn: Int => Unit,
    useReplica: Boolean
  ): Unit = {
    def settle(index: Int, result: Try[Any]): Unit = {
      Events.attributeNode(emits(index), target)
      emits(index)(result)
    }
    val callbacks: Vector[Try[Any] => Unit]        = indices.map { index => (result: Try[Any]) =>
      result match {
        case Success(_)     => settle(index, result)
        // a fault's disposition can block on CLUSTER SLOTS, whose reply needs this very reader thread
        case Failure(error) =>
          scheduler.offload {
            awaitTurn(index)
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
              case Fault.TryAgain                   => onRetryable(p.commands(index), error, refreshFirst = false, cluster.maxRedirects, emits(index))
              case Fault.Unavailable(clusterWide)   => onRetryable(p.commands(index), error, clusterWide, cluster.maxRedirects, emits(index))
              case Fault.Demoted | Fault.Lost(true) =>
                triggerRefresh()
                settle(index, result)
              case Fault.Fatal                      => settle(index, result)
            }
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
        scheduler.offload(withConn(command, tracked) { c =>
          armed.set(true)
          c.submit(command, faulting(tracked))
        })
      }
    }

    def run[A](command: Command[A]): CIO[A] =
      if (command.isBlocking)
        CIO.fail(InvalidArgument("a Transaction cannot run blocking commands; run them individually on the client"))
      else if (command.requiresClusterWideTxResult)
        CIO.fail(
          InvalidArgument(
            s"${command.name} returns a cluster-wide result that a single-node Transaction cannot produce; run it individually on the client"
          )
        )
      else
        CIO.async[A] { complete =>
          val tracked = Events.trackSpan(events, command, complete)
          scheduler.offload(withConn(command, tracked)(c => c.submit(command, faulting(tracked))))
        }

    def discard: CIO[Unit] =
      CIO.async[Unit] { complete =>
        scheduler.offload {
          lock.lock()
          try
            if (released) complete(Failure(TxSupport.scopeReleasedError))
            else if (conn == null) complete(Success(())) // nothing leased, nothing watched
            else
              Client.completing(complete) {
                armed.set(false)
                conn.submit(Connection.unwatch, faulting(complete))
              }
          finally lock.unlock()
        }
      }

    // a transaction never follows a redirect (that would break MULTI/EXEC atomicity), but on any ownership/connection fault it refreshes in the
    // background so the caller's retry re-pins to the new owner instead of looping on the stale node; a data error refreshes nothing
    private def refreshOnFault(error: Throwable): Unit = refreshFor(Vector(Fault.categorize(error)))

    private def refreshFor(faults: Vector[Fault]): Unit =
      faults.iterator.map(_.refreshPolicy).maxByOption(_.ordinal) match {
        case Some(RefreshPolicy.Forced)    => forceRefresh()
        case Some(RefreshPolicy.Throttled) => triggerRefresh()
        case _                             => ()
      }

    private def faulting[A](complete: Try[A] => Unit): Try[A] => Unit = {
      case failure @ Failure(error) =>
        refreshOnFault(error)
        complete(failure)
      case success                  => complete(success)
    }

    private def refreshOnExecFault(frames: Vector[Frame]): Unit =
      refreshFor(TxSupport.execErrors(frames).map(Fault.categorize).toVector)

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
        CIO.fail(InvalidArgument("a Transaction cannot carry blocking commands; run them individually on the client"))
      else if (p.commands.exists(_.requiresClusterWideTxResult))
        CIO.fail(
          InvalidArgument(
            "a Transaction cannot carry a command that returns a cluster-wide result; run it individually on the client"
          )
        )
      else
        CIO
          .async[Vector[Frame]] { complete =>
            val tracked = Events.trackSpan(events, Connection.multi, complete)
            scheduler.offload(submitExec(p, tracked))
          }
          .flatMap { frames =>
            armed.set(false) // EXEC clears WATCH/MULTI state server-side whether it committed or aborted
            refreshOnExecFault(frames)
            TxSupport.interpretExec(p.commands, frames)
          }

    // validates the whole pipeline's slots against the pin *before* sending MULTI, so a cross-slot transaction is rejected with nothing on the wire
    private def submitExec[Out, R](p: Pipeline[Out, R], complete: Try[Vector[Frame]] => Unit): Unit =
      onConn(pipelineSlot(p), complete)(c => c.submitRaw(Connection.multi +: p.commands :+ Connection.exec, faulting(complete)))

    private def withConn[A](command: Command[?], complete: Try[A] => Unit)(use: DedicatedConnection => Unit): Unit =
      onConn(commandSlot(command), complete)(use)

    // checks and submits stay under `lock` (release-vs-late-submit is atomic); the slow acquire runs outside it so release() never stalls behind it
    private def onConn[A](slotResult: Either[Throwable, Option[Slot]], complete: Try[A] => Unit)(use: DedicatedConnection => Unit): Unit = {
      var retry = true
      while (retry) {
        retry = false
        var fault: Throwable   = null
        var acquire            = false
        var slot: Option[Slot] = None
        lock.lock()
        try
          if (released) complete(Failure(TxSupport.scopeReleasedError))
          else
            slotResult match {
              case Left(error) =>
                fault = error
                complete(Failure(error))
              case Right(s)    =>
                if (conn == null) {
                  acquire = true
                  slot = s
                } else
                  checkPin(s) match {
                    case Left(error) =>
                      fault = error
                      complete(Failure(error))
                    case Right(())   => Client.completing(complete)(use(conn))
                  }
            }
        finally lock.unlock()
        if (fault != null) refreshOnFault(fault)
        else if (acquire)
          acquireConn(slot) match {
            case Left(error)               =>
              refreshOnFault(error)
              complete(Failure(error))
            case Right((nc, c, node, pin)) =>
              var giveBack = false
              lock.lock()
              try
                if (released) {
                  giveBack = true
                  complete(Failure(TxSupport.scopeReleasedError))
                }
                // lost the acquire race; re-validate against the winner's pin
                else if (conn != null) {
                  giveBack = true
                  retry = true
                } else {
                  nodeClient = nc
                  conn = c
                  pinnedNode = node
                  pinnedSlot = pin
                  Client.completing(complete)(use(conn))
                }
              finally lock.unlock()
              if (giveBack) nc.releaseTransaction(c, reusable = true)
          }
      }
    }

    // must hold `lock`, conn != null; a keyless-acquired pin adopts the first keyed slot only if its node already owns it
    private def checkPin(slot: Option[Slot]): Either[Throwable, Unit] =
      slot match {
        case None    => Right(())
        case Some(s) =>
          pinnedSlot match {
            case Some(ps) if ps == s => Right(())
            case Some(ps)            =>
              Left(CrossSlot(s"transaction touches slot ${s.value} but is pinned to slot ${ps.value}; MULTI/EXEC requires a single slot"))
            case None                =>
              if (topologyRef.get().nodeForSlot(s).contains(pinnedNode)) {
                pinnedSlot = Some(s)
                Right(())
              } else Left(CrossSlot(s"transaction touches slot ${s.value} on a node other than its pinned one; MULTI/EXEC requires a single slot"))
          }
      }

    // runs outside `lock`: may force a topology refresh, connect, or wait for a pool slot
    private def acquireConn(slot: Option[Slot]): Either[Throwable, (NodeClient, DedicatedConnection, Node, Option[Slot])] = {
      val target = slot match {
        case Some(s) => nodeForSlotRefreshing(s).map(_ -> slot)
        case None    => pickNode(topologyRef.get()).map(_ -> None)
      }
      target match {
        case None              => Left(NotConnected())
        case Some((node, pin)) =>
          try {
            val nc = masterPool.getOrEstablish(node)
            Right((nc, nc.acquireForTransaction(), node, pin))
          } catch {
            case error: SageException => Left(error)
            case NonFatal(_)          => Left(ConnectionLost(mayHaveExecuted = false))
          }
      }
    }

    private def nodeForSlotRefreshing(slot: Slot): Option[Node] =
      topologyRef.get().nodeForSlot(slot).orElse {
        refresh(force = true)
        topologyRef.get().nodeForSlot(slot)
      }

    // transaction pinning depends on the key slot, not its current owner, so both owned and unowned routes retain the classified slot
    private def commandSlot(command: Command[?]): Either[Throwable, Option[Slot]] =
      topologyRef.get().route(command) match {
        case Route.Malformed        => Left(malformedKeys(command.name))
        case Route.Keyless          => Right(None)
        case Route.ToNode(_, slot)  => Right(Some(slot))
        case Route.Unowned(slot)    => Right(Some(slot))
        case Route.CrossSlot(slots) => Left(crossSlot(command.name, slots))
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
        try {
          released = true
          (nodeClient, conn, conn != null && conn.isHealthy && conn.isQuiescent && !armed.get)
        } finally lock.unlock()
      if (nc != null) nc.releaseTransaction(c, reusable)
    }
  }

  private def flushNode(node: Node): Unit =
    if (cachingEnabled) {
      val nc = masterPool.existing(node)
      if (nc != null) nc.flushCache()
    }

  // --- topology refresh (single-flight, throttled) -------------------------------------------------------------------------------------

  private def triggerRefresh(): Unit = scheduler.offload(refresh(force = false))

  private def startRefreshPoll(): Unit = refreshThrottle.startPolling(cluster.topologyRefreshInterval)(triggerRefresh())

  // a refresh queued before close must not re-establish what the close tore down
  private def refresh(force: Boolean): Unit =
    if (!closed) refreshThrottle(force) {
      querySlots(refreshCandidates()) match {
        case Some((from, shards)) => adopt(from, shards)
        // no candidate answered CLUSTER SLOTS: ownership is unknowable, so retire every cache
        case None                 => if (cachingEnabled) masterPool.foreachEstablished(_.flushCache())
      }
    }

  private def refreshCandidates(): Vector[Node] = (masterPool.candidatesByLiveness ++ seeds).distinct

  private def querySlots(candidates: Vector[Node]): Option[(Node, Vector[Shard])] =
    candidates.iterator.flatMap(trySlots).nextOption()

  private def trySlots(node: Node): Option[(Node, Vector[Shard])] =
    try querySlotsVia(node).toOption.map(node -> _)
    catch { case NonFatal(_) => None }

  // a node outside a formed cluster answers CLUSTER SLOTS with an empty array: a non-answer, not a topology owning no slots
  private def querySlotsVia(node: Node): Either[Throwable, Vector[Shard]] = {
    val nc = masterPool.getOrEstablish(node)
    Bootstrap.awaitReply[Vector[Shard]](connectTimeout.toMillis)(callback => nc.submit(Cluster.slots, asking = false, callback)) match {
      case None                                     =>
        Left(TimedOut(s"CLUSTER SLOTS on ${node.host}:${node.port} timed out after ${connectTimeout.toMillis}ms"))
      case Some(Success(shards)) if shards.nonEmpty =>
        Right(shards)
      case Some(Success(_))                         =>
        Left(UnsupportedServer(s"${node.host}:${node.port} owns no slots: it is not part of a formed cluster"))
      case Some(Failure(error: ServerError))        =>
        Left(UnsupportedServer(s"${node.host}:${node.port} rejected CLUSTER SLOTS: ${error.getMessage}"))
      case Some(Failure(error))                     =>
        Left(error)
    }
  }

  // prunes bundles for masters it no longer lists, so a vanished node's reconnect loop cannot leak; an empty announce-IP from CLUSTER SLOTS
  // means "the node I queried", so substitute `from` as redirects do
  private def adopt(from: Node, shards: Vector[Shard]): Unit = {
    val resolved     = shards.map(shard => shard.copy(master = resolve(shard.master, from), replicas = shard.replicas.map(resolve(_, from))))
    val oldTopology  = topologyRef.get()
    val previous     = if (events.emitsEvents) slotOwningMasters(oldTopology).toSet else Set.empty[Node]
    val newTopology  = ClusterTopology.from(resolved)
    // retire losing masters' caches before the new topology is published
    if (cachingEnabled) newTopology.mastersLosingSlots(oldTopology).foreach(flushNode)
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
    reads.retain(masters.contains)
    // re-home shard subscriptions only when slot ownership changed; else a forced refresh mid-failover loops (refresh -> adopt -> reconcile ->
    // refresh) at RTT. A classic subscription follows the cluster bus, so it re-homes only when its pinned master's socket drops, never here.
    if (!newTopology.sameOwnership(oldTopology)) subscriptions.onTopologyChanged()
  }

  private def closeAll(): Unit = {
    closed = true
    refreshThrottle.stopPolling()
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
        events,
        config.clientCache.enabled,
        config.clientCache.maxBytes
      )
      // translate discovery's handshake/TLS failures here rather than via mapError, which the per-backend CIO alias does not reconcile through
      // `Client`'s invariant type parameter
      try {
        live.bootstrapTopology()
        live
      } catch {
        case NonFatal(error) =>
          events.close()
          throw translate(error)
      }
    }
}

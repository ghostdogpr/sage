package sage.client.internal

import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import kyo.compat.*

import sage.{Message, PatternMessage, SageException}
import sage.SageException.InvalidArgument
import sage.client.{ReadFrom, SageConfig}
import sage.cluster.Node
import sage.codec.ValueCodec
import sage.commands.{Command, Pipeline}
import sage.ratelimit.Decision

/**
  * The operations of the shared `CIO` client that backend adapters run directly. `scanTargets` returns every keyspace SCAN must visit,
  * and `lockWrite` waits for the replica acknowledgement a lock requires.
  */
private[sage] trait SharedRunner extends ScanTarget {

  // a single keyspace: the cluster runtime overrides this to scan every master
  def scanTargets: CIO[Vector[ScanTarget]] = CIO.value(Vector(this))

  // a standalone server has no replicas to wait for
  def lockWrite(command: Command[Boolean], @unused timeout: FiniteDuration, @unused replicaAcknowledgement: Boolean): CIO[Boolean] =
    run(command)
}

private[sage] object SharedRunner {

  // the default of Client.runner, which no Sage client uses
  val unavailable: SharedRunner = new SharedRunner {
    def run[A](command: Command[A]): CIO[A] = CIO.fail(InvalidArgument("this operation needs a client created by Sage"))
  }
}

private[internal] trait LiveClient(events: Events) extends Client[CIO, String] with SharedRunner {

  final protected inline def tracked[A](command: Command[A])(inline submit: (Try[A] => Unit) => Unit): CIO[A] =
    CIO.async[A] { complete =>
      val t = Events.trackCommand(events, command, complete)
      Client.completing(t)(submit(t))
    }

  // receives only non-empty pipelines without blocking commands
  protected def submitPipeline[R](p: Pipeline[R]): CIO[Vector[Either[SageException, Any]]]

  // receives only cacheable commands
  protected def cachedChecked[A](command: Command[A], ttl: FiniteDuration): CIO[A]

  protected def pubsub: SubscriptionConnection.PubSub

  protected def openTransaction: CIO[LiveTransactionScope]

  // the first connection or topology discovery
  protected def establish(): Unit

  protected def shutdown(): Unit

  // a failed start, including an interrupted one, closes everything the client opened
  final private[client] def start(): Unit = onThrow(establish())(_ => shutdown())

  final def close: CIO[Unit] = CIO.blocking(shutdown())

  // Release the transaction connection after success, failure, or interruption. Return it to the pool only after EXEC or UNWATCH has
  // cleared WATCH/MULTI state and no replies remain pending. Discard it when watched keys or commands may still be active.
  final def transaction[A](body: TransactionScope[CIO, String] => CIO[A]): CIO[A] =
    CIO.acquireReleaseWith(openTransaction)(scope => CIO.blocking(scope.release()))(scope => CIO.unit.flatMap(_ => body(scope)))

  final def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (!Client.cacheable(command)) CIO.fail(Client.notCacheable(command)) else cachedChecked(command, ttl)

  final def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    CIO.blocking(Client.channelMessages(pubsub.subscribeChannels(channel +: rest.toVector)))

  final def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]] =
    CIO.blocking(Client.patternMessages(pubsub.subscribePatterns(pattern +: rest.toVector)))

  final def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    CIO.blocking(Client.channelMessages(pubsub.subscribeShard(channel +: rest.toVector)))

  final override private[sage] def runner: SharedRunner = this

  final private[sage] def rateLimitAcquire[RK](executor: RateLimitExecutor[RK], subject: RK, cost: Long, peek: Boolean): CIO[Decision] =
    executor.evalSha(this, subject, cost, peek)

  final private[sage] def lockTryWith[LK, A](executor: LockExecutor[LK], key: LK)(body: => CIO[A]): CIO[Option[A]] =
    executor.tryWithLock(this, key)(body)

  final private[sage] def lockWith[LK, A](executor: LockExecutor[LK], key: LK, waitTimeout: FiniteDuration)(body: => CIO[A]): CIO[A] =
    executor.withLock(this, key, waitTimeout)(body)

  final private[sage] def pipeline[R](p: Pipeline[R]): CIO[R] = checked(p).flatMap(p.finish(_).fold(CIO.fail(_), CIO.value(_)))

  private def checked[R](p: Pipeline[R]): CIO[Vector[Either[SageException, Any]]] =
    if (p.commands.isEmpty) CIO.value(Vector.empty)
    else if (p.commands.exists(_.isBlocking))
      CIO.fail(InvalidArgument("a Pipeline cannot carry blocking commands; run them individually on the client"))
    else submitPipeline(p)
}

// The cluster and master-replica runtimes route each command to a node according to its DispatchMode, chosen once per call.
abstract private[internal] class RoutedClient(
  nodeFactory: Node => MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  replicaRole: MultiplexedConnection.NodeRole,
  config: SageConfig,
  minRefreshInterval: FiniteDuration,
  pollInterval: Option[FiniteDuration],
  events: Events
) extends LiveClient(events) {
  import RoutedClient.DispatchMode
  import RoutedClient.DispatchMode.*

  protected val masterPool       = new NodePool(nodeFactory, scheduler, config, MultiplexedConnection.NodeRole.Master, events)
  protected val replicaPool      = new NodePool(nodeFactory, scheduler, config, replicaRole, events)
  protected val reads            = new ReadRouting(masterPool, replicaPool, scheduler, config.readFrom, () => triggerRefresh())
  protected val refreshThrottle  = new RefreshThrottle(scheduler, minRefreshInterval.toMillis, () => rediscover())
  // set once by close; routing refuses afterwards, so close is terminal like the standalone client's
  @volatile private var isClosed = false
  @volatile private var polling  = Option.empty[Scheduler.Cancelable]

  final protected def closed: Boolean = isClosed

  protected def route[A](command: Command[A], complete: Try[A] => Unit, lease: DedicatedPool.Lease, mode: DispatchMode): Unit

  // the replicas a lock write on this master waits for
  protected def replicaCount(master: Node): Int

  // the first topology discovery, from the seeds
  protected def discover(): Either[Throwable, Unit]

  protected def rediscover(): Unit

  final protected def triggerRefresh(): Unit = refreshThrottle.request()

  final protected def establish(): Unit = discover().fold(error => throw error, _ => polling = pollInterval.map(scheduler.every(_)(triggerRefresh())))

  final protected def shutdown(): Unit = {
    refreshThrottle.stop()
    polling.foreach(_.cancel())
    isClosed = true
    pubsub.close()
    masterPool.close()
    replicaPool.close()
    events.close()
  }

  final def run[A](command: Command[A]): CIO[A] =
    Client.withLeaseIfBlocking(command)(lease => tracked(command)(route(command, _, lease, readMode(command))))

  final override def lockWrite(command: Command[Boolean], timeout: FiniteDuration, replicaAcknowledgement: Boolean): CIO[Boolean] =
    Client.withLockLease(timeout, scheduler) { (lease, deadlineMillis) =>
      tracked(command)(route(command, _, lease, Confirmed(deadlineMillis, replicaAcknowledgement)))
    }

  final protected def cachedChecked[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (!config.clientCache.enabled) tracked(command)(route(command, _, null, MasterOnly))
    else
      CIO.async[A](complete =>
        Events.trackCached(events, command, complete)((t, trace) => Client.completing(t)(route(command, t, null, Cached(ttl.toMillis, trace))))
      )

  final protected def readMode(command: Command[?]): DispatchMode =
    if (config.readFrom != ReadFrom.Master && ReadRouting.replicaEligible(command)) ReplicaRead else MasterOnly

  // a pipeline goes to a replica only when every command is eligible
  final protected def pipelineMode(commands: Vector[Command[?]]): DispatchMode =
    if (config.readFrom != ReadFrom.Master && commands.forall(ReadRouting.replicaEligible)) ReplicaRead else MasterOnly

  // Sends one attempt to node; onReply receives this attempt's result.
  final protected def submitOn[A](
    nc: MultiplexedConnection,
    node: Node,
    command: Command[A],
    lease: DedicatedPool.Lease,
    mode: DispatchMode,
    asking: Boolean,
    onReply: Try[A] => Unit
  ): Unit =
    mode match {
      case Cached(ttlMillis, trace) if !asking               => nc.cachedSubmit[A](command, ttlMillis, onReply, trace)
      // an ASK attempt bypasses the cache
      case Cached(_, trace)                                  => nc.submit[A](command, trace.fetching(command, onReply), asking, lease)
      case Confirmed(deadlineMillis, replicaAcknowledgement) =>
        val replication =
          new LockReplication(scheduler, replicaCount(node), deadlineMillis, () => triggerRefresh(), replicaAcknowledgement)
        nc.pool.useLockWrite(command, asking, onReply, lease, replication)
      case ReplicaRead | MasterOnly                          => nc.submit[A](command, onReply, asking, lease)
    }
}

private[internal] object RoutedClient {

  enum DispatchMode {
    case ReplicaRead, MasterOnly
    // trace starts the call's span when a read is sent, whichever callback the attempt completes
    case Cached(ttlMillis: Long, trace: Events.Fetching)
    case Confirmed(deadlineMillis: Long, replicaAcknowledgement: Boolean)
  }
}

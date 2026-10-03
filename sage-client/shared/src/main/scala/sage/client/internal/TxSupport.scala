package sage.client.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReferenceArray}
import java.util.concurrent.locks.ReentrantLock

import scala.annotation.unused
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import kyo.compat.*

import sage.{Message, PatternMessage, SageException}
import sage.SageException.{DecodeError, InvalidArgument, ProtocolError, ServerError, TransactionDiscarded}
import sage.client.{ReadFrom, SageConfig}
import sage.cluster.Node
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Connection, Pipeline, Reply}
import sage.protocol.Frame
import sage.ratelimit.Decision

/**
  * The Pipeline/Transaction result-shaping shared by the standalone client and the cluster runtime, kept in one place so the
  * per-position model and the MULTI/EXEC interpretation cannot drift between them.
  */
private[internal] object TxSupport {

  // decoders should return Either; another exception indicates a decoder bug and becomes DecodeError to preserve per-command results
  def toEither(result: Try[Any]): Either[SageException, Any] =
    result match {
      case Success(value)            => Right(value)
      case Failure(e: SageException) => Left(e)
      case Failure(other)            => Left(DecodeError.fromThrowable(other))
    }

  // the replies to MULTI and to each queued command, then the reply to EXEC; a Failure is the server's error reply
  final case class ExecReplies(queued: Vector[Try[Frame]], exec: Try[Frame])

  // Return None when EXEC reports that a watched key changed. Otherwise, return each command's decoded result. A queueing error fails the
  // transaction before it executes.
  def interpretExec(commands: Vector[Command[?]], replies: ExecReplies): Either[SageException, Option[Vector[Either[SageException, Any]]]] =
    replies.queued.collectFirst { case Failure(error) => error } match {
      case Some(error) => Left(TransactionDiscarded(error.getMessage))
      case None        =>
        val n = commands.length
        replies.exec match {
          case Failure(error)                                   => Left(TransactionDiscarded(error.getMessage))
          case Success(Frame.Null)                              => Right(None)
          case Success(Frame.Array(elems)) if elems.length == n =>
            Right(Some(Vector.tabulate(n)(i => toEither(Reply.decode(commands(i), elems(i))))))
          case Success(Frame.Array(elems))                      =>
            Left(ProtocolError(s"EXEC returned ${elems.length} results for $n queued commands"))
          case Success(other)                                   => Left(ProtocolError(s"unexpected EXEC reply: ${Frame.describe(other)}"))
        }
    }

  // Return error replies from either the top-level transaction replies or the commands' results inside EXEC.
  def execErrors(replies: ExecReplies, interpreted: Either[SageException, Option[Vector[Either[SageException, Any]]]]): Iterator[ServerError] =
    (replies.queued.iterator ++ Iterator.single(replies.exec)).collect { case Failure(error: ServerError) => error } ++
      interpreted.toOption.flatten.iterator.flatten.collect { case Left(error: ServerError) => error }

  // using a transaction scope after its block ends is an invalid state and returns IllegalStateException instead of a SageException
  def scopeReleasedError: IllegalStateException =
    new IllegalStateException("transaction scope used after its block returned")

  /**
    * Collects results from independent callbacks by their original index. Each index is set once, either by its command reply or its final
    * routing result. When all indices are set, the countdown invokes `complete` once. Standalone and cluster pipelines use this collector
    * because they wait for every result. `RawBatch` uses separate logic because it completes after the first failure.
    */
  final class IndexedCollector[A](n: Int, complete: Vector[A] => Unit) {
    private val slots     = new AtomicReferenceArray[A](n)
    private val remaining = new AtomicInteger(n)

    def set(index: Int, value: A): Unit = {
      slots.set(index, value)
      if (remaining.decrementAndGet() == 0) complete(Vector.tabulate(n)(slots.get))
    }
  }
}

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

private[internal] trait LiveTransactionScope(events: Events, refresh: RefreshPolicy => Unit) extends TransactionScope[CIO, String] {

  protected val lock     = new ReentrantLock()
  protected var released = false

  // true after WATCH is attempted and false after EXEC or UNWATCH; prevents reuse while the server may still track watched keys
  protected val armed = new AtomicBoolean(false)

  // what selects the transaction connection; the cluster derives a slot from the commands, standalone needs nothing
  protected type Target
  protected def targetOf(command: Command[?]): Target
  protected def targetOf(commands: Vector[Command[?]]): Target

  // Run `use` on the transaction connection while holding `lock`, or complete with the reason no connection is available. A command
  // accepted before release is recorded as in flight before [[release]] checks the connection, and commands submitted after release are
  // rejected.
  protected def withConn[A](target: Target, complete: Try[A] => Unit)(use: DedicatedConnection => Unit): Unit

  final protected def onFault(error: Throwable): Unit = refresh(Fault.categorize(error).refreshPolicy)

  final protected def faulting[A](complete: Try[A] => Unit): Try[A] => Unit = {
    case failure @ Failure(error) =>
      onFault(error)
      complete(failure)
    case success                  => complete(success)
  }

  final def watch[K: KeyCodec](key: K, rest: K*): CIO[Unit] = {
    val command = Connection.watch(key, rest*)
    CIO.async[Unit] { complete =>
      val tracked = Events.trackSpan(events, command, complete)
      withConn(targetOf(command), tracked) { conn =>
        armed.set(true)
        conn.submit(command, faulting(tracked))
      }
    }
  }

  def run[A](command: Command[A]): CIO[A] =
    if (command.isBlocking)
      CIO.fail(InvalidArgument("a Transaction cannot run blocking commands; run them individually on the client"))
    else
      CIO.async[A] { complete =>
        val tracked = Events.trackSpan(events, command, complete)
        withConn(targetOf(command), tracked)(_.submit(command, faulting(tracked)))
      }

  final def discard: CIO[Unit] =
    CIO.async[Unit] { complete =>
      lock.lock()
      try
        if (released) complete(Failure(TxSupport.scopeReleasedError))
        else {
          val conn = leasedConn
          if (conn == null) complete(Success(())) // the transaction has not leased a connection or sent WATCH
          else
            Client.completing(complete) {
              armed.set(false)
              conn.submit(Connection.unwatch, faulting(complete))
            }
        }
      finally lock.unlock()
    }

  // receives only pipelines without blocking commands, from a scope that is not released
  protected def sendMultiExec[R](p: Pipeline[R]): CIO[TxSupport.ExecReplies] =
    CIO.async[TxSupport.ExecReplies] { complete =>
      val tracked = Events.trackSpan(events, Connection.multi, complete)
      withConn(targetOf(p.commands), tracked)(_.submitExec(p.commands, faulting(tracked)))
    }

  // the leased connection, or null while none is leased; read under `lock`
  protected def leasedConn: DedicatedConnection

  protected def giveBack(conn: DedicatedConnection, reusable: Boolean): Unit

  // Reject further operations, then release the transaction connection. Reuse it only when healthy, with no pending commands or watched
  // keys. A transaction that did not submit any commands has no connection to release.
  final private[internal] def release(): Unit = {
    lock.lock()
    val (conn, reusable) =
      try {
        released = true
        val c = leasedConn
        (c, c != null && c.isQuiescent && !armed.get)
      } finally lock.unlock()
    if (conn != null) giveBack(conn, reusable)
  }

  protected def isReleased: Boolean = {
    lock.lock()
    try released
    finally lock.unlock()
  }

  final private[sage] def exec[R](p: Pipeline[R]): CIO[Option[R]] =
    runExec(p).flatMap {
      case None          => CIO.value(None)
      case Some(results) => p.finish(results).map(Some(_)).fold(CIO.fail(_), CIO.value(_))
    }

  // return None when EXEC reports a WATCH abort and Some with one decoded result per command; a queueing error fails the effect before execution
  private def runExec[R](p: Pipeline[R]): CIO[Option[Vector[Either[SageException, Any]]]] =
    if (isReleased)
      CIO.fail(TxSupport.scopeReleasedError)
    // skip MULTI/EXEC for an empty pipeline only when WATCH is inactive; watched keys still require EXEC to detect concurrent changes
    else if (p.commands.isEmpty && !armed.get)
      CIO.value(Some(Vector.empty))
    else if (p.commands.exists(_.isBlocking))
      CIO.fail(InvalidArgument("a Transaction cannot carry blocking commands; run them individually on the client"))
    else
      sendMultiExec(p).flatMap { replies =>
        armed.set(false) // EXEC clears WATCH/MULTI state server-side whether it committed or aborted
        val interpreted = TxSupport.interpretExec(p.commands, replies)
        TxSupport.execErrors(replies, interpreted).map(Fault.categorize(_).refreshPolicy).maxByOption(_.ordinal).foreach(refresh)
        interpreted.fold(CIO.fail(_), CIO.value(_))
      }
}

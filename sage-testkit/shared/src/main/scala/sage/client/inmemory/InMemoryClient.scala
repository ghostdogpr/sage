package sage.client.inmemory

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

import kyo.compat.*

import sage.{Message, PatternMessage, SageException}
import sage.SageException.{InvalidArgument, NotCacheable, NotConnected, TransactionDiscarded}
import sage.client.internal.{Client, LockExecutor, RateLimitExecutor, SharedRunner, Subscription, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Attempt, Command, Pipeline, Reply}
import sage.protocol.Frame
import sage.ratelimit.Decision
import sage.testkit.{InMemoryServer, TestClock}

/**
  * A [[Client]] that runs commands against an in-memory [[Store]] instead of a server. Every command is executed from its name and encoded
  * arguments, and its reply frame goes through the command's own decoder, so results and errors have the same types as with a real
  * server. Each backend wraps this client in its usual adapter, the same way it wraps the networked client.
  *
  * `currentMillis` gives the current Unix time in milliseconds and is read once per command, pipeline or `EXEC`, before the store lock is taken.
  * By default it is `CIO.now`, which each backend binds to its own clock, so test clocks such as ZIO's `TestClock` control expiry. Two
  * commands that run concurrently may therefore execute in a different order than their time readings, which is harmless because each
  * command still sees a consistent time.
  *
  * Commands that the store does not implement fail with a `ServerError` whose code is `ERR`.
  */
final private[sage] class InMemoryClient(executor: Executor, currentMillis: CIO[Long]) extends Client[CIO, String] with SharedRunner {

  private val closed = new AtomicBoolean(false)

  def run[A](command: Command[A]): CIO[A] =
    ifOpen {
      val reply =
        if (command.isBlocking) blocking(command)
        else currentMillis.flatMap(now => CIO.defer(executor.execute(command.name, command.args, now)))
      reply.flatMap(frame => CIO.get(Reply.decode(command, frame)))
    }

  // The in-memory store has no client-side cache to keep, so a cached read is an ordinary read once the command is known to qualify.
  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A] =
    if (command.cacheable && !command.isBlocking && command.keyIndices.nonEmpty && !command.hasMalformedKeys) run(command)
    else
      CIO.fail(
        NotCacheable(s"${command.name} is not cacheable: cached requires a cacheable command with at least one key within its arguments")
      )

  private[sage] def pipeline[R](p: Pipeline[R]): CIO[R] =
    if (p.commands.exists(_.isBlocking))
      CIO.fail(InvalidArgument("a Pipeline cannot carry blocking commands; run them individually on the client"))
    else
      ifOpen(currentMillis.flatMap { now =>
        CIO.defer {
          // a pipeline is not atomic, so each command takes the lock separately
          val results = p.commands.map(c => attempt(c, executor.execute(c.name, c.args, now)))
          p.finish(results)
        }
      }).flatMap(_.fold(CIO.fail(_), CIO.value(_)))

  def transaction[A](body: TransactionScope[CIO, String] => CIO[A]): CIO[A] =
    ifOpen(CIO.acquireReleaseWith(CIO.defer(new Scope))(scope => CIO.defer(scope.release()))(scope => body(scope)))

  // The lock and rate-limit executors run their Lua scripts through `run`. Until the store implements those scripts, these calls fail
  // with the same `ERR` reply as any other unsupported command.
  private[sage] def lockTryWith[LK, A](executor: LockExecutor[LK], key: LK)(body: => CIO[A]): CIO[Option[A]] =
    executor.tryWithLock(this, key)(body)

  private[sage] def lockWith[LK, A](executor: LockExecutor[LK], key: LK, waitTimeout: FiniteDuration)(body: => CIO[A]): CIO[A] =
    executor.withLock(this, key, waitTimeout)(body)

  private[sage] def rateLimitAcquire[RK](executor: RateLimitExecutor[RK], subject: RK, cost: Long, peek: Boolean): CIO[Decision] =
    executor.evalSha(this, subject, cost, peek)

  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    subscribe(Broker.Kind.Channel, channel +: rest.toVector)(delivery => Message(delivery.channel, decodePayload[V](delivery.payload)))

  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]] =
    subscribe(Broker.Kind.Pattern, pattern +: rest.toVector)(delivery =>
      PatternMessage(delivery.pattern.getOrElse(""), delivery.channel, decodePayload[V](delivery.payload))
    )

  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    subscribe(Broker.Kind.Shard, channel +: rest.toVector)(delivery => Message(delivery.channel, decodePayload[V](delivery.payload)))

  override private[sage] def runner: SharedRunner = this

  def close: CIO[Unit] = CIO.defer(closed.set(true))

  /**
    * Registers a subscription with the broker. The subscription is active once the effect completes, so a message published afterwards is
    * delivered, as with a real server once it confirms the subscription. Closing it unsubscribes and ends the stream.
    */
  private def subscribe[M](kind: Broker.Kind, names: Vector[String])(build: Broker.Delivery => M): CIO[Subscription[CIO, M]] =
    ifOpen(CIO.defer {
      val subscriber = executor.locked {
        val subscriber = new executor.store.pubsub.Subscriber(kind, names)
        executor.store.pubsub.add(subscriber)
        subscriber
      }
      new Subscription[CIO, M] {
        def next: CIO[Option[M]] = {
          // remove the callback when this call is interrupted, so that the next call can register its own
          val registered = new java.util.concurrent.atomic.AtomicReference[Option[Broker.Delivery] => Unit]()
          val deregister = CIO.defer {
            val callback = registered.getAndSet(null)
            if (callback != null) executor.locked(subscriber.cancel(callback))
          }
          CIO
            .ensure(deregister) {
              CIO.async[Option[Broker.Delivery]] { complete =>
                val callback: Option[Broker.Delivery] => Unit = delivery => {
                  registered.set(null)
                  complete(Success(delivery))
                }
                registered.set(callback)
                executor.locked(subscriber.next(callback))
              }
            }
            .flatMap(delivery => CIO.defer(delivery.map(build)))
        }

        def close: CIO[Unit] = CIO.defer(executor.locked(subscriber.close()))
      }
    })

  // a payload the codec cannot decode fails the stream, as it does with the networked client
  private def decodePayload[V](payload: sage.Bytes)(using codec: ValueCodec[V]): V =
    codec.decode(payload) match {
      case Right(value) => value
      case Left(error)  => throw error
    }

  private def ifOpen[A](body: CIO[A]): CIO[A] = CIO.defer(closed.get).flatMap(isClosed => if (isClosed) CIO.fail(NotConnected()) else body)

  /**
    * Runs a blocking command such as `BLPOP`. When it has nothing to return, it waits until another command writes and then tries again,
    * until the command's timeout passes and the reply is null. The timeout is measured with `CIO.timeout`, so it follows the backend's
    * clock in the same way as key expiry.
    */
  private def blocking(command: Command[?]): CIO[Frame] =
    executor.blockTimeout(command.name, command.args) match {
      case Left(error)    => CIO.value(error)
      case Right(timeout) =>
        currentMillis.flatMap { now =>
          val args = executor.prepareBlocking(command.name, command.args, now)
          timeout.fold(waitFor(command.name, args))(t => CIO.timeout(t)(waitFor(command.name, args)).map(_.getOrElse(Frame.Null)))
        }
    }

  private def waitFor(name: String, args: Vector[sage.Bytes]): CIO[Frame] =
    currentMillis
      .flatMap { now =>
        CIO.async[Option[Frame]] { complete =>
          executor.attemptBlocking(name, args, now, () => complete(Success(None))) match {
            case Some(frame) => complete(Success(Some(frame)))
            case None        => ()
          }
        }
      }
      .flatMap {
        case Some(frame) => CIO.value(frame)
        case None        => waitFor(name, args)
      }

  private def attempt(command: Command[?], frame: Frame): Attempt[Any] = Reply.decode(command, frame).toEither.left.map {
    case error: SageException => error
    case other                => SageException.DecodeError.fromThrowable(other)
  }

  /**
    * A transaction. Commands passed to `run` execute immediately. `watch` records the current version of each key, and `exec` aborts
    * when any of those versions has changed. `exec` runs its commands under one lock, so no other command can run between them.
    */
  final private class Scope extends TransactionScope[CIO, String] {

    private val watched            = mutable.HashMap.empty[Key, Long]
    @volatile private var released = false

    def release(): Unit = released = true

    def run[A](command: Command[A]): CIO[A] =
      if (command.isBlocking) CIO.fail(InvalidArgument("a Transaction cannot run blocking commands; run them individually on the client"))
      else active(InMemoryClient.this.run(command))

    def watch[K: KeyCodec](key: K, rest: K*): CIO[Unit] =
      active(CIO.defer(executor.locked {
        (key +: rest).foreach { k =>
          val encoded = new Key(summon[KeyCodec[K]].encode(k))
          if (!watched.contains(encoded)) watched.update(encoded, executor.store.version(encoded))
        }
      }))

    private[sage] def exec[R](p: Pipeline[R]): CIO[Option[R]] =
      if (p.commands.exists(_.isBlocking))
        CIO.fail(InvalidArgument("a Transaction cannot carry blocking commands; run them individually on the client"))
      else
        active(currentMillis.flatMap(now => CIO.defer(runExec(p, now)))).flatMap {
          case Left(error)          => CIO.fail(error)
          case Right(None)          => CIO.value(None)
          case Right(Some(results)) => p.finish(results).fold(CIO.fail(_), r => CIO.value(Some(r)))
        }

    def discard: CIO[Unit] = active(CIO.defer(executor.locked(watched.clear())))

    // A command the server would refuse to queue discards the whole transaction, with that command's error as the message. Like EXEC on
    // a server, this clears the watched keys whether the transaction commits or aborts.
    private def runExec(p: Pipeline[?], now: Long): Either[SageException, Option[Vector[Attempt[Any]]]] =
      p.commands.iterator.flatMap(c => executor.queueError(c.name, c.args)).nextOption() match {
        case Some(error) =>
          executor.locked(watched.clear())
          Left(TransactionDiscarded(describe(error)))
        case None        =>
          val unchanged = () => watched.forall((key, version) => executor.store.version(key) == version)
          val frames    = executor.executeAll(p.commands.map(c => (c.name, c.args)), now) {
            val ok = unchanged()
            watched.clear()
            ok
          }
          Right(frames.map(fs => p.commands.zip(fs).map(attempt(_, _))))
      }

    private def describe(frame: Frame): String =
      frame match {
        case Frame.SimpleError(message) => message
        case other                      => Frame.describe(other)
      }

    private def active[A](body: CIO[A]): CIO[A] =
      CIO.defer(released).flatMap { isReleased =>
        if (isReleased) CIO.fail(new IllegalStateException("transaction scope used after its block returned")) else body
      }
  }
}

private[sage] object InMemoryClient {

  /**
    * A client with an empty keyspace that reads time from the backend's own clock.
    */
  def apply(): InMemoryClient = apply(InMemoryServer())

  /**
    * A client with an empty keyspace that reads time from `clock`.
    */
  def apply(clock: TestClock): InMemoryClient = apply(InMemoryServer(clock))

  /**
    * A client of an existing keyspace, which behaves like another connection to the same server.
    */
  def apply(server: InMemoryServer): InMemoryClient =
    new InMemoryClient(server.executor, server.clock.fold(CIO.now.map(_.toEpochMilli))(clock => CIO.defer(clock.nowMillis)))
}

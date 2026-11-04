package sage.client.inmemory

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Message, PatternMessage, SageException}
import sage.SageException.{DecodeError, InvalidArgument, NotCacheable, NotConnected, TransactionDiscarded}
import sage.client.internal.{Client, LockExecutor, RateLimitExecutor, SharedRunner, Subscription, TransactionScope}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Attempt, Command, Pipeline, Reply}
import sage.protocol.Frame
import sage.ratelimit.Decision
import sage.testkit.InMemoryServer

/**
  * A [[Client]] that runs commands against an in-memory [[Store]] instead of a server. Every command is executed from its name and encoded
  * arguments, and its reply frame goes through the command's own decoder, so results and errors have the same types as with a real
  * server. Each backend wraps this client in its usual adapter, the same way it wraps the networked client.
  *
  * The client uses `server`'s keyspace, so it behaves like another connection to the same server. The server's `currentMillis` gives the
  * current Unix time in milliseconds and is read once per command, pipeline or `EXEC`, before the store lock is taken. By default it is
  * `CIO.now`, which each backend binds to its own clock, so test clocks such as ZIO's `TestClock` control expiry. Two commands that run
  * concurrently may therefore execute in a different order than their time readings, which is harmless because each command still sees a
  * consistent time.
  *
  * Commands that the store does not implement fail with a `ServerError` whose code is `ERR`.
  */
final private[sage] class InMemoryClient(server: InMemoryServer = InMemoryServer()) extends Client[CIO, String] with SharedRunner {
  import server.{currentMillis, executor}

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
      PatternMessage(delivery.matched, delivery.channel, decodePayload[V](delivery.payload))
    )

  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    subscribe(Broker.Kind.Shard, channel +: rest.toVector)(delivery => Message(delivery.channel, decodePayload[V](delivery.payload)))

  override private[sage] def runner: SharedRunner = this

  def close: CIO[Unit] = CIO.defer(if (!closed.getAndSet(true)) executor.locked(executor.store.pubsub.close(this)))

  /**
    * Registers a subscription with the broker. The subscription is active once the effect completes, so a message published afterwards is
    * delivered, as with a real server once it confirms the subscription. Closing it unsubscribes and ends the stream.
    */
  private def subscribe[M](kind: Broker.Kind, names: Vector[String])(build: Broker.Delivery => M): CIO[Subscription[CIO, M]] =
    // a server keeps each name of a client once; reading the flag under the lock keeps `close` from running between the check and the
    // registration
    CIO.defer(executor.locked(Option.unless(closed.get)(new executor.store.pubsub.Subscriber(this, kind, names.distinct)))).flatMap {
      case None             => CIO.fail(NotConnected())
      case Some(subscriber) =>
        CIO.value(new Subscription[CIO, M] {
          def next: CIO[Option[M]] =
            once[Option[Broker.Delivery]](wakeUp => executor.locked(subscriber.take(wakeUp)))
              .flatMap(_.fold(next)(delivery => CIO.defer(delivery.map(build))))

          def close: CIO[Unit] = CIO.defer(executor.locked(subscriber.close()))
        })
    }

  // a payload the codec cannot decode fails the stream, as it does with the networked client
  private def decodePayload[V](payload: sage.Bytes)(using codec: ValueCodec[V]): V =
    try codec.decode(payload).fold(throw _, identity)
    catch {
      case error: SageException => throw error
      case NonFatal(error)      => throw DecodeError.fromThrowable(error)
    }

  private def ifOpen[A](body: CIO[A]): CIO[A] = CIO.defer(closed.get).flatMap(isClosed => if (isClosed) CIO.fail(NotConnected()) else body)

  /**
    * Runs a blocking command such as `BLPOP`. When it has nothing to return, it waits until another command writes and then tries again,
    * until the command's timeout passes and the reply is null. The timeout is measured with `CIO.timeout` on the backend's timer, which a
    * `TestClock` does not control.
    */
  private def blocking(command: Command[?]): CIO[Frame] =
    currentMillis.flatMap { now =>
      executor.startBlocking(command.name, command.args, now) match {
        case Left(error) => CIO.value(error)
        case Right(wait) => wait.timeout.fold(waitFor(command, wait))(t => CIO.timeout(t)(waitFor(command, wait)).map(_.getOrElse(Frame.Null)))
      }
    }

  private def waitFor(command: Command[?], wait: Wait): CIO[Frame] =
    currentMillis
      .flatMap(now => once(executor.attemptBlocking(command.name, command.args, wait, now, _)))
      .flatMap(_.fold(waitFor(command, wait))(frame => CIO.value(frame)))

  // runs `attempt` once; when it has nothing to return, it keeps the wake-up it receives, which completes this with None
  private def once[A](attempt: (() => Unit) => Option[A]): CIO[Option[A]] =
    CIO.async[Option[A]](complete => attempt(() => complete(Success(None))).foreach(value => complete(Success(Some(value)))))

  private def attempt(command: Command[?], frame: Frame): Attempt[Any] = Reply.decode(command, frame).toEither.left.map {
    case error: SageException => error
    case other                => DecodeError.fromThrowable(other)
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
      active(
        currentMillis.flatMap(now =>
          CIO.defer(executor.locked {
            (key +: rest).foreach { k =>
              val encoded = new Key(summon[KeyCodec[K]].encode(k))
              if (!watched.contains(encoded)) watched.update(encoded, executor.store.version(encoded, now))
            }
          })
        )
      )

    // Like EXEC on a server, this clears the watched keys whether the transaction commits, aborts or is discarded.
    private[sage] def exec[R](p: Pipeline[R]): CIO[Option[R]] =
      if (p.commands.exists(_.isBlocking))
        CIO.fail(InvalidArgument("a Transaction cannot carry blocking commands; run them individually on the client"))
      else
        active(currentMillis.flatMap { now =>
          CIO.defer(executor.executeAll(p.commands.map(c => (c.name, c.args)), now) {
            val ok = watched.forall((key, version) => executor.store.version(key, now) == version)
            watched.clear()
            ok
          })
        }).flatMap {
          case Left(error)         => CIO.fail(TransactionDiscarded(error))
          case Right(None)         => CIO.value(None)
          case Right(Some(frames)) => p.finish(p.commands.zip(frames).map(attempt(_, _))).fold(CIO.fail(_), r => CIO.value(Some(r)))
        }

    def discard: CIO[Unit] = active(CIO.defer(executor.locked(watched.clear())))

    private def active[A](body: CIO[A]): CIO[A] =
      CIO.defer(released).flatMap { isReleased =>
        if (isReleased) CIO.fail(new IllegalStateException("transaction scope used after its block returned")) else body
      }
  }
}

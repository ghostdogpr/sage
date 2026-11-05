package sage.client.inmemory

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.control.NonFatal

import kyo.compat.*

import sage.{Bytes, Message, PatternMessage, SageException}
import sage.SageException.{DecodeError, InvalidArgument, NotCacheable, NotConnected, ServerError, TimedOut, TransactionDiscarded}
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

  private[sage] def lockTryWith[LK, A](lock: LockExecutor[LK], key: LK)(body: => CIO[A]): CIO[Option[A]] = tryWithLock(lock, key, this)(body)

  private[sage] def lockWith[LK, A](lock: LockExecutor[LK], key: LK, waitTimeout: FiniteDuration)(body: => CIO[A]): CIO[A] =
    withLock(lock, key, waitTimeout, this)(body)

  override private[client] def tryWithLock[K, A](lock: LockExecutor[K], key: K, runner: SharedRunner)(body: CIO[A]): CIO[Option[A]] =
    lock.validate(None).flatMap { _ =>
      holding(lock.key(key))(take =>
        currentMillis.flatMap(now => CIO.defer(executor.locked(take(now)))).flatMap { taken =>
          if (taken) body.map(Some(_)) else CIO.value(None)
        }
      )
    }

  override private[client] def withLock[K, A](lock: LockExecutor[K], key: K, waitTimeout: FiniteDuration, runner: SharedRunner)(
    body: CIO[A]
  ): CIO[A] =
    lock.validate(Some(waitTimeout)).flatMap { _ =>
      holding(lock.key(key)) { take =>
        waitFor(Some(waitTimeout))(now => Option.when(take(now))(())).flatMap {
          case Some(_) => body
          case None    => CIO.fail(TimedOut("distributed lock acquisition timed out"))
        }
      }
    }

  private[sage] def rateLimitAcquire[RK](limiter: RateLimitExecutor[RK], subject: RK, cost: Long, peek: Boolean): CIO[Decision] =
    limiter.definition.validate(cost) match {
      case Some(problem) => CIO.fail(InvalidArgument(problem))
      case None          =>
        ifOpen(currentMillis.flatMap { now =>
          CIO.defer {
            try executor.locked(TokenBucket.acquire(executor.store, limiter.definition, subject, cost, peek, now))
            catch { case reject: Reject => throw ServerError.of(reject.frame.value) }
          }
        })
    }

  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    subscribe(Broker.Kind.Channel, channel +: rest.toVector)(delivery => Message(delivery.channel, decodePayload[V](delivery.payload)))

  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]] =
    subscribe(Broker.Kind.Pattern, pattern +: rest.toVector)(delivery =>
      PatternMessage(delivery.matched, delivery.channel, decodePayload[V](delivery.payload))
    )

  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]] =
    CIO.fail(ServerError.of(Errors.unknown("SSUBSCRIBE").value))

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
  private def decodePayload[V](payload: Bytes)(using codec: ValueCodec[V]): V =
    try codec.decode(payload).fold(throw _, identity)
    catch {
      case error: SageException => throw error
      case NonFatal(error)      => throw DecodeError.fromThrowable(error)
    }

  /**
    * Runs `use` with a function that tries to take the lock at `key` at a given time, which must run under the store lock. A lock is held
    * while its key stores this call's token, so clients of the same server exclude each other. It has no lease and stays held until `use`
    * ends, fails or is interrupted.
    */
  private def holding[A](key: Bytes)(use: (Long => Boolean) => CIO[A]): CIO[A] = {
    val held  = new Key(key)
    val token = Bytes.utf8(UUID.randomUUID().toString)

    def take(now: Long): Boolean =
      executor.store.lookup(held, now).isEmpty && {
        executor.store.put(held, Value.Str(token), Ttl.Clear, now)
        true
      }

    // releasing checks the token, so it also runs when `use` was interrupted before it learned that it had taken the lock
    val release: CIO[Unit] = currentMillis.flatMap { now =>
      CIO.defer(executor.locked {
        executor.store.lookup(held, now).map(_.value) match {
          case Some(Value.Str(value)) if value.sameBytes(token) => executor.store.delete(held, now): Unit
          case _                                                => ()
        }
      })
    }
    ifOpen(CIO.acquireReleaseWith(CIO.unit)(_ => release)(_ => use(take)))
  }

  private def ifOpen[A](body: CIO[A]): CIO[A] = CIO.defer(closed.get).flatMap(isClosed => if (isClosed) CIO.fail(NotConnected()) else body)

  /**
    * Runs a blocking command such as `BLPOP`. When it has nothing to return, it waits until another command writes and then tries again,
    * until the command's timeout passes and the reply is null.
    */
  private def blocking(command: Command[?]): CIO[Frame] =
    CIO.defer(executor.startBlocking(command.name, command.args)).flatMap {
      case Left(error)    => CIO.value(error)
      case Right(timeout) => waitFor(timeout)(executor.attemptBlocking(command.name, command.args)).map(_.getOrElse(Frame.Null))
    }

  /**
    * Runs `attempt` under the store lock, and again after every write, until it returns a value, or until `timeout` passes and this returns
    * `None`. The timeout is measured with `CIO.timeout` on the backend's timer, which a `TestClock` does not control. The waiter is
    * abandoned under the store lock once the timeout passes, so an attempt and the timeout never both succeed.
    */
  private def waitFor[A](timeout: Option[FiniteDuration])(attempt: Long => Option[A]): CIO[Option[A]] = {
    val waiter       = new executor.Waiter(attempt)
    def poll: CIO[A] = currentMillis.flatMap(now => once(waiter.poll(now, _))).flatMap(_.fold(poll)(CIO.value(_)))
    timeout.fold(poll.map(Some(_)))(t => CIO.timeout(t)(poll).flatMap(_.fold(CIO.defer(waiter.abandon()))(value => CIO.value(Some(value)))))
  }

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

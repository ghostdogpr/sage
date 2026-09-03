package sage.client.internal

import scala.concurrent.duration.FiniteDuration

import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Pipeline}
import sage.ratelimit.Decision

/**
  * Adapts the shared [[Client]] from `CIO` to a backend effect `F`. A backend provides [[lower]] and [[lift]], and this class applies them to
  * the shared client operations. Streaming helpers remain in each backend because their return types differ.
  */
abstract class LoweredClient[F[_]](underlying: Client[CIO, String]) extends Client[F, String] {

  protected def lower[A](c: CIO[A]): F[A]

  protected def lift[A](fa: F[A]): CIO[A]

  // Native adapters encode failure and cancellation for the shared race, then restore the original outcome after cleanup.
  // Explicit deferral is required because mapping CIO.unit can evaluate immediately in Kyo.
  protected def lockScope[A, B](body: () => F[A])(runScope: CIO[A] => CIO[B]): F[B] =
    lower(runScope(CIO.defer(()).flatMap(_ => lift(body()))))

  protected def lockCommand[A](command: Command[A]): CIO[A] = underlying.run(command)

  protected def confirmedLockCommand(command: Command[Boolean], timeout: FiniteDuration): CIO[Boolean] =
    underlying.lockWrite(command, timeout)

  private val lockRunner: CommandRunner[CIO, String] = new CommandRunner[CIO, String] {
    def run[A](command: Command[A]): CIO[A]                                                                = lockCommand(command)
    override private[sage] def lockWrite(command: Command[Boolean], timeout: FiniteDuration): CIO[Boolean] =
      confirmedLockCommand(command, timeout)
  }

  final def run[A](command: Command[A]): F[A] = lower(underlying.run(command))

  final def cached[A](command: Command[A], ttl: FiniteDuration): F[A] = lower(underlying.cached(command, ttl))

  final private[sage] def pipeline[Out, R](p: Pipeline[Out, R]): F[Out] = lower(underlying.pipeline(p))

  final private[sage] def pipelineAttempt[Out, R](p: Pipeline[Out, R]): F[R] = lower(underlying.pipelineAttempt(p))

  final def transaction[A](body: TransactionScope[F, String] => F[A]): F[A] =
    lower(underlying.transaction[A](scope => lift(body(lowerScope(scope)))))

  final def subscribeChannels[V: ValueCodec](channel: String, rest: String*): F[Subscription[F, Message[V]]] =
    lower(underlying.subscribeChannels[V](channel, rest*).map(lowerSub))

  final def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): F[Subscription[F, PatternMessage[V]]] =
    lower(underlying.subscribePatterns[V](pattern, rest*).map(lowerSub))

  final def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): F[Subscription[F, Message[V]]] =
    lower(underlying.subscribeShardChannels[V](channel, rest*).map(lowerSub))

  final private[sage] def scanTargets: F[Vector[ScanTarget]] = lower(underlying.scanTargets)

  final private[sage] def runOn[A](target: ScanTarget, command: Command[A]): F[A] = lower(underlying.runOn(target, command))

  final private[sage] def rateLimitAcquire[RK](executor: RateLimitExecutor[RK], subject: RK, cost: Long, peek: Boolean): F[Decision] =
    lower(underlying.rateLimitAcquire(executor, subject, cost, peek))

  final private[sage] def lockTryWith[LK, A](executor: LockExecutor[LK], key: LK)(body: => F[A]): F[Option[A]] = {
    val bodyThunk = () => body
    lockScope(bodyThunk)(executor.tryWithLock(lockRunner, key)(_))
  }

  final private[sage] def lockWith[LK, A](executor: LockExecutor[LK], key: LK, waitTimeout: FiniteDuration)(body: => F[A]): F[A] = {
    val bodyThunk = () => body
    lockScope(bodyThunk)(executor.withLock(lockRunner, key, waitTimeout)(_))
  }

  final def close: F[Unit] = lower(underlying.close)

  private def lowerScope(scope: TransactionScope[CIO, String]): TransactionScope[F, String] =
    new TransactionScope[F, String] {
      def watch[K: KeyCodec](key: K, rest: K*): F[Unit]                        = lower(scope.watch(key, rest*))
      def run[A](command: Command[A]): F[A]                                    = lower(scope.run(command))
      private[sage] def exec[Out, R](p: Pipeline[Out, R]): F[Option[Out]]      = lower(scope.exec(p))
      private[sage] def execAttempt[Out, R](p: Pipeline[Out, R]): F[Option[R]] = lower(scope.execAttempt(p))
      def discard: F[Unit]                                                     = lower(scope.discard)
    }

  private def lowerSub[A](sub: Subscription[CIO, A]): Subscription[F, A] =
    new Subscription[F, A] {
      def next: F[Option[A]] = lower(sub.next)
      def close: F[Unit]     = lower(sub.close)
    }
}

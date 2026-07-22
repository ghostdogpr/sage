package sage.client.internal

import scala.concurrent.duration.FiniteDuration

import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.codec.{KeyCodec, ValueCodec}
import sage.commands.{Command, Pipeline}
import sage.ratelimit.Decision

/**
  * Lowers the runtime's [[Client]] from the kyo-compat carrier `CIO` to a Backend's native effect `F`, written once for every Backend. A
  * Backend supplies only the two natural transformations [[lower]] (`CIO ~> F`) and [[lift]] (`F ~> CIO`); the whole client surface is
  * expressed in terms of them. The native streaming sugar (`scanAll`, `subscribe`, …) stays per Backend, since it returns that ecosystem's
  * own stream type.
  */
abstract class LoweredClient[F[_]](underlying: Client[CIO, String]) extends Client[F, String] {

  protected def lower[A](c: CIO[A]): F[A]

  protected def lift[A](fa: F[A]): CIO[A]

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

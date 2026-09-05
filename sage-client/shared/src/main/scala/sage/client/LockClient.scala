package sage.client

import scala.concurrent.duration.FiniteDuration

import sage.client.internal.{Client, LockExecutor}

/**
  * `LockClient` provides a distributed mutex bound to a client. Each key has an expiring lease that Sage renews while the protected effect
  * runs. All callers coordinating the same work must use the same namespace and key. Locks are not reentrant and do not guarantee
  * acquisition order. Master-replica and cluster clients wait for replica acknowledgement by default.
  *
  * Mutual exclusion depends on the lease remaining valid and the server retaining the lock state. Process pauses beyond expiry and server
  * failover can allow overlapping work. Cancellation after lease loss is cooperative and cannot undo completed external effects.
  */
final class LockClient[F[_], K] private[sage] (client: Client[F, ?], executor: LockExecutor[K]) {

  /**
    * Runs `body` while holding the lock and returns its result in `Some` after releasing the lock. If the lock is busy, returns `None`
    * without evaluating `body`. The lease is renewed while `body` runs. A body failure is propagated after cleanup, and an ownership or
    * renewal failure raises [[sage.SageException.LockLost]]. Temporary acquisition failures are retried for up to about one second, bounded by
    * the usable lease, before raising [[sage.SageException.TimedOut]].
    */
  def tryWithLock[A](key: K)(body: => F[A]): F[Option[A]] = client.lockTryWith(executor, key)(body)

  /**
    * Waits up to `waitTimeout` to acquire the lock, runs `body` while holding it, and returns the body's result after releasing the lock. If
    * the lock cannot be acquired in time, raises [[sage.SageException.TimedOut]] without evaluating `body`. The lease is renewed while `body`
    * runs. Sage never retries the body. A body failure is propagated after cleanup, and an ownership or renewal failure raises
    * [[sage.SageException.LockLost]]. The timeout must be positive and covers acquisition only.
    */
  def withLock[A](key: K, waitTimeout: FiniteDuration)(body: => F[A]): F[A] =
    client.lockWith(executor, key, waitTimeout)(body)
}

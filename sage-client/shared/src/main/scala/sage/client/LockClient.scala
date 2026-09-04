package sage.client

import scala.concurrent.duration.FiniteDuration

import sage.client.internal.{Client, LockExecutor}

/**
  * `LockClient` provides a distributed mutex bound to a client. Each key has an expiring lease that Sage renews while the protected effect
  * runs. All callers coordinating the same work must use the same namespace and key. Locks are not reentrant and do not guarantee
  * acquisition order. Master-replica and cluster clients wait for replica acknowledgement by default.
  *
  * Mutual exclusion depends on the lease remaining valid and Redis retaining its state. Process pauses beyond expiry and Redis failover
  * can allow overlapping work. Cancellation after lease loss is cooperative and cannot undo completed external effects.
  */
final class LockClient[F[_], K] private[sage] (client: Client[F, ?], executor: LockExecutor[K]) {

  /**
    * Checks contention once and returns `None` when busy, without evaluating `body`. Temporary failures may be retried for about one second,
    * after which acquisition raises [[sage.SageException.TimedOut]]. After acquisition, renews the lease and releases it when `body`
    * finishes. Returns `Some(result)` on success. Ownership or renewal failure raises [[sage.SageException.LockLost]].
    */
  def tryWithLock[A](key: K)(body: => F[A]): F[Option[A]] = client.lockTryWith(executor, key)(body)

  /**
    * Retries acquisition with exponential backoff and jitter until `waitTimeout` elapses, then raises [[sage.SageException.TimedOut]]. The timeout
    * must be positive and covers acquisition only. Once acquired, the lease is renewed for the duration of `body`. The body is evaluated
    * only after acquisition and is never retried by Sage.
    */
  def withLock[A](key: K, waitTimeout: FiniteDuration)(body: => F[A]): F[A] =
    client.lockWith(executor, key, waitTimeout)(body)
}

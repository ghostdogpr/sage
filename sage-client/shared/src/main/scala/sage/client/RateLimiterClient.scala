package sage.client

import sage.client.internal.{Client, RateLimitExecutor}
import sage.commands.Command
import sage.ratelimit.Decision

/**
  * A rate limiter bound to a client, returned by `client.rateLimiter(...)`. Checks come back in the backend's own effect `F`.
  */
final class RateLimiterClient[F[_], K] private[sage] (client: Client[F, ?], executor: RateLimitExecutor[K]) {

  /**
    * Consume `cost` tokens for `subject` if available, returning a [[Decision]]: `Allowed` with the tokens left, otherwise `Denied`.
    */
  def tryAcquire(subject: K, cost: Long = 1): F[Decision] = client.rateLimitAcquire(executor, subject, cost, peek = false)

  /**
    * Report the current standing for `subject` without consuming: `Allowed` while at least one token is available, otherwise `Denied`
    * with the wait until one is.
    */
  def peek(subject: K): F[Decision] = client.rateLimitAcquire(executor, subject, cost = 1, peek = true)

  /**
    * Clear `subject`'s bucket, so its next request starts from full capacity.
    */
  def reset(subject: K): F[Unit] = client.run(executor.resetCommand(subject))

  /**
    * An equivalent plain-`EVAL` [[sage.commands.Command]] for pipelining or running yourself. Unlike native [[tryAcquire]], it does not use
    * the cached-script `EVALSHA` fast path.
    */
  def command(subject: K, cost: Long = 1): Command[Decision] = executor.command(subject, cost)
}

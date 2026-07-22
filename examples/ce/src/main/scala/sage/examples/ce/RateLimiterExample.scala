package sage.examples.ce

import cats.effect.IO

import sage.*
import sage.backend.*

/**
  * A distributed token-bucket rate limiter: `rateLimiter` binds a policy, each `tryAcquire` is one atomic allow/deny check on server-side state.
  */
object RateLimiterExample {

  def run(client: SageClient): IO[Unit] = {
    val limiter = client.rateLimiter[String](RateLimit.perMinute(2))
    for {
      first  <- limiter.tryAcquire("user:42")
      second <- limiter.tryAcquire("user:42")
      third  <- limiter.tryAcquire("user:42")
      _      <- IO.println(s"1st allowed=${first.isAllowed} remaining=${first.remainingTokens}")
      _      <- IO.println(s"2nd allowed=${second.isAllowed} remaining=${second.remainingTokens}")
      _      <- IO.println(s"3rd allowed=${third.isAllowed}")
    } yield ()
  }
}

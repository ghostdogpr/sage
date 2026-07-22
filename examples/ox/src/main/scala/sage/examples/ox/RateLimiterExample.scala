package sage.examples.ox

import ox.Ox

import sage.*
import sage.backend.*

/**
  * A distributed token-bucket rate limiter: `rateLimiter` binds a policy, each `tryAcquire` is one atomic allow/deny check on server-side state.
  */
object RateLimiterExample {

  def run(client: SageClient)(using Ox): Unit = {
    val limiter = client.rateLimiter[String](RateLimit.perMinute(2))
    val first   = limiter.tryAcquire("user:42")
    val second  = limiter.tryAcquire("user:42")
    val third   = limiter.tryAcquire("user:42")
    println(s"1st allowed=${first.isAllowed} remaining=${first.remainingTokens}")
    println(s"2nd allowed=${second.isAllowed} remaining=${second.remainingTokens}")
    println(s"3rd allowed=${third.isAllowed}")
  }
}

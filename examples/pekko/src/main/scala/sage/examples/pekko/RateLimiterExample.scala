package sage.examples.pekko

import scala.concurrent.{ExecutionContext, Future}

import sage.*
import sage.backend.*

/**
  * A distributed token-bucket rate limiter: `rateLimiter` binds a policy, each `tryAcquire` is one atomic allow/deny check on server-side state.
  */
object RateLimiterExample {

  def run(client: SageClient)(using ExecutionContext): Future[Unit] = {
    val limiter = client.rateLimiter[String](RateLimit.perMinute(2))
    for {
      first  <- limiter.tryAcquire("user:42")
      second <- limiter.tryAcquire("user:42")
      third  <- limiter.tryAcquire("user:42")
    } yield {
      println(s"1st allowed=${first.isAllowed} remaining=${first.remainingTokens}")
      println(s"2nd allowed=${second.isAllowed} remaining=${second.remainingTokens}")
      println(s"3rd allowed=${third.isAllowed}")
    }
  }
}

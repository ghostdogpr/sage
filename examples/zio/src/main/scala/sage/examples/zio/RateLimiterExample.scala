package sage.examples.zio

import zio.*

import sage.*
import sage.backend.*

/**
  * A distributed token-bucket rate limiter: `rateLimiter` binds a policy, each `tryAcquire` is one atomic allow/deny check on server-side state.
  */
object RateLimiterExample {

  val run: ZIO[SageClient, Throwable, Unit] =
    ZIO.serviceWithZIO[SageClient] { client =>
      val limiter = client.rateLimiter[String](RateLimit.perMinute(2))
      for {
        first  <- limiter.tryAcquire("user:42")
        second <- limiter.tryAcquire("user:42")
        third  <- limiter.tryAcquire("user:42")
        _      <- Console.printLine(s"1st allowed=${first.isAllowed} remaining=${first.remainingTokens}")
        _      <- Console.printLine(s"2nd allowed=${second.isAllowed} remaining=${second.remainingTokens}")
        _      <- Console.printLine(s"3rd allowed=${third.isAllowed}")
      } yield ()
    }
}

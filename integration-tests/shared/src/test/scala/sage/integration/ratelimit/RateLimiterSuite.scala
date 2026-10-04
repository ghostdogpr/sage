package sage.integration.ratelimit

import scala.concurrent.duration.*

import kyo.compat.*

import sage.SageException.ServerError
import sage.integration.BothServersSuite
import sage.integration.Ttls.expiresWithin
import sage.ratelimit.{Decision, RateLimit, RateLimiter}
import sage.ratelimit.Decision.{Allowed, Denied}

class RateLimiterSuite extends BothServersSuite {

  private def limit(capacity: Long) = RateLimit(capacity, refillTokens = 1, refillPeriod = 1.hour)

  private def bucketKey(subject: String) = s"9:ratelimit:$subject"

  private def retryAfter(decision: Decision): FiniteDuration = decision match {
    case Decision.Denied(_, wait) => wait
    case other                    => fail(s"expected Denied, got $other")
  }

  // Waits depend on the server clock between calls, so the wall-clock tests compare admission and remaining tokens only.
  private def untimed(decision: Decision): Decision = decision match {
    case Allowed(remaining, _) => Allowed(remaining, Duration.Zero)
    case Denied(remaining, _)  => Denied(remaining, Duration.Zero)
  }

  clientTest("admits up to capacity, then denies (via client.rateLimiter)") { client =>
    val rl = client.rateLimiter[String](limit(3))
    for {
      _  <- rl.tryAcquire("admit").map(untimed).is(Allowed(2, Duration.Zero))
      _  <- rl.tryAcquire("admit").map(untimed).is(Allowed(1, Duration.Zero))
      _  <- rl.tryAcquire("admit").map(untimed).is(Allowed(0, Duration.Zero))
      d4 <- rl.tryAcquire("admit")
    } yield {
      assertEquals(untimed(d4), Denied(0, Duration.Zero))
      assert(retryAfter(d4) > Duration.Zero, "4th denied")
    }
  }

  clientTest("cost consumes multiple tokens") { client =>
    val rl = client.rateLimiter[String](limit(5))
    rl.tryAcquire("cost", cost = 3).map(untimed).is(Allowed(2, Duration.Zero)) >>
      rl.tryAcquire("cost", cost = 3).map(untimed).is(Denied(2, Duration.Zero))
  }

  clientTest("peek reports without consuming") { client =>
    val rl = client.rateLimiter[String](limit(3))
    rl.tryAcquire("peek") >>
      rl.peek("peek").map(untimed).is(Allowed(2, Duration.Zero)) >>
      rl.tryAcquire("peek").map(untimed).is(Allowed(1, Duration.Zero))
  }

  clientTest("peek on an empty bucket is denied, still consuming nothing") { client =>
    val rl = client.rateLimiter[String](limit(1))
    for {
      _      <- rl.tryAcquire("peek-empty")
      peeked <- rl.peek("peek-empty")
    } yield {
      assertEquals(peeked.remainingTokens, 0L)
      assert(retryAfter(peeked) > Duration.Zero, "reports the wait until one token is available")
    }
  }

  clientTest("reset refills the bucket") { client =>
    val rl = client.rateLimiter[String](limit(1))
    rl.tryAcquire("reset").map(untimed).is(Allowed(0, Duration.Zero)) >>
      rl.tryAcquire("reset").map(untimed).is(Denied(0, Duration.Zero)) >>
      rl.reset("reset") >>
      rl.tryAcquire("reset").map(untimed).is(Allowed(0, Duration.Zero))
  }

  clientTest("the computed sha matches the server's, and EVALSHA runs once loaded") { client =>
    val rl = RateLimiter[String](limit(2))
    client.scriptLoad(RateLimiter.script).is(RateLimiter.compiled.sha) >>
      client.run(rl.eval(cached = true, "es", 1, peek = false)).map(untimed).is(Allowed(1, Duration.Zero)) >>
      client.run(rl.eval(cached = true, "es", 1, peek = false)).map(untimed).is(Allowed(0, Duration.Zero))
  }

  clientTest("the native EVALSHA path recovers from NOSCRIPT by re-running with the script body") { client =>
    val rl = client.rateLimiter[String](limit(2), namespace = "noscript")
    client.scriptFlush() >>
      rl.tryAcquire("cold").map(untimed).is(Allowed(1, Duration.Zero)) >>
      rl.tryAcquire("cold").map(untimed).is(Allowed(0, Duration.Zero))
  }

  clientTest("the composable EVAL guard and validate agree on every policy and cost rule") { client =>
    val cases: List[(RateLimit, Long)]                                   = List(
      RateLimit(3, 1, 1.hour)                     -> 1L,
      RateLimit(3, 1, 1.hour)                     -> 0L,
      RateLimit(3, 1, 1.hour)                     -> 3L,
      RateLimit(0, 1, 1.hour)                     -> 1L,
      RateLimit(3, 0, 1.hour)                     -> 1L,
      RateLimit(3, 1, 500.nanos)                  -> 1L,
      RateLimit(9007199254740993L, 1, 1.hour)     -> 1L, // 2^53 + 1
      RateLimit(3, 9007199254740993L, 1.hour)     -> 1L,
      RateLimit(3, 1, 9007199254740993L.micros)   -> 1L,
      RateLimit(1_000_000_000L, 1, 10000.seconds) -> 1L,
      RateLimit(3, 1, 3002399751580331L.micros)   -> 1L, // 3x = 2^53 + 1
      RateLimit(3, 1, 1.hour)                     -> 4L,
      RateLimit(3, 1, 1.hour)                     -> -1L,
      RateLimit(3, 1, 1.hour)                     -> 9007199254740993L
    )
    def serverRejects(rl: RateLimiter[String], cost: Long): CIO[Boolean] =
      client.run(rl.tryAcquire("k", cost)).map(_ => false).recover {
        case ServerError("SAGE", _) => CIO.value(true)
        case other                  => CIO.fail(other)
      }
    CIO.foreachDiscard(cases.zipWithIndex) { case ((limit, cost), i) =>
      val rl = RateLimiter[String](limit, namespace = s"conform$i")
      serverRejects(rl, cost).map(rejected => assertEquals(rejected, rl.validate(cost).isDefined, s"case $i: $limit cost=$cost"))
    }
  }

  clientTest("RateLimiterClient.command exposes the same composable check") { client =>
    val rl = client.rateLimiter[String](limit(1))
    client.run(rl.command("cmd-client")).map(untimed).is(Allowed(0, Duration.Zero)) >>
      client.run(rl.command("cmd-client")).map(untimed).is(Denied(0, Duration.Zero))
  }

  clientTest("accrues at the exact rate even at a large capacity") { client =>
    val rl = RateLimiter[String](RateLimit(capacity = 1_000_000_000L, refillTokens = 1000, refillPeriod = 1.second))
    val t0 = 7_000_000_000_000L
    client.run(rl.tryAcquireAt("big", 1_000_000_000L, t0)) >>
      client.run(rl.tryAcquireAt("big", 500, t0 + 500_000L)).is(Allowed(0, 1_000_000.seconds)) >>
      client.run(rl.tryAcquireAt("big", 500, t0 + 1_000_000L)).is(Allowed(0, 1_000_000.seconds)) >>
      client.run(rl.tryAcquireAt("big", 1, t0 + 1_000_000L)).is(Denied(0, 1.millis))
  }

  clientTest("refills continuously as injected time advances") { client =>
    val rl = RateLimiter[String](RateLimit(capacity = 10, refillTokens = 10, refillPeriod = 1.second))
    val t0 = 2_000_000_000_000L
    client.run(rl.tryAcquireAt("refill", 10, t0)).is(Allowed(0, 1.second)) >>
      client.run(rl.tryAcquireAt("refill", 1, t0)).is(Denied(0, 100.millis)) >>
      client.run(rl.tryAcquireAt("refill", 3, t0 + 300_000L)).is(Allowed(0, 1.second)) >>
      client.run(rl.tryAcquireAt("refill", 1, t0 + 300_000L)).is(Denied(0, 100.millis))
  }

  clientTest("a backward server clock does not double-credit refill") { client =>
    val rl = RateLimiter[String](RateLimit(capacity = 10, refillTokens = 10, refillPeriod = 1.second))
    val t0 = 5_000_000_000_000L
    client.run(rl.tryAcquireAt("clock", 10, t0)) >>
      client.run(rl.tryAcquireAt("clock", 1, t0 - 500_000L)).is(Denied(0, 600.millis)) >>
      client.run(rl.tryAcquireAt("clock", 1, t0 + 100_000L)).is(Allowed(0, 1.second)) >>
      client.run(rl.tryAcquireAt("clock", 1, t0 + 100_000L)).is(Denied(0, 100.millis))
  }

  clientTest("a non-integer refill rate credits whole tokens exactly and never re-counts elapsed time") { client =>
    val rl = RateLimiter[String](RateLimit(capacity = 10, refillTokens = 3, refillPeriod = 1.second))
    val t0 = 3_000_000_000_000L
    client.run(rl.tryAcquireAt("frac", 10, t0)) >>
      client.run(rl.tryAcquireAt("frac", 1, t0 + 333_333L)).is(Denied(0, 1.micro)) >>
      client.run(rl.tryAcquireAt("frac", 1, t0 + 333_333L)).is(Denied(0, 1.micro)) >>
      client.run(rl.tryAcquireAt("frac", 1, t0 + 333_334L)).is(Allowed(0, 3333333.micros))
  }

  clientTest("a clock rollback folds the catch-up interval into retryAfter and the key's TTL") { client =>
    val rl = RateLimiter[String](RateLimit(capacity = 1, refillTokens = 1, refillPeriod = 1.second))
    val t0 = 8_000_000_000_000L
    client.run(rl.tryAcquireAt("roll", 1, t0)) >>
      client.run(rl.tryAcquireAt("roll", 1, t0 - 4_000_000L)).map(retryAfter).is(5.seconds) >>
      client.pTtl(bucketKey("roll")).satisfies(expiresWithin(_, 5.seconds, above = 2.seconds))
  }

  clientTest("a large refill period and odd rollback retain the exact reported wait past 2^53") { client =>
    val period = 1L << 53
    val rl     = RateLimiter[String](RateLimit(capacity = 1, refillTokens = 1, refillPeriod = period.micros))
    val t0     = 9_000_000_000_000L
    client.run(rl.tryAcquireAt("big-period", 1, t0)) >>
      client.run(rl.tryAcquireAt("big-period", 1, t0 - 9L)).map(retryAfter).is((period + 9L).micros)
  }

  clientTest("reusing a namespace with alternating policies never mints a fresh bucket") { client =>
    val original  = RateLimiter[String](RateLimit(capacity = 10, refillTokens = 10, refillPeriod = 1.second), namespace = "policy")
    val tightened = RateLimiter[String](RateLimit(capacity = 1, refillTokens = 1, refillPeriod = 1.second), namespace = "policy")
    val t0        = 6_000_000_000_000L
    client.run(original.tryAcquireAt("same-subject", 1, t0)).is(Allowed(9, 100.millis)) >>
      client.run(tightened.tryAcquireAt("same-subject", 1, t0)).is(Allowed(0, 1.second)) >>
      client.run(original.tryAcquireAt("same-subject", 1, t0)).is(Denied(0, 100.millis)) >>
      client.run(tightened.tryAcquireAt("same-subject", 1, t0)).is(Denied(0, 1.second)) >>
      client.run(original.tryAcquireAt("same-subject", 1, t0 - 500_000L)).is(Denied(0, 600.millis)) >>
      client.run(original.tryAcquireAt("same-subject", 1, t0)).is(Denied(0, 100.millis))
  }

  clientTest("malformed bucket fields are rejected instead of granting tokens") { client =>
    val rl                                   = RateLimiter[String](limit(1))
    def rejected(subject: String): CIO[Unit] =
      failsWith[ServerError](client.run(rl.tryAcquireAt(subject, 1, 4_000_000_000_000L)))
        .map(assertEquals(_, ServerError("SAGE", "invalid rate-limit state"), subject))
    client.run(rl.tryAcquireAt("missing", 1, 4_000_000_000_000L)) >>
      client.hDel(bucketKey("missing"), "ts") >>
      rejected("missing") >>
      client.run(rl.tryAcquireAt("out-of-range", 1, 4_000_000_000_000L)) >>
      client.hSet(bucketKey("out-of-range"), ("t", "2")) >>
      rejected("out-of-range") >>
      client.run(rl.tryAcquireAt("full-with-fraction", 1, 4_000_000_000_000L)) >>
      client.hSet(bucketKey("full-with-fraction"), ("t", "1"), ("f", "1")) >>
      rejected("full-with-fraction")
  }

  clientTest("an already-full bucket reports zero time to full") { client =>
    val rl = RateLimiter[String](RateLimit(capacity = 5, refillTokens = 5, refillPeriod = 1.second))
    client.run(rl.peek("full")).is(Decision.Allowed(5L, Duration.Zero))
  }
}

package sage.integration.ratelimit

import scala.concurrent.duration.*

import kyo.compat.*

import sage.SageException.ServerError
import sage.commands.Ttl
import sage.integration.{Images, ServerSuite}
import sage.ratelimit.{Decision, RateLimit, RateLimiter}

abstract class RateLimiterSuite(image: String) extends ServerSuite(image) {

  private def limit(capacity: Long) = RateLimit(capacity, refillTokens = 1, refillPeriod = 1.hour)

  private def bucketKey(subject: String) = s"9:ratelimit:$subject"

  test("admits up to capacity, then denies (via client.rateLimiter)") {
    withClient { client =>
      val rl = client.rateLimiter[String](limit(3))
      for {
        d1 <- rl.tryAcquire("admit")
        d2 <- rl.tryAcquire("admit")
        d3 <- rl.tryAcquire("admit")
        d4 <- rl.tryAcquire("admit")
      } yield {
        assert(d1.isAllowed, "1st allowed")
        assert(d3.isAllowed && d3.remainingTokens == 0L, "3rd allowed, empties bucket")
        assert(!d4.isAllowed, "4th denied")
        d4 match {
          case Decision.Denied(remaining, retryAfter) =>
            assertEquals(remaining, 0L)
            assert(retryAfter > Duration.Zero)
          case other                                  => fail(s"expected Denied, got $other")
        }
      }
    }
  }

  test("cost consumes multiple tokens") {
    withClient { client =>
      val rl = client.rateLimiter[String](limit(5))
      for {
        first  <- rl.tryAcquire("cost", cost = 3)
        second <- rl.tryAcquire("cost", cost = 3)
      } yield {
        assert(first.isAllowed && first.remainingTokens == 2L, "cost 3 of 5 leaves 2")
        assert(!second.isAllowed && second.remainingTokens == 2L, "second cost-3 exceeds the 2 left, which stay untouched")
      }
    }
  }

  test("peek reports without consuming") {
    withClient { client =>
      val rl = client.rateLimiter[String](limit(3))
      for {
        _      <- rl.tryAcquire("peek")
        peeked <- rl.peek("peek")
        after  <- rl.tryAcquire("peek")
      } yield {
        assert(peeked.isAllowed && peeked.remainingTokens == 2L)
        assert(after.isAllowed && after.remainingTokens == 1L, "peek left the 2 tokens intact")
      }
    }
  }

  test("peek on an empty bucket is denied, still consuming nothing") {
    withClient { client =>
      val rl = client.rateLimiter[String](limit(1))
      for {
        _      <- rl.tryAcquire("peek-empty")
        peeked <- rl.peek("peek-empty")
      } yield peeked match {
        case Decision.Denied(remaining, retryAfter) =>
          assertEquals(remaining, 0L)
          assert(retryAfter > Duration.Zero, "reports the wait until one token is available")
        case other                                  => fail(s"expected Denied, got $other")
      }
    }
  }

  test("reset refills the bucket") {
    withClient { client =>
      val rl = client.rateLimiter[String](limit(1))
      for {
        first  <- rl.tryAcquire("reset")
        denied <- rl.tryAcquire("reset")
        _      <- rl.reset("reset")
        again  <- rl.tryAcquire("reset")
      } yield {
        assert(first.isAllowed)
        assert(!denied.isAllowed)
        assert(again.isAllowed, "reset restored capacity")
      }
    }
  }

  test("the computed sha matches the server's, and EVALSHA runs once loaded") {
    withClient { client =>
      val rl = RateLimiter[String](limit(2))
      for {
        loaded <- client.scriptLoad(RateLimiter.script)
        first  <- client.run(rl.evalSha("es", 1))
        second <- client.run(rl.evalSha("es", 1))
      } yield {
        assertEquals(loaded, RateLimiter.sha)
        assert(first.isAllowed && first.remainingTokens == 1L)
        assert(second.isAllowed && second.remainingTokens == 0L)
      }
    }
  }

  test("the native EVALSHA path recovers from NOSCRIPT by loading the script and retrying") {
    withClient { client =>
      val rl = client.rateLimiter[String](limit(2), namespace = "noscript")
      for {
        _      <- client.scriptFlush()
        first  <- rl.tryAcquire("cold")
        second <- rl.tryAcquire("cold")
      } yield {
        assert(first.isAllowed && first.remainingTokens == 1L, "a cold server is recovered by one load-and-retry")
        assert(second.isAllowed && second.remainingTokens == 0L, "the loaded script then serves EVALSHA directly")
      }
    }
  }

  test("composable escape hatch: run a RateLimiter Command directly (plain EVAL)") {
    withClient { client =>
      val rl = RateLimiter[String](limit(1))
      for {
        allowed <- client.run(rl.tryAcquire("cmd"))
        denied  <- client.run(rl.tryAcquire("cmd"))
      } yield {
        assert(allowed.isAllowed)
        assert(!denied.isAllowed)
      }
    }
  }

  test("the composable EVAL guard and validate agree on every policy and cost rule") {
    withClient { client =>
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
      def loop(remaining: List[((RateLimit, Long), Int)]): CIO[Unit]       =
        remaining match {
          case Nil                        => CIO.value(())
          case ((limit, cost), i) :: tail =>
            val rl = RateLimiter[String](limit, namespace = s"conform$i")
            serverRejects(rl, cost).flatMap { rejected =>
              assertEquals(rejected, rl.validate(cost).isDefined, s"case $i: $limit cost=$cost")
              loop(tail)
            }
        }
      loop(cases.zipWithIndex)
    }
  }

  test("RateLimiterClient.command exposes the same composable check") {
    withClient { client =>
      val rl = client.rateLimiter[String](limit(1))
      for {
        allowed <- client.run(rl.command("cmd-client"))
        denied  <- client.run(rl.command("cmd-client"))
      } yield {
        assert(allowed.isAllowed)
        assert(!denied.isAllowed)
      }
    }
  }

  test("accrues at the exact rate even at a large capacity") {
    withClient { client =>
      val rl = RateLimiter[String](RateLimit(capacity = 1_000_000_000L, refillTokens = 1000, refillPeriod = 1.second))
      val t0 = 7_000_000_000_000L
      for {
        _    <- client.run(rl.tryAcquireAt("big", 1_000_000_000L, t0))
        half <- client.run(rl.tryAcquireAt("big", 500, t0 + 500_000L))
        rest <- client.run(rl.tryAcquireAt("big", 500, t0 + 1_000_000L))
        over <- client.run(rl.tryAcquireAt("big", 1, t0 + 1_000_000L))
      } yield {
        assert(half.isAllowed && half.remainingTokens == 0L, "0.5s accrues exactly 500 tokens")
        assert(rest.isAllowed && rest.remainingTokens == 0L, "the next 0.5s accrues the other 500")
        assert(!over.isAllowed, "and no more than the rate allows")
      }
    }
  }

  test("refills continuously as injected time advances") {
    withClient { client =>
      val rl = RateLimiter[String](RateLimit(capacity = 10, refillTokens = 10, refillPeriod = 1.second))
      val t0 = 2_000_000_000_000L
      for {
        drain   <- client.run(rl.tryAcquireAt("refill", 10, t0))
        denied  <- client.run(rl.tryAcquireAt("refill", 1, t0))
        partial <- client.run(rl.tryAcquireAt("refill", 3, t0 + 300_000L))
        empty   <- client.run(rl.tryAcquireAt("refill", 1, t0 + 300_000L))
      } yield {
        assert(drain.isAllowed && drain.remainingTokens == 0L, "cost 10 empties a 10-token bucket")
        assert(!denied.isAllowed, "no tokens have refilled at t0")
        assert(partial.isAllowed && partial.remainingTokens == 0L, "0.3s refills exactly 3 tokens")
        assert(!empty.isAllowed, "and no more")
      }
    }
  }

  test("a backward server clock does not double-credit refill") {
    withClient { client =>
      val rl = RateLimiter[String](RateLimit(capacity = 10, refillTokens = 10, refillPeriod = 1.second))
      val t0 = 5_000_000_000_000L
      for {
        _        <- client.run(rl.tryAcquireAt("clock", 10, t0))
        backward <- client.run(rl.tryAcquireAt("clock", 1, t0 - 500_000L))
        recover  <- client.run(rl.tryAcquireAt("clock", 1, t0 + 100_000L))
        extra    <- client.run(rl.tryAcquireAt("clock", 1, t0 + 100_000L))
      } yield {
        assert(!backward.isAllowed, "a regressed clock credits nothing")
        assert(recover.isAllowed && recover.remainingTokens == 0L, "credit resumes from the high-water mark, not doubled")
        assert(!extra.isAllowed)
      }
    }
  }

  test("a non-integer refill rate credits whole tokens exactly and never re-counts elapsed time") {
    withClient { client =>
      val rl = RateLimiter[String](RateLimit(capacity = 10, refillTokens = 3, refillPeriod = 1.second))
      val t0 = 3_000_000_000_000L
      for {
        _      <- client.run(rl.tryAcquireAt("frac", 10, t0))
        early  <- client.run(rl.tryAcquireAt("frac", 1, t0 + 333_333L))
        repeat <- client.run(rl.tryAcquireAt("frac", 1, t0 + 333_333L))
        one    <- client.run(rl.tryAcquireAt("frac", 1, t0 + 333_334L))
      } yield {
        assert(!early.isAllowed, "just under 1/3 s accrues no whole token")
        assert(!repeat.isAllowed, "the same timestamp does not mint from re-counted elapsed")
        assert(one.isAllowed && one.remainingTokens == 0L, "one whole token at 333_334 micros, no more")
      }
    }
  }

  test("a clock rollback folds the catch-up interval into retryAfter and the key's TTL") {
    withClient { client =>
      val rl = RateLimiter[String](RateLimit(capacity = 1, refillTokens = 1, refillPeriod = 1.second))
      val t0 = 8_000_000_000_000L
      for {
        _      <- client.run(rl.tryAcquireAt("roll", 1, t0))
        rolled <- client.run(rl.tryAcquireAt("roll", 1, t0 - 4_000_000L))
        pttl   <- client.pTtl(bucketKey("roll"))
      } yield {
        rolled match {
          case Decision.Denied(_, retryAfter) => assertEquals(retryAfter, 5.seconds)
          case other                          => fail(s"expected Denied, got $other")
        }
        pttl match {
          case Ttl.Expires(remaining) => assert(remaining > 2.seconds && remaining <= 5.seconds, s"pttl was $remaining")
          case other                  => fail(s"expected an expiry, got $other")
        }
      }
    }
  }

  test("a large refill period and odd rollback retain the exact reported wait past 2^53") {
    withClient { client =>
      val period = 1L << 53
      val rl     = RateLimiter[String](RateLimit(capacity = 1, refillTokens = 1, refillPeriod = period.micros))
      val t0     = 9_000_000_000_000L
      for {
        _      <- client.run(rl.tryAcquireAt("big-period", 1, t0))
        rolled <- client.run(rl.tryAcquireAt("big-period", 1, t0 - 9L))
      } yield rolled match {
        case Decision.Denied(_, retryAfter) => assertEquals(retryAfter, (period + 9L).micros)
        case other                          => fail(s"expected Denied, got $other")
      }
    }
  }

  test("reusing a namespace with alternating policies never mints a fresh bucket") {
    withClient { client =>
      val original  = RateLimiter[String](RateLimit(capacity = 10, refillTokens = 10, refillPeriod = 1.second), namespace = "policy")
      val tightened = RateLimiter[String](RateLimit(capacity = 1, refillTokens = 1, refillPeriod = 1.second), namespace = "policy")
      val t0        = 6_000_000_000_000L
      for {
        old       <- client.run(original.tryAcquireAt("same-subject", 1, t0))
        firstNew  <- client.run(tightened.tryAcquireAt("same-subject", 1, t0))
        oldAgain  <- client.run(original.tryAcquireAt("same-subject", 1, t0))
        secondNew <- client.run(tightened.tryAcquireAt("same-subject", 1, t0))
        rolled    <- client.run(original.tryAcquireAt("same-subject", 1, t0 - 500_000L))
        recovered <- client.run(original.tryAcquireAt("same-subject", 1, t0))
      } yield {
        assertEquals(old.remainingTokens, 9L)
        assert(firstNew.isAllowed && firstNew.remainingTokens == 0L, "old credit is capped at the tightened capacity")
        assert(!oldAgain.isAllowed, "the old policy cannot recreate a full bucket during an overlapping deployment")
        assert(!secondNew.isAllowed, "alternating policy signatures cannot mint tokens")
        assert(!rolled.isAllowed, "a policy switch during clock rollback credits nothing")
        assert(!recovered.isAllowed, "clock recovery only reaches the preserved high-water mark")
      }
    }
  }

  test("malformed bucket fields are rejected instead of granting tokens") {
    withClient { client =>
      val rl                                      = RateLimiter[String](limit(1))
      def rejected(subject: String): CIO[Boolean] =
        client.run(rl.tryAcquireAt(subject, 1, 4_000_000_000_000L)).map(_ => false).recover {
          case ServerError("SAGE", message) if message.contains("invalid rate-limit state") => CIO.value(true)
          case other                                                                        => CIO.fail(other)
        }
      for {
        _            <- client.run(rl.tryAcquireAt("missing", 1, 4_000_000_000_000L))
        _            <- client.hDel(bucketKey("missing"), "ts")
        missingField <- rejected("missing")
        _            <- client.run(rl.tryAcquireAt("out-of-range", 1, 4_000_000_000_000L))
        _            <- client.hSet(bucketKey("out-of-range"), ("t", "2"))
        outOfRange   <- rejected("out-of-range")
        _            <- client.run(rl.tryAcquireAt("full-with-fraction", 1, 4_000_000_000_000L))
        _            <- client.hSet(bucketKey("full-with-fraction"), ("t", "1"), ("f", "1"))
        fullFraction <- rejected("full-with-fraction")
      } yield {
        assert(missingField, "a missing field must not restore capacity")
        assert(outOfRange, "out-of-range tokens must not exceed capacity")
        assert(fullFraction, "a full bucket must not carry fractional refill credit")
      }
    }
  }

  test("an already-full bucket reports zero time to full") {
    withClient { client =>
      val rl = RateLimiter[String](RateLimit(capacity = 5, refillTokens = 5, refillPeriod = 1.second))
      for {
        peeked <- client.run(rl.peek("full"))
      } yield peeked match {
        case Decision.Allowed(remaining, resetAfter) =>
          assertEquals(remaining, 5L)
          assertEquals(resetAfter, Duration.Zero)
        case other                                   => fail(s"expected Allowed, got $other")
      }
    }
  }
}

class RedisRateLimiterSuite  extends RateLimiterSuite(Images.redis)
class ValkeyRateLimiterSuite extends RateLimiterSuite(Images.valkey)

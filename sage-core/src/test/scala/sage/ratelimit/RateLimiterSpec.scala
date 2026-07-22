package sage.ratelimit

import scala.concurrent.duration.*

import sage.Bytes
import sage.protocol.Frame

class RateLimiterSpec extends munit.FunSuite {

  private val limiter = RateLimiter[String](RateLimit.perSecond(10))

  private def reply(allowed: Long, remaining: Long, catchup: Long, retry: Long, reset: Long): Frame =
    Frame.Array(
      Vector(Frame.Integer(allowed), Frame.Integer(remaining), Frame.Integer(catchup), Frame.Integer(retry), Frame.Integer(reset))
    )

  test("tryAcquire builds an EVAL command routed on the single namespaced key") {
    val command = limiter.tryAcquire("user", cost = 2)
    assertEquals(command.name, "EVAL")
    assertEquals(command.keyIndices, Vector(2))
    assert(command.args(2).sameBytes(Bytes.utf8("9:ratelimit:user")))     // length-framed namespace
    assertEquals(command.args(1).asUtf8String, "1")                       // numkeys
    assertEquals(command.args.last.asUtf8String, "")                      // empty peek flag => consume
    assertEquals(command.args(command.args.length - 2).asUtf8String, "")  // injected time empty => server TIME
    assertEquals(command.args(command.args.length - 3).asUtf8String, "2") // cost
  }

  test("a custom namespace prefixes the key") {
    assert(RateLimiter[String](RateLimit.perSecond(1), "api").tryAcquire("k").args(2).sameBytes(Bytes.utf8("3:api:k")))
  }

  test("decode maps an allowed reply to Allowed with remaining and reset") {
    assertEquals(limiter.tryAcquire("u").decode(reply(1, 8, 0, 0, 500000)), Right(Decision.Allowed(8, 500000.micros)))
  }

  test("decode maps a denied reply to Denied with the untouched balance and retry-after") {
    assertEquals(limiter.tryAcquire("u").decode(reply(0, 5, 0, 250000, 1000000)), Right(Decision.Denied(5, 250000.micros)))
  }

  test("decode combines exact timing components and saturates only at the documented maximum") {
    val maxExact    = 1L << 53
    val pastMaximum = RateLimiter.maximumReportedWait.toMicros - maxExact + 1L
    assertEquals(RateLimiter.maximumReportedWait.toMicros, Long.MaxValue / 1000L)
    assertEquals(limiter.tryAcquire("u").decode(reply(0, 0, 9, maxExact, 0)), Right(Decision.Denied(0, (maxExact + 9).micros)))
    assertEquals(
      limiter.tryAcquire("u").decode(reply(0, 0, maxExact, pastMaximum, 0)),
      Right(Decision.Denied(0, RateLimiter.maximumReportedWait))
    )
  }

  test("decode rejects a reply of the wrong shape") {
    assert(limiter.tryAcquire("u").decode(Frame.Integer(1)).isLeft)
    assert(limiter.tryAcquire("u").decode(Frame.Array(Vector(Frame.Integer(1)))).isLeft)
  }

  test("peek builds a cost-1 probe that consumes nothing") {
    val command = limiter.peek("u")
    assertEquals(command.args(command.args.length - 3).asUtf8String, "1") // cost
    assertEquals(command.args.last.asUtf8String, "1")                     // peek flag
  }

  test("reset builds a DEL on the namespaced key") {
    val command = limiter.reset("u")
    assertEquals(command.name, "DEL")
    assert(command.args.head.sameBytes(Bytes.utf8("9:ratelimit:u")))
  }

  test("evalSha builds an EVALSHA command carrying the script sha") {
    val command = limiter.evalSha("u", 1)
    assertEquals(command.name, "EVALSHA")
    assertEquals(command.args.head.asUtf8String, RateLimiter.sha)
    assertEquals(command.keyIndices, Vector(2))
  }

  test("loadCommand is SCRIPT LOAD and sha is a 40-char hex digest") {
    assertEquals(limiter.loadCommand.name, "SCRIPT")
    assertEquals(RateLimiter.sha.length, 40)
    assert(RateLimiter.sha.forall(c => c.isDigit || ('a' to 'f').contains(c)))
  }

  test("Decision helpers") {
    assert(Decision.Allowed(5, 1.second).isAllowed)
    assertEquals(Decision.Allowed(5, 1.second).remainingTokens, 5L)
    assert(!Decision.Denied(2, 1.second).isAllowed)
    assertEquals(Decision.Denied(2, 1.second).remainingTokens, 2L)
  }

  test("RateLimit smart constructors") {
    assertEquals(RateLimit.perSecond(10), RateLimit(10, 10, 1.second))
    assertEquals(RateLimit(100, 1.minute, burst = 200), RateLimit(200, 100, 1.minute))
  }

  test("validate accepts a usable policy and cost") {
    assertEquals(limiter.validate(1), None)
    assertEquals(RateLimiter[String](RateLimit.perSecond(10)).validate(10), None)
  }

  test("validate reports the first problem with the policy or cost") {
    assertEquals(RateLimiter[String](RateLimit(0, 1, 1.second)).validate(1), Some("capacity must be > 0"))
    assertEquals(RateLimiter[String](RateLimit(10, 0, 1.second)).validate(1), Some("refillTokens must be > 0"))
    assertEquals(RateLimiter[String](RateLimit(10, 1, 500.nanos)).validate(1), Some("refillPeriod must be at least 1 microsecond"))
    assertEquals(
      RateLimiter[String](RateLimit((1L << 53) + 1, 1, 1.second)).validate(1),
      Some("capacity must be <= 9007199254740992 (Lua number precision)")
    )
    assertEquals(
      RateLimiter[String](RateLimit(10, (1L << 53) + 1, 1.second)).validate(1),
      Some("refillTokens must be <= 9007199254740992 (Lua number precision)")
    )
    assertEquals(
      RateLimiter[String](RateLimit(10, 1, 9100000000000000L.micros)).validate(1),
      Some("refillPeriod must be <= 9007199254740992 microseconds (Lua number precision)")
    )
    assertEquals(
      RateLimiter[String](RateLimit(1000000000L, 1, 10000.seconds)).validate(1),
      Some("capacity times refillPeriod must be <= 9007199254740992 microseconds (so refill math stays exact)")
    )
    assertEquals( // 3 * 3002399751580331 = 2^53 + 1
      RateLimiter[String](RateLimit(3, 1, 3002399751580331L.micros)).validate(1),
      Some("capacity times refillPeriod must be <= 9007199254740992 microseconds (so refill math stays exact)")
    )
    assertEquals(limiter.validate(0), Some("cost must be >= 1"))
    assertEquals(limiter.validate(-1), Some("cost must be >= 1"))
    assertEquals(limiter.validate(11), Some("cost 11 cannot exceed capacity 10"))
  }

  test("a length-framed namespace keeps prefix-colliding namespaces from sharing a bucket") {
    val a = RateLimiter[String](RateLimit.perSecond(1), "a").tryAcquire("b:c").args(2)
    val b = RateLimiter[String](RateLimit.perSecond(1), "a:b").tryAcquire("c").args(2)
    assert(!a.sameBytes(b), "namespace `a`/subject `b:c` must not collide with namespace `a:b`/subject `c`")
  }
}

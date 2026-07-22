package sage.ratelimit

import scala.concurrent.duration.*

/**
  * A token-bucket policy: up to `capacity` tokens, refilled at `refillTokens` per `refillPeriod`.
  */
final case class RateLimit(capacity: Long, refillTokens: Long, refillPeriod: FiniteDuration) {

  def refillPeriodMicros: Long = refillPeriod.toMicros
}

object RateLimit {

  /**
    * `permits` requests per second, bursting up to `permits`.
    */
  def perSecond(permits: Long): RateLimit = RateLimit(permits, permits, 1.second)

  /**
    * `permits` requests per minute, bursting up to `permits`.
    */
  def perMinute(permits: Long): RateLimit = RateLimit(permits, permits, 1.minute)

  /**
    * `permits` tokens refilled every `per`, bursting up to `burst`.
    */
  def apply(permits: Long, per: FiniteDuration, burst: Long): RateLimit = RateLimit(burst, permits, per)
}

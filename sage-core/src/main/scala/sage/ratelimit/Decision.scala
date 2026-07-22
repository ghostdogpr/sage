package sage.ratelimit

import scala.concurrent.duration.FiniteDuration

/**
  * The outcome of a rate-limit check: an ordinary returned value, not an error.
  */
enum Decision {

  /**
    * Admitted, with `remaining` tokens left and `resetAfter` until the bucket refills to full.
    */
  case Allowed(remaining: Long, resetAfter: FiniteDuration)

  /**
    * Rejected, with the untouched `remaining` balance (a denial consumes nothing) and `retryAfter` until enough tokens are available.
    */
  case Denied(remaining: Long, retryAfter: FiniteDuration)

  def isAllowed: Boolean = this match {
    case _: Allowed => true
    case _: Denied  => false
  }

  /**
    * Tokens left in the bucket; a denial consumes nothing, so this is the untouched balance.
    */
  def remainingTokens: Long = this match {
    case Allowed(remaining, _) => remaining
    case Denied(remaining, _)  => remaining
  }
}

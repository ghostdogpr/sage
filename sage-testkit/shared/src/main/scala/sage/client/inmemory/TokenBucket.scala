package sage.client.inmemory

import scala.collection.immutable.VectorMap
import scala.concurrent.duration.{DurationLong, FiniteDuration}

import sage.Bytes
import sage.protocol.Frame
import sage.ratelimit.{Decision, RateLimiter}

/**
  * A Scala port of [[RateLimiter.script]], a token bucket stored in a hash. The script uses Lua numbers, which are doubles, and limits
  * every value to 2^53 so that they stay exact; this port computes with `Long` within the same limits, which gives the same results. The
  * comments of the Lua script explain each step.
  */
private[inmemory] object TokenBucket {

  private val stateFields: Vector[Key] = Vector("t", "ts", "f", "v").map(name => new Key(Bytes.utf8(name)))

  private def withinExact(text: String): Boolean = text.length < 16 || (text.length == 16 && text <= "9007199254740992")

  private def storedInteger(bytes: Bytes): Option[Long] =
    Some(bytes.asUtf8String).filter(text => text.nonEmpty && text.forall(_.isDigit) && withinExact(text)).flatMap(_.toLongOption)

  /**
    * Applies a request of `cost` tokens for `subject` at `now`, a Unix time in milliseconds, and stores the bucket. The caller holds the
    * store lock and has validated the policy and the cost with `RateLimiter.validate`. Throws [[Reject]] when the key holds another
    * type or the bucket's fields are invalid.
    */
  def acquire[K](store: Store, limiter: RateLimiter[K], subject: K, cost: Long, peek: Boolean, now: Long): Decision = {
    val capacity                                 = limiter.limit.capacity
    val refill                                   = limiter.limit.refillTokens
    val period                                   = limiter.limit.refillPeriodMicros
    val key                                      = new Key(limiter.keyBytes(subject))
    val micros                                   = now * 1000L
    val signature                                = Bytes.utf8(s"$capacity:$refill:$period")
    val fields                                   = store.lookup(key, now).fold(VectorMap.empty[Key, Bytes]) { entry =>
      entry.value match {
        case hash: Value.Hash => hash.fields
        case _                => throw new Reject(Errors.wrongType)
      }
    }
    val state                                    = stateFields.map(fields.get)
    val changed                                  = !state(3).exists(_.sameBytes(signature))
    var (tokens, ts, frac)                       =
      if (state.forall(_.isEmpty)) (capacity, micros, 0L)
      else
        state.take(3).map(_.flatMap(storedInteger)) match {
          case Vector(Some(tokens), Some(ts), Some(_)) if changed                                                                    =>
            (math.min(tokens, capacity), math.max(ts, micros), 0L)
          case Vector(Some(tokens), Some(ts), Some(frac)) if tokens <= capacity && frac < period && (tokens < capacity || frac == 0) =>
            (tokens, ts, frac)
          case _                                                                                                                     =>
            throw new Reject(Frame.SimpleError("SAGE invalid rate-limit state"))
        }
    if (micros > ts) {
      val space = capacity - tokens
      if (space <= 0) frac = 0
      else {
        val need = space * period - frac
        if (micros - ts >= Math.ceilDiv(need, refill)) {
          tokens = capacity
          frac = 0
        } else {
          val units  = (micros - ts) * refill + frac
          val gained = Math.floorDiv(units, period)
          frac = units - gained * period
          tokens += gained
        }
      }
      ts = micros
    }
    val catchup                                  = if (ts > micros) ts - micros else 0L
    val allowed                                  = tokens >= cost
    val retry                                    = if (allowed) 0L else Math.ceilDiv((cost - tokens) * period - frac, refill)
    if (allowed && !peek) tokens -= cost
    val (timedCatchup, reset)                    =
      if (tokens < capacity) (catchup, Math.ceilDiv((capacity - tokens) * period - frac, refill)) else (0L, 0L)
    val ttl                                      = math.max(1L, timedCatchup / 1000 + reset / 1000 + Math.ceilDiv(timedCatchup % 1000 + reset % 1000, 1000L))
    val updated                                  = fields ++ stateFields.zip(Vector(tokens, ts, frac).map(n => Bytes.utf8(n.toString)) :+ signature)
    store.put(key, Value.Hash(updated), Ttl.At(Math.addExact(now, ttl)), now)
    val max                                      = RateLimiter.maximumReportedWait.toMicros
    def wait(refillMicros: Long): FiniteDuration =
      if (timedCatchup > max || refillMicros > max - timedCatchup) RateLimiter.maximumReportedWait else (timedCatchup + refillMicros).micros
    if (allowed) Decision.Allowed(tokens, wait(reset)) else Decision.Denied(tokens, wait(retry))
  }
}

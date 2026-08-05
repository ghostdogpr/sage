package sage.ratelimit

import scala.concurrent.duration.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.KeyCodec
import sage.commands.*

/**
  * A distributed token-bucket rate limiter. Each method returns a [[sage.commands.Command]] that works with every backend. One hash key stores
  * each subject's state in a single cluster slot. An atomic Lua script reads the server clock, refills the bucket, and applies the request.
  * When a policy changes under an existing namespace, Sage retains the smaller of the current whole-token balance and the new capacity. This
  * prevents overlapping deployments from repeatedly creating full buckets. Full, idle buckets expire. The next request creates a bucket
  * using the current policy.
  */
final case class RateLimiter[K](limit: RateLimit, namespace: String = RateLimiter.defaultNamespace)(using keyCodec: KeyCodec[K]) {

  /**
    * Attempts to consume `cost` tokens for `subject`. Returns [[Decision.Allowed]] with the remaining balance, or [[Decision.Denied]] with the
    * time until enough tokens become available. Returns immediately.
    */
  def tryAcquire(subject: K, cost: Long = 1): Command[Decision] = eval(RateLimiter.Invocation.Eval, subject, cost, peek = false)

  /**
    * Checks `subject` without consuming a token. Elapsed time still refills the bucket. Returns [[Decision.Allowed]] when a token is available,
    * or [[Decision.Denied]] with the time until one becomes available.
    */
  def peek(subject: K): Command[Decision] = eval(RateLimiter.Invocation.Eval, subject, cost = 1, peek = true)

  /**
    * Clears `subject`'s bucket. Its next request starts with full capacity.
    */
  def reset(subject: K): Command[Unit] =
    Command("DEL", Command.FirstKey, Vector(keyBytes(subject)), _ => Right(()))

  private val capacityText            = limit.capacity.toString
  private val refillTokensText        = limit.refillTokens.toString
  private val refillPeriodMicros      = limit.refillPeriodMicros
  private val refillPeriodText        = refillPeriodMicros.toString
  private val capacityArgument        = Bytes.utf8(capacityText)
  private val refillArgument          = Bytes.utf8(refillTokensText)
  private val periodArgument          = Bytes.utf8(refillPeriodText)
  private val policySignatureArgument = Bytes.utf8(s"$capacityText:$refillTokensText:$refillPeriodText")
  private val keyPrefix               = {
    val ns = Bytes.utf8(namespace)
    Bytes.concat(Vector(Bytes.utf8(s"${ns.length}:"), ns, Bytes.utf8(":")))
  }
  private val policyProblem           =
    if (limit.capacity <= 0) Some("capacity must be > 0")
    else if (limit.refillTokens <= 0) Some("refillTokens must be > 0")
    else if (refillPeriodMicros < 1) Some("refillPeriod must be at least 1 microsecond")
    else if (limit.capacity > RateLimiter.maxExactInt) Some(s"capacity must be <= ${RateLimiter.maxExactInt} (Lua number precision)")
    else if (limit.refillTokens > RateLimiter.maxExactInt) Some(s"refillTokens must be <= ${RateLimiter.maxExactInt} (Lua number precision)")
    else if (refillPeriodMicros > RateLimiter.maxExactInt)
      Some(s"refillPeriod must be <= ${RateLimiter.maxExactInt} microseconds (Lua number precision)")
    else if (BigInt(limit.capacity) * refillPeriodMicros > BigInt(RateLimiter.maxExactInt))
      Some(s"capacity times refillPeriod must be <= ${RateLimiter.maxExactInt} microseconds (so refill math stays exact)")
    else None

  private[sage] def validate(cost: Long): Option[String] = policyProblem match {
    case problem @ Some(_) => problem
    case None              =>
      if (cost < 1) Some("cost must be >= 1")
      else if (cost > limit.capacity) Some(s"cost $cost cannot exceed capacity ${limit.capacity}")
      else None
  }

  private[sage] def evalSha(subject: K, cost: Long, peek: Boolean = false): Command[Decision] =
    eval(RateLimiter.Invocation.EvalSha, subject, cost, peek)

  // test-only entry point that supplies the script clock explicitly.
  private[sage] def tryAcquireAt(subject: K, cost: Long, nowMicros: Long): Command[Decision] =
    eval(RateLimiter.Invocation.Eval, subject, cost, peek = false, Some(nowMicros))

  // NOSCRIPT recovery for evalSha. Sending the body runs the check and caches the script on that node.
  private[sage] def evalScript(subject: K, cost: Long, peek: Boolean): Command[Decision] =
    eval(RateLimiter.Invocation.Eval, subject, cost, peek)

  // length framing distinguishes namespace `a` with subject `b:c` from namespace `a:b` with subject `c`.
  private def keyBytes(subject: K): Bytes = Bytes.concat(Vector(keyPrefix, keyCodec.encode(subject)))

  private def eval(invocation: RateLimiter.Invocation, subject: K, cost: Long, peek: Boolean, now: Option[Long] = None): Command[Decision] = {
    val costArgument = if (cost == 1L) RateLimiter.defaultCostArgument else Bytes.utf8(cost.toString)
    val nowArgument  = now match {
      case None        => Bytes.empty // an empty injected time uses the server's TIME value
      case Some(value) => Bytes.utf8(value.toString)
    }
    val allArgs      = Vector(
      invocation.scriptReference,
      RateLimiter.oneKeyArgument,
      keyBytes(subject),
      capacityArgument,
      refillArgument,
      periodArgument,
      policySignatureArgument,
      costArgument,
      nowArgument,
      if (peek) RateLimiter.peekArgument else Bytes.empty
    )
    Command(invocation.verb, RateLimiter.scriptKeyIndices, allArgs, RateLimiter.decode, Execution.Ordinary)
  }
}

object RateLimiter {

  // Reply: [allowed, remaining, catchupMicros, retryMicros, resetMicros]. Scala sums the separate catch-up value beyond Lua's 2^53 limit.
  // State hash: `t` tokens, `ts` last-refill micros, `f` sub-token remainder, `v` policy signature. ARGV[6] overrides server TIME for
  // tests. ARGV[7] = '1' checks `cost` without consuming tokens.
  val script: String =
    """local capacity = tonumber(ARGV[1])
      |local refill_tokens = tonumber(ARGV[2])
      |local refill_period = tonumber(ARGV[3])
      |local cost = tonumber(ARGV[5])
      |local injected = ARGV[6]
      |local is_peek = ARGV[7] == '1'
      |
      |-- last guard for the EVAL path; each field is bounded on its raw decimal string so a value just past 2^53 is rejected, not rounded in
      |local max_exact = 9007199254740992 -- 2^53
      |local function within_exact(s)
      |  local n = string.len(s)
      |  if n > 16 then return false end
      |  if n < 16 then return true end
      |  return s <= '9007199254740992'
      |end
      |local function stored_integer(s)
      |  return type(s) == 'string' and string.match(s, '^%d+$') ~= nil and within_exact(s)
      |end
      |-- 2^53+1 is the only integer a double rounds down to 2^53, and it is odd, so reject an odd product that reads as 2^53
      |local product = capacity * refill_period
      |local product_over = product > max_exact or (product == max_exact and capacity % 2 == 1 and refill_period % 2 == 1)
      |if capacity <= 0 or refill_tokens < 1 or refill_period < 1
      |   or not within_exact(ARGV[1]) or not within_exact(ARGV[2]) or not within_exact(ARGV[3]) or not within_exact(ARGV[5])
      |   or product_over
      |   or cost < 1 or cost > capacity then
      |  return redis.error_reply('SAGE invalid rate-limit policy or cost')
      |end
      |
      |local now
      |if injected ~= '' then
      |  now = tonumber(injected)
      |else
      |  local t = redis.call('TIME')
      |  now = tonumber(t[1]) * 1000000 + tonumber(t[2])
      |end
      |
      |local state = redis.call('HMGET', KEYS[1], 't', 'ts', 'f', 'v')
      |local state_absent = not state[1] and not state[2] and not state[3] and not state[4]
      |local tokens = tonumber(state[1])
      |local ts = tonumber(state[2])
      |local frac = tonumber(state[3])
      |local policy_changed = state[4] ~= ARGV[4]
      |if state_absent then
      |  tokens = capacity
      |  ts = now
      |  frac = 0
      |elseif not stored_integer(state[1]) or not stored_integer(state[2]) or not stored_integer(state[3])
      |   or (not policy_changed and (tokens > capacity or frac >= refill_period or (tokens == capacity and frac ~= 0))) then
      |  return redis.error_reply('SAGE invalid rate-limit state')
      |elseif policy_changed then
      |  -- keep only compatible whole-token credit; never refill merely because old and new policy instances alternate
      |  if tokens > capacity then tokens = capacity end
      |  if now > ts then ts = now end
      |  frac = 0
      |end
      |
      |-- refill by whole tokens, carrying the sub-token remainder in `frac`. Compare elapsed against the fill time, not elapsed * refill_tokens,
      |-- so that product is only formed when it is < capacity * refill_period and stays exact. A regressed clock (now <= ts) credits nothing.
      |if now > ts then
      |  local space = capacity - tokens
      |  if space <= 0 then
      |    frac = 0
      |  else
      |    local need = space * refill_period - frac
      |    if now - ts >= math.ceil(need / refill_tokens) then
      |      tokens = capacity
      |      frac = 0
      |    else
      |      local units = (now - ts) * refill_tokens + frac
      |      local gained = math.floor(units / refill_period)
      |      frac = units - gained * refill_period
      |      tokens = tokens + gained
      |    end
      |  end
      |  ts = now
      |end
      |
      |-- a regressed clock leaves ts ahead of now; return this catch-up separately so Scala sums the parts past 2^53
      |local catchup = 0
      |if ts > now then catchup = ts - now end
      |
      |local allowed = 0
      |local retry_wait = 0
      |if tokens >= cost then
      |  if not is_peek then tokens = tokens - cost end
      |  allowed = 1
      |else
      |  retry_wait = math.ceil(((cost - tokens) * refill_period - frac) / refill_tokens)
      |end
      |
      |redis.call('HSET', KEYS[1],
      |  't', string.format('%.0f', tokens), 'ts', string.format('%.0f', ts), 'f', string.format('%.0f', frac),
      |  'v', ARGV[4])
      |local reset_wait = 0
      |local timed_catchup = 0
      |if tokens < capacity then
      |  timed_catchup = catchup
      |  reset_wait = math.ceil(((capacity - tokens) * refill_period - frac) / refill_tokens)
      |end
      |-- sum the TTL in millisecond quotient/remainder form, rounding up, so it stays exact and never expires before the bucket could refill
      |local ttl = math.floor(timed_catchup / 1000) + math.floor(reset_wait / 1000)
      |local leftover_micros = timed_catchup % 1000 + reset_wait % 1000
      |ttl = ttl + math.ceil(leftover_micros / 1000)
      |if ttl < 1 then ttl = 1 end
      |redis.call('PEXPIRE', KEYS[1], ttl)
      |
      |return { allowed, tokens, timed_catchup, retry_wait, reset_wait }
      |""".stripMargin

  private[sage] val sha: String = {
    val digest = java.security.MessageDigest.getInstance("SHA-1").digest(script.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    digest.iterator.map(b => f"${b & 0xff}%02x").mkString
  }

  /**
    * The greatest retry/reset duration returned; a wait beyond it (severe clock rollback) saturates here, while the TTL still covers it.
    */
  val maximumReportedWait: FiniteDuration = (Long.MaxValue / 1000L).micros

  private val maximumReportedWaitMicros: Long = maximumReportedWait.toMicros

  private def waitDuration(catchup: Long, refill: Long): Either[DecodeError, FiniteDuration] =
    if (catchup < 0 || refill < 0) Left(DecodeError("non-negative rate-limit timing components", s"$catchup and $refill microseconds"))
    else if (catchup > maximumReportedWaitMicros || refill > maximumReportedWaitMicros - catchup) Right(maximumReportedWait)
    else Right((catchup + refill).micros)

  private val decode: sage.protocol.Frame => Either[DecodeError, Decision] = frame =>
    frame.asArray.flatMap {
      case Vector(allowedFrame, remainingFrame, catchupFrame, retryFrame, resetFrame) =>
        for {
          allowed    <- allowedFrame.asLong
          remaining  <- remainingFrame.asLong
          catchup    <- catchupFrame.asLong
          retry      <- retryFrame.asLong
          reset      <- resetFrame.asLong
          retryAfter <- waitDuration(catchup, retry)
          resetAfter <- waitDuration(catchup, reset)
        } yield if (allowed == 1L) Decision.Allowed(remaining, resetAfter) else Decision.Denied(remaining, retryAfter)
      case other                                                                      =>
        Left(DecodeError("rate-limit reply [allowed, remaining, catchup, retry, reset]", s"array of ${other.length} elements"))
    }

  private[sage] val defaultNamespace: String = "ratelimit"

  // Lua numbers are IEEE doubles, so integers (and capacity * refillPeriod) are held to 2^53 to stay exact
  private[sage] val maxExactInt: Long = 1L << 53

  private val scriptKeyIndices: Vector[Int] = Vector(2) // 0 = script/sha, 1 = numkeys, 2 = the single key
  private val oneKeyArgument: Bytes         = Bytes.utf8("1")
  private val defaultCostArgument: Bytes    = Bytes.utf8("1")
  private val peekArgument: Bytes           = Bytes.utf8("1")

  private enum Invocation(val verb: String, val scriptReference: Bytes) {
    case Eval    extends Invocation("EVAL", Bytes.utf8(script))
    case EvalSha extends Invocation("EVALSHA", Bytes.utf8(sha))
  }
}

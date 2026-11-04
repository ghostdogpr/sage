package sage.client.inmemory

import scala.collection.immutable.VectorMap

import sage.Bytes
import sage.client.internal.LockExecutor
import sage.protocol.Frame
import sage.ratelimit.RateLimiter

/**
  * `EVAL`, `EVALSHA` and `SCRIPT` for the Lua scripts that Sage itself sends: the distributed lock and the rate limiter. The in-memory
  * client has no Lua interpreter, so it recognizes those scripts by their SHA-1 digest and runs a Scala port of each. Any other script
  * fails with an `ERR` reply.
  *
  * The script cache behaves like a server's: `EVAL` and `SCRIPT LOAD` add a script, `SCRIPT FLUSH` empties the cache, and `EVALSHA` of a
  * script that is not cached fails with `NOSCRIPT`, which makes Sage resend the script body.
  */
private[inmemory] object Scripts {
  import Replies.*

  // The arguments of a script call after the script reference: `numkeys key… arg…`.
  final private case class Invocation(keys: Vector[Key], argv: Vector[Bytes])

  private val natives: Map[String, (Call, Invocation) => Frame] = Map(
    LockExecutor.compiled.sha -> lock,
    RateLimiter.compiled.sha  -> RateLimitScript.run
  )

  private val bodies: Map[String, String] = Map(
    LockExecutor.compiled.sha -> LockExecutor.script,
    RateLimiter.compiled.sha  -> RateLimiter.script
  )

  val specs: Map[String, Spec] = Map(
    "EVAL"       -> Spec(-3, call => eval(call, bySha = false)),
    "EVAL_RO"    -> Spec(-3, call => eval(call, bySha = false)),
    "EVALSHA"    -> Spec(-3, call => eval(call, bySha = true)),
    "EVALSHA_RO" -> Spec(-3, call => eval(call, bySha = true)),
    "SCRIPT"     -> Spec(-2, script)
  )

  private def sha1(body: Bytes): String =
    java.security.MessageDigest.getInstance("SHA-1").digest(body.toArray).iterator.map(b => f"${b & 0xff}%02x").mkString

  private val unsupported = Errors.err("the in-memory client cannot run this Lua script; it only runs the scripts of Sage's locks and rate limiters")

  private def eval(call: Call, bySha: Boolean): Frame = {
    val sha = if (bySha) call.text(0).toLowerCase(java.util.Locale.ROOT) else sha1(call.args(0))
    if (bySha && !call.store.scripts.contains(sha)) Frame.SimpleError("NOSCRIPT No matching script. Please use EVAL.")
    else {
      if (!bySha) call.store.scripts += sha
      val count      = call.long(1)
      if (count < 0) throw new Reject(Errors.err("Number of keys can't be negative"))
      if (2 + count > call.args.length) throw new Reject(Errors.err("Number of keys can't be greater than number of args"))
      val invocation = Invocation(Vector.tabulate(count.toInt)(i => call.key(2 + i)), call.args.drop(2 + count.toInt))
      natives.get(sha).fold(unsupported)(_(call, invocation))
    }
  }

  private def script(call: Call): Frame =
    call.keyword(0) match {
      case "LOAD" if call.args.length == 2  =>
        val sha = sha1(call.args(1))
        if (!natives.contains(sha)) unsupported
        else {
          call.store.scripts += sha
          bulk(sha)
        }
      case "EXISTS" if call.args.length > 1 =>
        Frame.Array(call.args.drop(1).map(arg => flag(call.store.scripts.contains(arg.asUtf8String.toLowerCase(java.util.Locale.ROOT)))))
      // no script ever runs long enough to be killed
      case "KILL" if call.args.length == 1  => Frame.SimpleError("NOTBUSY No scripts in execution right now.")
      case "FLUSH"                          =>
        call.store.scripts.clear()
        ok
      case "SHOW" if call.args.length == 2  =>
        val sha = call.text(1).toLowerCase(java.util.Locale.ROOT)
        if (call.store.scripts.contains(sha)) bodies.get(sha).fold(unsupported)(bulk(_))
        else Frame.SimpleError("NOSCRIPT No matching script")
      case _                                => throw new Reject(Errors.syntax)
    }

  /**
    * The lock script of [[LockExecutor]]. `acquire` sets the key to the caller's token with the lease as its expiry, or extends the lease
    * when the caller already holds it. `renew` extends and `release` deletes the key, both only for the token that holds it.
    */
  private def lock(call: Call, invocation: Invocation): Frame = {
    val key            = invocation.keys(0)
    val token          = invocation.argv(0)
    val operation      = invocation.argv(1).asUtf8String
    def leaseEnd: Long =
      Numbers.parseLong(invocation.argv(2)).filter(_ > 0).fold(throw new Reject(Errors.invalidExpire("set")))(call.now + _)
    def holds: Boolean = call.string(key).exists(_.sameBytes(token))
    operation match {
      case "acquire"                     =>
        if (call.lookup(key).isEmpty) {
          call.store.put(key, Value.Str(token), leaseEnd, call.now)
          int(1L)
        } else if (holds) {
          call.store.setExpiry(key, call.lookup(key).get, leaseEnd)
          int(1L)
        } else int(0L)
      case "renew" | "release" if !holds => int(0L)
      case "renew"                       =>
        call.store.setExpiry(key, call.lookup(key).get, leaseEnd)
        int(1L)
      case "release"                     => flag(call.store.delete(key, call.now))
      case _                             => Frame.SimpleError("SAGE invalid lock operation")
    }
  }

  /**
    * A Scala port of [[RateLimiter.script]], a token bucket stored in a hash. The script uses Lua numbers, which are doubles, and limits
    * every value to 2^53 so that they stay exact; this port uses the same limits and computes with `Long` and `BigInt`, which gives the same
    * results. The comments of the Lua script explain each step.
    */
  private object RateLimitScript {

    private val MaxExact = BigInt(1L << 53)

    private def withinExact(text: String): Boolean = text.length < 16 || (text.length == 16 && text <= "9007199254740992")

    private def storedInteger(value: Option[Bytes]): Boolean =
      value.exists(bytes => bytes.length > 0 && bytes.asUtf8String.forall(_.isDigit) && withinExact(bytes.asUtf8String))

    private def number(bytes: Bytes): BigInt = BigInt(bytes.asUtf8String)

    private def invalid(what: String): Frame = Frame.SimpleError(s"SAGE invalid rate-limit $what")

    def run(call: Call, invocation: Invocation): Frame = {
      val argv                                 = invocation.argv
      val texts                                = Vector(0, 1, 2, 4).map(argv(_).asUtf8String)
      val (capacityB, refillB, periodB, costB) = (number(argv(0)), number(argv(1)), number(argv(2)), number(argv(4)))
      if (
        capacityB <= 0 || refillB < 1 || periodB < 1 || !texts.forall(withinExact) || capacityB * periodB > MaxExact || costB < 1 ||
          costB > capacityB
      ) invalid("policy or cost")
      else {
        val capacity                                  = capacityB.toLong
        val refill                                    = refillB.toLong
        val period                                    = periodB.toLong
        val cost                                      = costB.toLong
        val peek                                      = argv(6).asUtf8String == "1"
        val now                                       = if (argv(5).length == 0) call.now * 1000L else number(argv(5)).toLong
        val key                                       = invocation.keys(0)
        val fields                                    = call.hash(key).fold(VectorMap.empty[Key, Value.Field])(_.fields)
        def field(name: String): Option[Bytes]        = fields.get(new Key(Bytes.utf8(name))).map(_.value)
        val state                                     = Vector("t", "ts", "f", "v").map(field)
        val changed                                   = !state(3).exists(_.sameBytes(argv(3)))
        def stored(i: Int): Long                      = state(i).fold(0L)(_.asUtf8String.toLong)
        val result: Either[Frame, (Long, Long, Long)] =
          if (state.forall(_.isEmpty)) Right((capacity, now, 0L))
          else if (!storedInteger(state(0)) || !storedInteger(state(1)) || !storedInteger(state(2))) Left(invalid("state"))
          else {
            val (tokens, ts, frac) = (stored(0), stored(1), stored(2))
            if (!changed && (tokens > capacity || frac >= period || (tokens == capacity && frac != 0))) Left(invalid("state"))
            else if (changed) Right((math.min(tokens, capacity), math.max(ts, now), 0L))
            else Right((tokens, ts, frac))
          }
        result.fold(
          identity,
          (startTokens, startTs, startFrac) =>
            decide(call, key, fields, capacity, refill, period, cost, peek, now, startTokens, startTs, startFrac, argv(3))
        )
      }
    }

    private def decide(
      call: Call,
      key: Key,
      fields: VectorMap[Key, Value.Field],
      capacity: Long,
      refill: Long,
      period: Long,
      cost: Long,
      peek: Boolean,
      now: Long,
      startTokens: Long,
      startTs: Long,
      startFrac: Long,
      signature: Bytes
    ): Frame = {
      var tokens                = startTokens
      var ts                    = startTs
      var frac                  = startFrac
      if (now > ts) {
        val space = capacity - tokens
        if (space <= 0) frac = 0
        else {
          val need = space * period - frac
          if (now - ts >= Math.ceilDiv(need, refill)) {
            tokens = capacity
            frac = 0
          } else {
            val units  = (now - ts) * refill + frac
            val gained = Math.floorDiv(units, period)
            frac = units - gained * period
            tokens += gained
          }
        }
        ts = now
      }
      val catchup               = if (ts > now) ts - now else 0L
      val allowed               = tokens >= cost
      val retry                 = if (allowed) 0L else Math.ceilDiv((cost - tokens) * period - frac, refill)
      if (allowed && !peek) tokens -= cost
      val (timedCatchup, reset) =
        if (tokens < capacity) (catchup, Math.ceilDiv((capacity - tokens) * period - frac, refill)) else (0L, 0L)
      val ttl                   = math.max(1L, timedCatchup / 1000 + reset / 1000 + Math.ceilDiv(timedCatchup % 1000 + reset % 1000, 1000L))
      val updated               = Vector("t" -> tokens.toString, "ts" -> ts.toString, "f" -> frac.toString)
        .map((name, value) => name -> Bytes.utf8(value))
        .appended("v" -> signature)
        .foldLeft(fields)((acc, pair) => acc.updated(new Key(Bytes.utf8(pair._1)), Value.Field(pair._2, Entry.NoExpiry)))
      call.store.put(key, Value.Hash(updated), Math.addExact(call.now, ttl), call.now)
      Frame.Array(Vector(flag(allowed), int(tokens), int(timedCatchup), int(retry), int(reset)))
    }
  }
}

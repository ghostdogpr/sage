package sage.client.inmemory

import java.util.Locale

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

  // the arguments of a script call after the script reference: `numkeys key… arg…`
  final private case class Invocation(keys: Vector[Key], argv: Vector[Bytes])

  private val natives: Map[String, (Call, Invocation) => Frame] =
    Map(LockExecutor.compiled.sha -> lock, RateLimiter.compiled.sha -> RateLimitScript.run)

  val specs: Map[String, Spec] = Map(
    "EVAL"          -> Spec(-3, eval(_, bySha = false)),
    "EVALSHA"       -> Spec(-3, eval(_, bySha = true)),
    "SCRIPT LOAD"   -> Spec(2, scriptLoad),
    "SCRIPT EXISTS" -> Spec(-2, call => Frame.Array(call.args.map(arg => flag(call.store.scripts.contains(digest(arg)))))),
    "SCRIPT FLUSH"  -> Spec(-1, scriptFlush)
  )

  // a server accepts a digest in any case
  private def digest(arg: Bytes): String = arg.asUtf8String.toLowerCase(Locale.ROOT)

  private def sha1(body: Bytes): String =
    java.security.MessageDigest.getInstance("SHA-1").digest(body.toArray).iterator.map(b => f"${b & 0xff}%02x").mkString

  private val unsupported = Errors.err("the in-memory client cannot run this Lua script; it only runs the scripts of Sage's locks and rate limiters")

  // where a server's Lua interpreter fails with a runtime error
  private val failed = Errors.err("the script failed because a key or an argument it reads is missing or invalid")

  private def eval(call: Call, bySha: Boolean): Frame = {
    val sha  = if (bySha) digest(call.args(0)) else sha1(call.args(0))
    val keys = call.keys(1, call.bounded(1, Errors.err("Number of keys can't be negative"))(_ >= 0))
    if (bySha && !call.store.scripts.contains(sha)) Frame.SimpleError("NOSCRIPT No matching script. Please use EVAL.")
    else {
      if (!bySha) call.store.scripts(sha) = call.args(0)
      natives.get(sha).fold(unsupported)(_(call, Invocation(keys, call.args.drop(call.position))))
    }
  }

  // a server caches any script that compiles, so a script that is not Sage's fails only when EVALSHA runs it
  private def scriptLoad(call: Call): Frame = {
    val sha = sha1(call.args(0))
    call.store.scripts(sha) = call.args(0)
    bulk(sha)
  }

  private def scriptFlush(call: Call): Frame = {
    call.optional(0)(call.choice(_, "SYNC", "ASYNC")): Unit
    call.store.scripts.clear()
    ok
  }

  /**
    * The lock script of [[LockExecutor]]. `acquire` sets the key to the caller's token with the lease as its expiry, or extends the lease
    * when the caller already holds it. `renew` extends and `release` deletes the key, both only for the token that holds it.
    */
  private def lock(call: Call, invocation: Invocation): Frame =
    // the script checks the operation before it reads the key
    (invocation.argv, invocation.argv.lift(1).map(_.asUtf8String)) match {
      case (token +: _ +: lease, Some(operation @ ("acquire" | "renew" | "release"))) =>
        val key            = invocation.keys.headOption.getOrElse(throw new Reject(failed))
        def leaseEnd: Long =
          lease.headOption.flatMap(Numbers.parseLong).filter(_ > 0).fold(throw new Reject(Errors.invalidExpire("set")))(call.now + _)
        def holds: Boolean = call.string(key).exists(_.sameBytes(token))
        (operation, call.lookup(key)) match {
          case ("acquire", None)                          =>
            call.put(key, Value.Str(token), Ttl.At(leaseEnd))
            int(1L)
          case ("acquire" | "renew", Some(held)) if holds =>
            call.store.setExpiry(key, held, leaseEnd)
            int(1L)
          case ("release", Some(_)) if holds              =>
            call.store.delete(key, call.now): Unit
            int(1L)
          case _                                          => int(0L)
        }
      case _                                                                          => invalidLock
    }

  private val invalidLock = Frame.SimpleError("SAGE invalid lock operation")

  /**
    * A Scala port of [[RateLimiter.script]], a token bucket stored in a hash. The script uses Lua numbers, which are doubles, and limits
    * every value to 2^53 so that they stay exact; this port uses the same limits and computes with `Long` and `BigInt`, which gives the same
    * results. The comments of the Lua script explain each step.
    */
  private object RateLimitScript {

    private val MaxExact = BigInt(1L << 53)

    private def withinExact(text: String): Boolean = text.length < 16 || (text.length == 16 && text <= "9007199254740992")

    private def storedInteger(bytes: Bytes): Option[Long] =
      Some(bytes.asUtf8String).filter(text => text.nonEmpty && text.forall(_.isDigit) && withinExact(text)).flatMap(_.toLongOption)

    private def invalid(what: String): Frame.SimpleError = Frame.SimpleError(s"SAGE invalid rate-limit $what")

    def run(call: Call, invocation: Invocation): Frame =
      invocation match {
        case Invocation(keys, Vector(capacityText, refillText, periodText, version, costText, rest*)) =>
          val texts                              = Vector(capacityText, refillText, periodText, costText).map(_.asUtf8String)
          val (capacity, refill, period, cost)   = texts.map(_.toLongOption) match {
            case Vector(Some(capacity), Some(refill), Some(period), Some(cost))
                if capacity > 0 && refill >= 1 && period >= 1 && texts.forall(withinExact) && BigInt(capacity) * period <= MaxExact && cost >= 1 &&
                  cost <= capacity =>
              (capacity, refill, period, cost)
            case _ => throw new Reject(invalid("policy or cost"))
          }
          // the script checks the policy before it reads the key and the time
          val (key, injected, peekText)          = (keys, rest) match {
            case (key +: _, injected +: peekText +: _) => (key, injected, peekText)
            case _                                     => throw new Reject(failed)
          }
          val peek                               = peekText.asUtf8String == "1"
          val now                                =
            if (injected.length == 0) call.now * 1000L else injected.asUtf8String.toLongOption.getOrElse(throw new Reject(failed))
          val fields                             = call.typed[Value.Hash](key).fold(VectorMap.empty[Key, Bytes])(_.fields)
          def field(name: String): Option[Bytes] = fields.get(new Key(Bytes.utf8(name)))
          val state                              = Vector("t", "ts", "f", "v").map(field)
          val changed                            = !state(3).exists(_.sameBytes(version))
          var (tokens, ts, frac)                 =
            if (state.forall(_.isEmpty)) (capacity, now, 0L)
            else
              state.take(3).map(_.flatMap(storedInteger)) match {
                case Vector(Some(tokens), Some(ts), Some(_)) if changed                                                                    =>
                  (math.min(tokens, capacity), math.max(ts, now), 0L)
                case Vector(Some(tokens), Some(ts), Some(frac)) if tokens <= capacity && frac < period && (tokens < capacity || frac == 0) =>
                  (tokens, ts, frac)
                case _                                                                                                                     =>
                  throw new Reject(invalid("state"))
              }
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
          val catchup                            = if (ts > now) ts - now else 0L
          val allowed                            = tokens >= cost
          val retry                              = if (allowed) 0L else Math.ceilDiv((cost - tokens) * period - frac, refill)
          if (allowed && !peek) tokens -= cost
          val (timedCatchup, reset)              =
            if (tokens < capacity) (catchup, Math.ceilDiv((capacity - tokens) * period - frac, refill)) else (0L, 0L)
          val ttl                                = math.max(1L, timedCatchup / 1000 + reset / 1000 + Math.ceilDiv(timedCatchup % 1000 + reset % 1000, 1000L))
          val updated                            = Vector("t" -> tokens.toString, "ts" -> ts.toString, "f" -> frac.toString)
            .map((name, value) => name -> Bytes.utf8(value))
            .appended("v" -> version)
            .foldLeft(fields)((acc, pair) => acc.updated(new Key(Bytes.utf8(pair._1)), pair._2))
          call.put(key, Value.Hash(updated), Ttl.At(Math.addExact(call.now, ttl)))
          Frame.Array(Vector(flag(allowed), int(tokens), int(timedCatchup), int(retry), int(reset)))
        case _                                                                                        => failed
      }
  }
}

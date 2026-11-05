package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

/**
  * Parses the expiry times of commands. An amount that overflows when converted to milliseconds fails with the error Redis returns for
  * the command.
  */
private[inmemory] object Expiry {

  // converts the `EX`, `PX`, `EXAT` and `PXAT` options to an absolute expiry time, rejecting an amount that is not positive
  private def absolute(call: Call, option: String, bytes: Bytes): Ttl = {
    val amount = Numbers.parseLong(bytes).getOrElse(throw new Reject(Errors.notInteger))
    if (amount <= 0) throw new Reject(Errors.invalidExpire(call.name))
    Ttl.At(at(call, amount, relative = option.length == 2, scale = if (option.startsWith("E")) 1000L else 1L))
  }

  private val options: Set[String] = Set("EX", "PX", "EXAT", "PXAT")

  /**
    * The options of `SET` and `GETEX`. `mustExist` is `Some(false)` for NX, `Some(true)` for XX, and `None` without a condition.
    */
  final case class Write(mustExist: Option[Boolean], ttl: Ttl, get: Boolean) {
    def allows(exists: Boolean): Boolean = mustExist.forall(_ == exists)
  }

  /**
    * Parses `[NX | XX] [GET] [EX | PX | EXAT | PXAT time | KEEPTTL]` from `from` in any order, with GET only when `get` is true, or `[EX |
    * PX | EXAT | PXAT time | PERSIST]` when `persist` is true. As on a server, a repeated word is allowed and the last time wins, two
    * different expiry words or NX with XX are a syntax error, and the time is parsed after every option was read.
    */
  def write(call: Call, from: Int, get: Boolean = false, persist: Boolean = false): Write = {
    var mustExist = Option.empty[Boolean]
    var getting   = false
    var expiry    = Option.empty[String]
    var amount    = Bytes.empty
    val keep      = if (persist) "PERSIST" else "KEEPTTL"
    call.options(from) {
      case word @ ("NX" | "XX") if !persist && mustExist.forall(_ == (word == "XX")) => mustExist = Some(word == "XX")
      case word @ ("IFEQ" | "IFNE" | "IFDEQ" | "IFDNE") if get                       => call.unsupported(word)
      case "GET" if get                                                              => getting = true
      case word if word == keep && expiry.forall(_ == word)                          => expiry = Some(word)
      case word if options.contains(word) && expiry.forall(_ == word)                =>
        amount = call.next()
        expiry = Some(word)
      case _                                                                         => throw new Reject(Errors.syntax)
    }
    val ttl       = expiry.fold(if (persist) Ttl.Keep else Ttl.Clear) {
      case "KEEPTTL" => Ttl.Keep
      case "PERSIST" => Ttl.Clear
      case option    => absolute(call, option, amount)
    }
    Write(mustExist, ttl, getting)
  }

  // the expiry time that `EXPIRE` and its variants set, from an amount of seconds (`scale` 1000) or milliseconds (`scale` 1)
  def at(call: Call, amount: Long, relative: Boolean, scale: Long): Long =
    try {
      val scaled = Math.multiplyExact(amount, scale)
      if (relative) Math.addExact(call.now, scaled) else scaled
    } catch { case _: ArithmeticException => throw new Reject(Errors.invalidExpire(call.name)) }
}

private[inmemory] object StringCommands {
  import Replies.*

  val specs: Map[String, Spec] = Map(
    "APPEND"      -> Spec(3, append),
    "DECR"        -> Spec(2, call => incrBy(call, -1L)),
    "DECRBY"      -> Spec(3, decrBy),
    "GET"         -> Spec(2, call => optional(call.string(call.key(0)))),
    "GETDEL"      -> Spec(2, getDel),
    "GETEX"       -> Spec(-2, getEx),
    "INCR"        -> Spec(2, call => incrBy(call, 1L)),
    "INCRBY"      -> Spec(3, call => incrBy(call, call.long(1))),
    "INCRBYFLOAT" -> Spec(3, incrByFloat),
    "MGET"        -> Spec(-2, mGet),
    "MSET"        -> Spec(-3, mSet),
    "MSETNX"      -> Spec(-3, mSetNx),
    "SET"         -> Spec(-3, set),
    "STRLEN"      -> Spec(2, call => int(call.string(call.key(0)).fold(0L)(_.length.toLong)))
  )

  private def append(call: Call): Frame = {
    val key     = call.key(0)
    val current = call.string(key)
    val updated = Bytes.concat(current.toVector :+ call.args(1))
    call.put(key, Value.Str(updated))
    int(updated.length.toLong)
  }

  private def decrBy(call: Call): Frame = {
    val amount = call.long(1)
    if (amount == Long.MinValue) throw new Reject(Errors.err("decrement would overflow"))
    incrBy(call, -amount)
  }

  private def incrBy(call: Call, amount: Long): Frame = {
    val key     = call.key(0)
    val updated = Numbers.added(call.string(key), amount, Errors.notInteger)
    call.put(key, Value.Str(Bytes.utf8(updated.toString)))
    int(updated)
  }

  private def incrByFloat(call: Call): Frame = {
    val key  = call.key(0)
    val text = Numbers.addedFloat(call.string(key), call.double(1), Errors.notFloat)
    call.put(key, Value.Str(text))
    bulk(text)
  }

  private def mSetNx(call: Call): Frame = {
    val pairs   = call.pairs(0)
    val allowed = pairs.forall((key, _) => call.lookup(key).isEmpty)
    if (allowed) pairs.foreach((key, value) => call.put(key, Value.Str(value), Ttl.Clear))
    flag(allowed)
  }

  private def getDel(call: Call): Frame = {
    val key     = call.key(0)
    val current = call.string(key)
    if (current.isDefined) call.store.delete(key, call.now): Unit
    optional(current)
  }

  private def getEx(call: Call): Frame = {
    val key      = call.key(0)
    val ttl: Ttl = Expiry.write(call, 1, persist = true).ttl
    val current  = call.string(key)
    for {
      _     <- current
      entry <- call.lookup(key)
      // a time is a write even when it is unchanged, and PERSIST only when there was an expiry, as on a server
      if ttl != Ttl.Keep && (ttl != Ttl.Clear || entry.hasExpiry)
    } call.store.setExpiry(key, entry, ttl.resolve(entry.expiresAt))
    optional(current)
  }

  private def mGet(call: Call): Frame =
    Frame.Array(call.args.indices.toVector.map { i =>
      call.lookup(call.key(i)).map(_.value).collect { case Value.Str(bytes) => bulk(bytes) }.getOrElse(Frame.Null)
    })

  private def mSet(call: Call): Frame = {
    call.pairs(0).foreach((key, value) => call.put(key, Value.Str(value), Ttl.Clear))
    ok
  }

  private def set(call: Call): Frame = {
    val key      = call.key(0)
    val options  = Expiry.write(call, 2, get = true)
    val previous = if (options.get) call.string(key) else None
    val allowed  = options.allows(call.lookup(key).isDefined)
    if (allowed) call.put(key, Value.Str(call.args(1)), options.ttl)
    if (options.get) optional(previous) else if (allowed) ok else Frame.Null
  }
}

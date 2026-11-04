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
    * The options of `SET`, `MSETEX` and `GETEX`. `mustExist` is `Some(false)` for NX, `Some(true)` for XX, and `None` without a condition.
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

  // the NX, XX, GT and LT conditions; no expiry counts as never expiring, so GT never applies to it and LT always does
  enum Condition {
    case NX, XX, GT, LT

    def allows(current: Entry, at: Long): Boolean =
      this match {
        case NX => !current.hasExpiry
        case XX => current.hasExpiry
        case GT => current.hasExpiry && at > current.expiresAt
        case LT => !current.hasExpiry || at < current.expiresAt
      }
  }

  def condition(keyword: String): Option[Condition] = Condition.values.find(_.toString == keyword)
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
    "GETRANGE"    -> Spec(4, getRange),
    "INCR"        -> Spec(2, call => incrBy(call, 1L)),
    "INCRBY"      -> Spec(3, call => incrBy(call, call.long(1))),
    "INCRBYFLOAT" -> Spec(3, incrByFloat),
    "MGET"        -> Spec(-2, mGet),
    "MSET"        -> Spec(-3, mSet),
    "MSETEX"      -> Spec(-4, mSetEx),
    "MSETNX"      -> Spec(-3, call => setAll(call, call.pairs(0), Expiry.Write(Some(false), Ttl.Clear, get = false))),
    "SET"         -> Spec(-3, set),
    "SETRANGE"    -> Spec(4, setRange),
    "STRLEN"      -> Spec(2, call => int(call.string(call.key(0)).fold(0L)(_.length.toLong)))
  )

  // Redis limits a string to 512 MB by default
  private val MaxLength = 512L * 1024L * 1024L

  private def append(call: Call): Frame = {
    val key     = call.key(0)
    val current = call.string(key)
    checkLength(current.fold(0L)(_.length.toLong), call.args(1).length)
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

  // MSETEX numkeys key value … [NX | XX] [EX | PX | EXAT | PXAT time | KEEPTTL] writes every pair only when the condition holds for all keys
  private def mSetEx(call: Call): Frame = {
    val count = call.bounded(0, Errors.err("invalid numkeys value"))(n => n > 0 && n <= Int.MaxValue)
    if (count * 2 > call.args.length - 1) throw new Reject(Errors.err("wrong number of key-value pairs"))
    val pairs = Vector.tabulate(count.toInt)(i => (call.key(1 + i * 2), call.args(2 + i * 2)))
    setAll(call, pairs, Expiry.write(call, 1 + count.toInt * 2))
  }

  private def setAll(call: Call, pairs: Vector[(Key, Bytes)], options: Expiry.Write): Frame = {
    val allowed = pairs.forall((key, _) => options.allows(call.lookup(key).isDefined))
    if (allowed) pairs.foreach((key, value) => call.put(key, Value.Str(value), options.ttl))
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

  private def getRange(call: Call): Frame = {
    val start  = call.long(1)
    val end    = call.long(2)
    val bytes  = call.string(call.key(0)).getOrElse(Bytes.empty)
    val length = bytes.length.toLong
    // both positions are clamped to the string, so an end before the string selects its first byte unless both count from the end
    val from   = math.max(0L, if (start < 0) length + start else start)
    val to     = math.min(length - 1, math.max(0L, if (end < 0) length + end else end))
    if (from > to || start < 0 && end < 0 && start > end) bulk(Bytes.empty) else bulk(Bytes.wrap(bytes.toIArray.slice(from.toInt, to.toInt + 1)))
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

  private def setRange(call: Call): Frame = {
    val key     = call.key(0)
    val offset  = call.long(1)
    val value   = call.args(2)
    if (offset < 0) throw new Reject(Errors.err("offset is out of range"))
    val current = call.string(key)
    if (value.length == 0) int(current.fold(0L)(_.length.toLong))
    else {
      checkLength(offset, value.length)
      val base    = current.getOrElse(Bytes.empty).toArray
      val updated = java.util.Arrays.copyOf(base, math.max(base.length, offset.toInt + value.length))
      System.arraycopy(value.toArray, 0, updated, offset.toInt, value.length)
      call.put(key, Value.Str(Bytes.wrap(IArray.unsafeFromArray(updated))))
      int(updated.length.toLong)
    }
  }

  // compares without adding, so that an offset near Long.MaxValue cannot overflow
  private def checkLength(start: Long, added: Long): Unit =
    if (start > MaxLength - added) throw new Reject(Errors.err("string exceeds maximum allowed size (proto-max-bulk-len)"))
}

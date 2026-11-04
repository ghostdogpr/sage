package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

/**
  * Converts the `EX`, `PX`, `EXAT` and `PXAT` options to an absolute expiry time in milliseconds. A value that is not positive, or that
  * overflows when converted, is rejected with the error Redis returns for the command.
  */
private[inmemory] object Expiry {

  def absolute(call: Call, option: String, index: Int): Long = {
    val amount  = call.long(index)
    val invalid = new Reject(Errors.invalidExpire(call.name))
    if (amount <= 0) throw invalid
    try
      option match {
        case "EX"   => Math.addExact(call.now, Math.multiplyExact(amount, 1000L))
        case "PX"   => Math.addExact(call.now, amount)
        case "EXAT" => Math.multiplyExact(amount, 1000L)
        case _      => amount
      }
    catch { case _: ArithmeticException => throw invalid }
  }

  val options: Set[String] = Set("EX", "PX", "EXAT", "PXAT")
}

private[inmemory] object StringCommands {
  import Replies.*

  val specs: Map[String, Spec] = Map(
    "APPEND"      -> Spec(3, append),
    "DECR"        -> Spec(2, call => incrBy(call, -1L)),
    "DECRBY"      -> Spec(3, decrBy),
    "DELEX"       -> Spec(-2, delex),
    "DELIFEQ"     -> Spec(3, delIfEq),
    "GET"         -> Spec(2, call => optional(call.string(call.key(0)))),
    "GETDEL"      -> Spec(2, getDel),
    "GETEX"       -> Spec(-2, getEx),
    "GETRANGE"    -> Spec(4, getRange),
    "INCR"        -> Spec(2, call => incrBy(call, 1L)),
    "INCRBY"      -> Spec(3, call => incrBy(call, call.long(1))),
    "INCRBYFLOAT" -> Spec(3, incrByFloat),
    "INCREX"      -> Spec(-4, incrEx),
    "LCS"         -> Spec(-3, Lcs.run),
    "MGET"        -> Spec(-2, mGet),
    "MSET"        -> Spec(-3, mSet),
    "MSETEX"      -> Spec(-4, mSetEx),
    "MSETNX"      -> Spec(-3, mSetNx),
    "SET"         -> Spec(-3, set),
    "SETRANGE"    -> Spec(4, setRange),
    "STRLEN"      -> Spec(2, call => int(call.string(call.key(0)).fold(0L)(_.length.toLong)))
  )

  // Redis limits a string to 512 MB by default
  private val MaxLength = 512L * 1024L * 1024L

  private def append(call: Call): Frame = {
    val key     = call.key(0)
    val current = call.string(key)
    val updated = Bytes.concat(current.toVector :+ call.args(1))
    checkLength(updated.length.toLong)
    write(call, key, updated)
    int(updated.length.toLong)
  }

  private def decrBy(call: Call): Frame = {
    val amount = call.long(1)
    if (amount == Long.MinValue) throw new Reject(Errors.err("decrement would overflow"))
    incrBy(call, -amount)
  }

  private def incrBy(call: Call, amount: Long): Frame = {
    val key     = call.key(0)
    val current = call.string(key).fold(0L)(bytes => Numbers.parseLong(bytes).getOrElse(throw new Reject(Errors.notInteger)))
    val updated =
      try Math.addExact(current, amount)
      catch { case _: ArithmeticException => throw new Reject(Errors.err("increment or decrement would overflow")) }
    write(call, key, Bytes.utf8(updated.toString))
    int(updated)
  }

  private def incrByFloat(call: Call): Frame = {
    val key       = call.key(0)
    val increment = Numbers.parseDouble(call.args(1)).getOrElse(throw new Reject(Errors.notFloat))
    val current   = call.string(key).fold(0.0)(bytes => Numbers.parseDouble(bytes).getOrElse(throw new Reject(Errors.notFloat)))
    val updated   = current + increment
    if (updated.isNaN || updated.isInfinite) throw new Reject(Errors.err("increment would produce NaN or Infinity"))
    val text      = Bytes.utf8(Numbers.formatDouble(updated))
    write(call, key, text)
    bulk(text)
  }

  private def delIfEq(call: Call): Frame = {
    val key = call.key(0)
    call.string(key) match {
      case Some(current) if current.sameBytes(call.args(1)) => flag(call.store.delete(key, call.now))
      case _                                                => flag(false)
    }
  }

  // DELEX without a condition deletes a key of any type. The digest conditions need the server's XXH3 digest, which DIGEST would also
  // need, so the in-memory client does not support them.
  private def delex(call: Call): Frame = {
    val key = call.key(0)
    call.args.drop(1).map(_.asUtf8String.toUpperCase(java.util.Locale.ROOT)) match {
      case Vector()                                 => flag(call.store.delete(key, call.now))
      case Vector(condition @ ("IFEQ" | "IFNE"), _) =>
        call.string(key) match {
          case Some(current) if current.sameBytes(call.args(2)) == (condition == "IFEQ") => flag(call.store.delete(key, call.now))
          case _                                                                         => flag(false)
        }
      case Vector("IFDEQ" | "IFDNE", _)             =>
        throw new Reject(Errors.err("the in-memory client does not support digest conditions"))
      case _                                        => throw new Reject(Errors.syntax)
    }
  }

  // MSETEX numkeys key value … [NX | XX] [EX | PX | EXAT | PXAT time | KEEPTTL] writes every pair only when the condition holds for all keys
  private def mSetEx(call: Call): Frame = {
    val count                   = call.long(0)
    if (count <= 0 || 1 + count * 2 > call.args.length) throw new Reject(Errors.syntax)
    val pairs                   = Vector.tabulate(count.toInt)(i => (call.key(1 + i * 2), call.args(2 + i * 2)))
    var condition               = ""
    var keepTtl                 = false
    var expiresAt: Option[Long] = None
    var i                       = 1 + count.toInt * 2
    while (i < call.args.length) {
      call.keyword(i) match {
        case word @ ("NX" | "XX") if condition.isEmpty                                                              => condition = word
        case "KEEPTTL" if !keepTtl && expiresAt.isEmpty                                                             => keepTtl = true
        case option if Expiry.options.contains(option) && expiresAt.isEmpty && !keepTtl && i + 1 < call.args.length =>
          i += 1
          expiresAt = Some(Expiry.absolute(call, option, i))
        case _                                                                                                      => throw new Reject(Errors.syntax)
      }
      i += 1
    }
    val existing                = pairs.map((key, _) => call.lookup(key))
    val allowed                 = condition match {
      case "NX" => existing.forall(_.isEmpty)
      case "XX" => existing.forall(_.isDefined)
      case _    => true
    }
    if (allowed)
      pairs.zip(existing).foreach { case ((key, value), entry) =>
        val expiry = expiresAt.getOrElse(if (keepTtl) entry.fold(Entry.NoExpiry)(_.expiresAt) else Entry.NoExpiry)
        call.store.put(key, Value.Str(value), expiry, call.now)
      }
    flag(allowed)
  }

  /**
    * INCREX key BYINT | BYFLOAT increment [SATURATE] [LBOUND min] [UBOUND max] [EX | PX | EXAT | PXAT time [ENX] | PERSIST]. The reply is
    * the new value and the change that was applied. An increment that would leave the bounds changes nothing and applies 0, unless
    * SATURATE caps the value at the bound. The expiry options apply only when the value is written, and ENX sets the expiry only when the
    * key has none.
    */
  private def incrEx(call: Call): Frame = {
    val key                            = call.key(0)
    val integral                       = call.keyword(1) match {
      case "BYINT"   => true
      case "BYFLOAT" => false
      case _         => throw new Reject(Errors.syntax)
    }
    def number(index: Int): BigDecimal =
      if (integral) BigDecimal(call.long(index)) else BigDecimal(call.double(index))
    val increment                      = number(2)
    var saturate                       = false
    var lower                          = Option.empty[BigDecimal]
    var upper                          = Option.empty[BigDecimal]
    var expiry                         = Option.empty[Long]
    var onlyIfNoTtl                    = false
    var i                              = 3
    while (i < call.args.length) {
      call.keyword(i) match {
        case "SATURATE"                                                                              => saturate = true
        case "LBOUND" if i + 1 < call.args.length                                                    => i += 1; lower = Some(number(i))
        case "UBOUND" if i + 1 < call.args.length                                                    => i += 1; upper = Some(number(i))
        case "PERSIST" if expiry.isEmpty                                                             => expiry = Some(Entry.NoExpiry)
        case "ENX"                                                                                   => onlyIfNoTtl = true
        case option if Expiry.options.contains(option) && expiry.isEmpty && i + 1 < call.args.length =>
          i += 1
          expiry = Some(Expiry.absolute(call, option, i))
        case _                                                                                       => throw new Reject(Errors.syntax)
      }
      i += 1
    }
    val entry                          = call.lookup(key)
    val current                        = call.string(key).fold(BigDecimal(0)) { bytes =>
      if (integral) BigDecimal(Numbers.parseLong(bytes).getOrElse(throw new Reject(Errors.notInteger)))
      else BigDecimal(Numbers.parseDouble(bytes).getOrElse(throw new Reject(Errors.notFloat)))
    }
    val target                         = current + increment
    val inRange                        = lower.forall(target >= _) && upper.forall(target <= _)
    val updated                        =
      if (inRange) target
      else if (saturate) upper.filter(target > _).orElse(lower.filter(target < _)).getOrElse(target)
      else current
    if (integral && !updated.isValidLong) throw new Reject(Errors.err("increment or decrement would overflow"))
    val applied                        = updated - current
    if (inRange || (saturate && applied != 0)) {
      val keep = entry.fold(Entry.NoExpiry)(_.expiresAt)
      val next = expiry match {
        case Some(at) if !(onlyIfNoTtl && entry.exists(_.hasExpiry)) => at
        case _                                                       => keep
      }
      val text = if (integral) updated.toLong.toString else Numbers.formatDouble(updated.toDouble)
      call.store.put(key, Value.Str(Bytes.utf8(text)), next, call.now)
    }
    if (integral) Frame.Array(Vector(int(updated.toLong), int(applied.toLong)))
    else Frame.Array(Vector(Frame.Double(updated.toDouble), Frame.Double(applied.toDouble)))
  }

  private def getDel(call: Call): Frame = {
    val key     = call.key(0)
    val current = call.string(key)
    if (current.isDefined) call.store.delete(key, call.now): Unit
    optional(current)
  }

  private def getEx(call: Call): Frame = {
    val key                  = call.key(0)
    // None leaves the expiry unchanged; Some(Entry.NoExpiry) removes it
    val expiry: Option[Long] = call.args.length match {
      case 1                                             => None
      case 2 if call.keyword(1) == "PERSIST"             => Some(Entry.NoExpiry)
      case 3 if Expiry.options.contains(call.keyword(1)) => Some(Expiry.absolute(call, call.keyword(1), 2))
      case _                                             => throw new Reject(Errors.syntax)
    }
    val current              = call.string(key)
    for {
      _      <- current
      entry  <- call.lookup(key)
      update <- expiry
    }
      if (update != Entry.NoExpiry && update <= call.now) call.store.delete(key, call.now): Unit
      else if (update != entry.expiresAt) call.store.setExpiry(key, entry, update)
    optional(current)
  }

  private def getRange(call: Call): Frame = {
    val bytes  = call.string(call.key(0)).getOrElse(Bytes.empty)
    val length = bytes.length.toLong
    val start  = call.long(1)
    val end    = call.long(2)
    val from   = math.max(0L, if (start < 0) length + start else start)
    val to     = math.min(length - 1, math.max(0L, if (end < 0) length + end else end))
    if (length == 0 || from > to) bulk(Bytes.empty)
    else bulk(Bytes.wrap(bytes.toIArray.slice(from.toInt, to.toInt + 1)))
  }

  private def mGet(call: Call): Frame =
    Frame.Array(call.args.indices.toVector.map { i =>
      call.lookup(call.key(i)).map(_.value).collect { case Value.Str(bytes) => bulk(bytes) }.getOrElse(Frame.Null)
    })

  private def mSet(call: Call): Frame = {
    if (call.args.length % 2 != 0) throw new Reject(Errors.arity(call.name))
    pairs(call).foreach((key, value) => call.store.put(key, Value.Str(value), Entry.NoExpiry, call.now))
    ok
  }

  private def mSetNx(call: Call): Frame = {
    if (call.args.length % 2 != 0) throw new Reject(Errors.arity(call.name))
    val all = pairs(call)
    if (all.exists((key, _) => call.lookup(key).isDefined)) flag(false)
    else {
      all.foreach((key, value) => call.store.put(key, Value.Str(value), Entry.NoExpiry, call.now))
      flag(true)
    }
  }

  private def pairs(call: Call): Vector[(Key, Bytes)] =
    call.args.grouped(2).map(pair => (new Key(pair(0)), pair(1))).toVector

  private def set(call: Call): Frame = {
    val key                     = call.key(0)
    var condition: String       = ""
    var get                     = false
    var keepTtl                 = false
    var expiresAt: Option[Long] = None
    var i                       = 2
    while (i < call.args.length) {
      call.keyword(i) match {
        case word @ ("NX" | "XX") if condition.isEmpty  => condition = word
        case "GET" if !get                              => get = true
        case "KEEPTTL" if !keepTtl && expiresAt.isEmpty => keepTtl = true
        case option
            if Expiry.options.contains(option) && expiresAt.isEmpty && !keepTtl &&
              i + 1 < call.args.length =>
          i += 1
          expiresAt = Some(Expiry.absolute(call, option, i))
        case _                                          => throw new Reject(Errors.syntax)
      }
      i += 1
    }
    val existing                = call.lookup(key)
    val previous                = if (get) call.string(key) else None
    val skip                    = (condition == "NX" && existing.isDefined) || (condition == "XX" && existing.isEmpty)
    if (!skip) {
      val expiry = expiresAt.getOrElse(if (keepTtl) existing.fold(Entry.NoExpiry)(_.expiresAt) else Entry.NoExpiry)
      call.store.put(key, Value.Str(call.args(1)), expiry, call.now)
    }
    if (get) optional(previous) else if (skip) Frame.Null else ok
  }

  private def setRange(call: Call): Frame = {
    val key     = call.key(0)
    val offset  = call.long(1)
    val value   = call.args(2)
    if (offset < 0) throw new Reject(Errors.err("offset is out of range"))
    val current = call.string(key)
    if (value.length == 0) int(current.fold(0L)(_.length.toLong))
    else {
      checkLength(offset + value.length)
      val base    = current.getOrElse(Bytes.empty).toArray
      val updated = java.util.Arrays.copyOf(base, math.max(base.length, offset.toInt + value.length))
      System.arraycopy(value.toArray, 0, updated, offset.toInt, value.length)
      write(call, key, Bytes.wrap(IArray.unsafeFromArray(updated)))
      int(updated.length.toLong)
    }
  }

  private def checkLength(length: Long): Unit =
    if (length > MaxLength) throw new Reject(Errors.err("string exceeds maximum allowed size (proto-max-bulk-len)"))

  // Changing a string's contents keeps its expiry, as APPEND, INCR and SETRANGE do on a real server.
  private def write(call: Call, key: Key, bytes: Bytes): Unit =
    call.store.put(key, Value.Str(bytes), call.lookup(key).fold(Entry.NoExpiry)(_.expiresAt), call.now)
}

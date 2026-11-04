package sage.client.inmemory

import scala.util.Random

import sage.Bytes
import sage.protocol.Frame

private[inmemory] object KeyCommands {
  import Replies.*

  val specs: Map[String, Spec] = Map(
    "COPY"        -> Spec(-3, copy),
    "DEL"         -> Spec(-2, delete),
    "EXISTS"      -> Spec(-2, call => int(call.args.indices.count(i => call.lookup(call.key(i)).isDefined).toLong)),
    "EXPIRE"      -> Spec(-3, call => expire(call, relative = true, scale = 1000L)),
    "EXPIREAT"    -> Spec(-3, call => expire(call, relative = false, scale = 1000L)),
    "EXPIRETIME"  -> Spec(2, call => ttl(call, absolute = true, millis = false)),
    "KEYS"        -> Spec(2, keys),
    "PERSIST"     -> Spec(2, persist),
    "PEXPIRE"     -> Spec(-3, call => expire(call, relative = true, scale = 1L)),
    "PEXPIREAT"   -> Spec(-3, call => expire(call, relative = false, scale = 1L)),
    "PEXPIRETIME" -> Spec(2, call => ttl(call, absolute = true, millis = true)),
    "PTTL"        -> Spec(2, call => ttl(call, absolute = false, millis = true)),
    "RANDOMKEY"   -> Spec(1, randomKey),
    "RENAME"      -> Spec(3, call => rename(call, onlyIfAbsent = false)),
    "RENAMENX"    -> Spec(3, call => rename(call, onlyIfAbsent = true)),
    "SCAN"        -> Spec(-2, scan),
    "SORT"        -> Spec(-2, Sort.run),
    "SORT_RO"     -> Spec(-2, Sort.run),
    "TOUCH"       -> Spec(-2, call => int(call.args.indices.count(i => call.lookup(call.key(i)).isDefined).toLong)),
    "TTL"         -> Spec(2, call => ttl(call, absolute = false, millis = false)),
    "TYPE"        -> Spec(2, call => Frame.SimpleString(call.lookup(call.key(0)).fold("none")(_.value.typeName))),
    "UNLINK"      -> Spec(-2, delete)
  )

  private def copy(call: Call): Frame = {
    val source      = call.key(0)
    val destination = call.key(1)
    val replace     = call.args.drop(2) match {
      case Vector()                                                      => false
      case Vector(word) if word.asUtf8String.equalsIgnoreCase("REPLACE") => true
      case _                                                             => throw new Reject(Errors.syntax)
    }
    if (source == destination) throw new Reject(Errors.err("source and destination objects are the same"))
    call.lookup(source) match {
      case None                                                      => flag(false)
      case Some(_) if !replace && call.lookup(destination).isDefined => flag(false)
      case Some(entry)                                               =>
        call.store.put(destination, entry.value, entry.expiresAt, call.now)
        flag(true)
    }
  }

  private def delete(call: Call): Frame = int(call.args.indices.count(i => call.store.delete(call.key(i), call.now)).toLong)

  private def expire(call: Call, relative: Boolean, scale: Long): Frame = {
    val key        = call.key(0)
    val amount     = call.long(1)
    val conditions = call.args.drop(2).map(_.asUtf8String.toUpperCase(java.util.Locale.ROOT)).toSet
    if (!conditions.subsetOf(Set("NX", "XX", "GT", "LT"))) throw new Reject(Errors.err(s"Unsupported option ${conditions.mkString(" ")}"))
    if (conditions.contains("NX") && conditions.size > 1)
      throw new Reject(Errors.err("NX and XX, GT or LT options at the same time are not compatible"))
    if (conditions.contains("GT") && conditions.contains("LT"))
      throw new Reject(Errors.err("GT and LT options at the same time are not compatible"))
    val at         =
      try {
        val scaled = Math.multiplyExact(amount, scale)
        if (relative) Math.addExact(call.now, scaled) else scaled
      } catch { case _: ArithmeticException => throw new Reject(Errors.invalidExpire(call.name)) }
    call.lookup(key) match {
      case None        => flag(false)
      case Some(entry) =>
        // a key without an expiry counts as never expiring, so GT never applies to it and LT always does
        val current = if (entry.hasExpiry) entry.expiresAt else Long.MaxValue
        val allowed =
          (!conditions.contains("NX") || !entry.hasExpiry) &&
            (!conditions.contains("XX") || entry.hasExpiry) &&
            (!conditions.contains("GT") || at > current) &&
            (!conditions.contains("LT") || at < current)
        if (!allowed) flag(false)
        else {
          if (at <= call.now) call.store.delete(key, call.now): Unit
          else call.store.setExpiry(key, entry, at)
          flag(true)
        }
    }
  }

  private def ttl(call: Call, absolute: Boolean, millis: Boolean): Frame =
    call.lookup(call.key(0)) match {
      case None                            => int(-2L)
      case Some(entry) if !entry.hasExpiry => int(-1L)
      case Some(entry)                     =>
        val amount = if (absolute) entry.expiresAt else entry.expiresAt - call.now
        int(if (millis) amount else (amount + 500L) / 1000L)
    }

  private def keys(call: Call): Frame = {
    val pattern = call.args(0)
    Frame.Array(call.store.live(call.now).collect { case (key, _) if Glob.matches(pattern, key.bytes) => bulk(key.bytes) })
  }

  private def persist(call: Call): Frame = {
    val key = call.key(0)
    call.lookup(key) match {
      case Some(entry) if entry.hasExpiry =>
        call.store.setExpiry(key, entry, Entry.NoExpiry)
        flag(true)
      case _                              => flag(false)
    }
  }

  private def randomKey(call: Call): Frame = {
    val all = call.store.live(call.now)
    if (all.isEmpty) Frame.Null else bulk(all(Random.nextInt(all.length))._1.bytes)
  }

  private def rename(call: Call, onlyIfAbsent: Boolean): Frame = {
    val source      = call.key(0)
    val destination = call.key(1)
    call.lookup(source) match {
      case None                                                          => throw new Reject(Errors.noSuchKey)
      case Some(_) if source == destination                              => if (onlyIfAbsent) flag(false) else ok
      case Some(_) if onlyIfAbsent && call.lookup(destination).isDefined => flag(false)
      case Some(entry)                                                   =>
        call.store.delete(source, call.now): Unit
        call.store.delete(destination, call.now): Unit
        call.store.put(destination, entry.value, entry.expiresAt, call.now)
        if (onlyIfAbsent) flag(true) else ok
    }
  }

  // The cursor is the creation sequence number of the next key to visit. Keys are visited in creation order, so a key that exists for the
  // whole iteration is returned exactly once, and a key created during the iteration may or may not be returned, as Redis allows.
  private def scan(call: Call): Frame = {
    val cursor       = Numbers.parseLong(call.args(0)).filter(_ >= 0).getOrElse(throw new Reject(Errors.err("invalid cursor")))
    var pattern      = Option.empty[Bytes]
    var count        = 10L
    var ofType       = Option.empty[String]
    var i            = 1
    while (i < call.args.length) {
      if (i + 1 >= call.args.length) throw new Reject(Errors.syntax)
      call.keyword(i) match {
        case "MATCH" => pattern = Some(call.args(i + 1))
        case "COUNT" =>
          count = call.long(i + 1)
          if (count < 1) throw new Reject(Errors.syntax)
        case "TYPE"  => ofType = Some(call.text(i + 1).toLowerCase(java.util.Locale.ROOT))
        case _       => throw new Reject(Errors.syntax)
      }
      i += 2
    }
    val remaining    = call.store.live(call.now).filter(_._2.seq >= cursor).sortBy(_._2.seq)
    val (page, rest) = remaining.splitAt(math.min(count, Int.MaxValue.toLong).toInt)
    val next         = rest.headOption.fold(0L)(_._2.seq)
    val items        = page.collect {
      case (key, entry) if pattern.forall(Glob.matches(_, key.bytes)) && ofType.forall(_ == entry.value.typeName) => bulk(key.bytes)
    }
    Frame.Array(Vector(bulk(next.toString), Frame.Array(items)))
  }
}

private[inmemory] object ServerCommands {
  import Replies.*

  val specs: Map[String, Spec] = Map(
    "DBSIZE"   -> Spec(1, call => int(call.store.live(call.now).length.toLong)),
    "ECHO"     -> Spec(2, call => bulk(call.args(0))),
    "FLUSHALL" -> Spec(-1, flush),
    "FLUSHDB"  -> Spec(-1, flush),
    "PING"     -> Spec(-1, ping),
    "TIME"     -> Spec(1, call => Frame.Array(Vector(bulk((call.now / 1000L).toString), bulk((call.now % 1000L * 1000L).toString))))
  )

  private def flush(call: Call): Frame =
    call.args.map(_.asUtf8String.toUpperCase(java.util.Locale.ROOT)) match {
      case Vector() | Vector("SYNC") | Vector("ASYNC") =>
        call.store.clear()
        ok
      case _                                           => throw new Reject(Errors.syntax)
    }

  private def ping(call: Call): Frame =
    call.args match {
      case Vector()        => Frame.SimpleString("PONG")
      case Vector(message) => bulk(message)
      case _               => throw new Reject(Errors.arity(call.name))
    }
}

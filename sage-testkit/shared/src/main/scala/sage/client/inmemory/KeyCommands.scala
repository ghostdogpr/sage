package sage.client.inmemory

import sage.protocol.Frame

private[inmemory] object KeyCommands {
  import Replies.*

  val specs: Map[String, Spec] = Map(
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
    "RENAME"      -> Spec(3, rename),
    "SCAN"        -> Spec(-2, scan),
    "TOUCH"       -> Spec(-2, call => int(call.args.indices.count(i => call.lookup(call.key(i)).isDefined).toLong)),
    "TTL"         -> Spec(2, call => ttl(call, absolute = false, millis = false)),
    "TYPE"        -> Spec(2, call => Frame.SimpleString(call.lookup(call.key(0)).fold("none")(_.value.typeName))),
    "UNLINK"      -> Spec(-2, delete)
  )

  private def delete(call: Call): Frame = int(call.args.indices.count(i => call.store.delete(call.key(i), call.now)).toLong)

  private def expire(call: Call, relative: Boolean, scale: Long): Frame = {
    val key = call.key(0)
    if (call.args.length > 2) call.unsupported(call.keyword(2))
    val at  = Expiry.at(call, call.long(1), relative, scale)
    call.lookup(key) match {
      case None        => flag(false)
      case Some(entry) =>
        if (at <= call.now) call.store.delete(key, call.now): Unit
        else call.store.setExpiry(key, entry, at)
        flag(true)
    }
  }

  // a server rounds seconds to the nearest
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

  private def rename(call: Call): Frame = {
    val source      = call.key(0)
    val destination = call.key(1)
    call.lookup(source) match {
      case None                             => throw new Reject(Errors.noSuchKey)
      case Some(_) if source == destination => ok
      case Some(entry)                      =>
        call.store.delete(source, call.now): Unit
        call.put(destination, entry.value, Ttl.At(entry.expiresAt))
        ok
    }
  }

  // The cursor is the creation sequence number of the next key to visit. Keys are visited in creation order, so a key that exists for the
  // whole iteration is returned exactly once, and a key created during the iteration may or may not be returned, as Redis allows.
  private def scan(call: Call): Frame = {
    val cursor       = Scanning.cursor(call, 0)
    val options      = Scanning.options(call, 1, allowType = true)
    val remaining    = call.store.live(call.now).filter(_._2.seq >= cursor).sortBy(_._2.seq)
    val (page, rest) = remaining.splitAt(Numbers.size(options.count))
    val next         = rest.headOption.fold(0L)(_._2.seq)
    val items        = page.collect {
      case (key, entry) if options.matches(key.bytes) && options.ofType.forall(_ == entry.value.typeName) => bulk(key.bytes)
    }
    Frame.Array(Vector(bulk(next.toString), Frame.Array(items)))
  }
}

private[inmemory] object ServerCommands {
  import Replies.*

  val specs: Map[String, Spec] = Map(
    "DBSIZE"   -> Spec(1, call => int(call.store.live(call.now).length.toLong)),
    "FLUSHALL" -> Spec(-1, flush),
    "FLUSHDB"  -> Spec(-1, flush),
    "PING"     -> Spec(-1, ping),
    "TIME"     -> Spec(1, call => Frame.Array(Vector(bulk((call.now / 1000L).toString), bulk((call.now % 1000L * 1000L).toString))))
  )

  private def flush(call: Call): Frame = {
    call.optional(0)(call.choice(_, "SYNC", "ASYNC")): Unit
    call.store.clear()
    ok
  }

  private def ping(call: Call): Frame =
    call.args match {
      case Vector()        => Frame.SimpleString("PONG")
      case Vector(message) => bulk(message)
      case _               => throw new Reject(Errors.arity(call.name))
    }
}

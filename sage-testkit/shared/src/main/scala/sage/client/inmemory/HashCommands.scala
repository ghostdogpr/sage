package sage.client.inmemory

import scala.collection.immutable.VectorMap
import scala.util.Random

import sage.Bytes
import sage.protocol.Frame

private[inmemory] object HashCommands {
  import Replies.*
  import Value.{Field, Hash}

  val specs: Map[String, Spec] = Map(
    "HDEL"         -> Spec(-3, hDel),
    "HEXISTS"      -> Spec(3, call => flag(call.hash(call.key(0)).exists(_.fields.contains(call.key(1))))),
    "HEXPIRE"      -> Spec(-6, call => hExpire(call, relative = true, scale = 1000L)),
    "HEXPIREAT"    -> Spec(-6, call => hExpire(call, relative = false, scale = 1000L)),
    "HEXPIRETIME"  -> Spec(-5, call => hTtl(call, absolute = true, millis = false)),
    "HGET"         -> Spec(3, call => optional(field(call, call.key(0), call.key(1)))),
    "HGETALL"      -> Spec(2, hGetAll),
    "HGETDEL"      -> Spec(-5, hGetDel),
    "HGETEX"       -> Spec(-5, hGetEx),
    "HINCRBY"      -> Spec(4, hIncrBy),
    "HINCRBYFLOAT" -> Spec(4, hIncrByFloat),
    "HKEYS"        -> Spec(2, call => keys(fieldsOf(call).keys)),
    "HLEN"         -> Spec(2, call => int(fieldsOf(call).size.toLong)),
    "HMGET"        -> Spec(-3, hmGet),
    "HPERSIST"     -> Spec(-5, hPersist),
    "HPEXPIRE"     -> Spec(-6, call => hExpire(call, relative = true, scale = 1L)),
    "HPEXPIREAT"   -> Spec(-6, call => hExpire(call, relative = false, scale = 1L)),
    "HPEXPIRETIME" -> Spec(-5, call => hTtl(call, absolute = true, millis = true)),
    "HPTTL"        -> Spec(-5, call => hTtl(call, absolute = false, millis = true)),
    "HRANDFIELD"   -> Spec(-2, hRandField),
    "HSCAN"        -> Spec(-3, hScan),
    "HSET"         -> Spec(-4, hSet),
    "HSETEX"       -> Spec(-6, hSetEx),
    "HSETNX"       -> Spec(4, hSetNx),
    "HSTRLEN"      -> Spec(3, call => int(field(call, call.key(0), call.key(1)).fold(0L)(_.length.toLong))),
    "HTTL"         -> Spec(-5, call => hTtl(call, absolute = false, millis = false)),
    "HVALS"        -> Spec(2, call => bulks(fieldsOf(call).values.map(_.value)))
  )

  private def fieldsOf(call: Call): VectorMap[Key, Field] = call.hash(call.key(0)).fold(VectorMap.empty[Key, Field])(_.fields)

  private def field(call: Call, key: Key, name: Key): Option[Bytes] = call.hash(key).flatMap(_.fields.get(name)).map(_.value)

  private def store(call: Call, key: Key, fields: VectorMap[Key, Field]): Unit = call.replace(key, Hash(fields))

  // Parses the `FIELDS numfields field…` block that starts at `index`. With `withValues`, each field is followed by its value.
  private def fieldsBlock(call: Call, index: Int, withValues: Boolean): Vector[(Key, Bytes)] = {
    if (index >= call.args.length || call.keyword(index) != "FIELDS") throw new Reject(Errors.syntax)
    if (index + 1 >= call.args.length) throw new Reject(Errors.syntax)
    val count = call.long(index + 1)
    val width = if (withValues) 2 else 1
    if (count <= 0) throw new Reject(Errors.err("Parameter `numFields` should be greater than 0"))
    if (call.args.length - index - 2 != count * width)
      throw new Reject(Errors.err("The `numfields` parameter must match the number of arguments"))
    Vector.tabulate(count.toInt) { i =>
      val at = index + 2 + i * width
      (call.key(at), if (withValues) call.args(at + 1) else Bytes.empty)
    }
  }

  private def hDel(call: Call): Frame = {
    val key     = call.key(0)
    val fields  = fieldsOf(call)
    val names   = call.args.indices.drop(1).map(call.key).distinct
    val removed = names.count(fields.contains)
    if (removed > 0) store(call, key, fields -- names)
    int(removed.toLong)
  }

  private def hGetAll(call: Call): Frame =
    Frame.Map(fieldsOf(call).iterator.map((name, field) => bulk(name.bytes) -> bulk(field.value)).toVector)

  private def hmGet(call: Call): Frame = {
    val fields = fieldsOf(call)
    Frame.Array(call.args.indices.drop(1).toVector.map(i => optional(fields.get(call.key(i)).map(_.value))))
  }

  private def hGetDel(call: Call): Frame = {
    val key    = call.key(0)
    val names  = fieldsBlock(call, 1, withValues = false).map(_._1)
    val fields = fieldsOf(call)
    val values = names.map(name => fields.get(name).map(_.value))
    if (names.exists(fields.contains)) store(call, key, fields -- names)
    Frame.Array(values.map(optional))
  }

  private def hGetEx(call: Call): Frame = {
    val key                = call.key(0)
    val (expiry, fieldsAt) = call.keyword(1) match {
      case "PERSIST"                                 => (Some(Entry.NoExpiry), 2)
      case option if Expiry.options.contains(option) => (Some(Expiry.absolute(call, option, 2)), 3)
      case _                                         => (None, 1)
    }
    val names              = fieldsBlock(call, fieldsAt, withValues = false).map(_._1)
    val fields             = fieldsOf(call)
    val values             = names.map(name => fields.get(name).map(_.value))
    expiry.foreach { at =>
      val present = names.filter(fields.contains)
      if (present.nonEmpty) {
        val updated =
          if (at != Entry.NoExpiry && at <= call.now) fields -- present
          else present.foldLeft(fields)((acc, name) => acc.updated(name, acc(name).copy(expiresAt = at)))
        store(call, key, updated)
      }
    }
    Frame.Array(values.map(optional))
  }

  private def hIncrBy(call: Call): Frame = {
    val key     = call.key(0)
    val name    = call.key(1)
    val amount  = call.long(2)
    val fields  = fieldsOf(call)
    val current = fields.get(name).fold(0L)(f => Numbers.parseLong(f.value).getOrElse(throw new Reject(Errors.err("hash value is not an integer"))))
    val updated =
      try Math.addExact(current, amount)
      catch { case _: ArithmeticException => throw new Reject(Errors.err("increment or decrement would overflow")) }
    store(call, key, fields.updated(name, Field(Bytes.utf8(updated.toString), fields.get(name).fold(Entry.NoExpiry)(_.expiresAt))))
    int(updated)
  }

  private def hIncrByFloat(call: Call): Frame = {
    val key       = call.key(0)
    val name      = call.key(1)
    val increment = call.double(2)
    val fields    = fieldsOf(call)
    val current   =
      fields.get(name).fold(0.0)(f => Numbers.parseDouble(f.value).getOrElse(throw new Reject(Errors.err("hash value is not a float"))))
    val updated   = current + increment
    if (updated.isNaN || updated.isInfinite) throw new Reject(Errors.err("increment would produce NaN or Infinity"))
    val text      = Bytes.utf8(Numbers.formatDouble(updated))
    store(call, key, fields.updated(name, Field(text, fields.get(name).fold(Entry.NoExpiry)(_.expiresAt))))
    bulk(text)
  }

  private def hRandField(call: Call): Frame = {
    val fields = fieldsOf(call).toVector
    call.args.length match {
      case 1     => fields.headOption.fold(Frame.Null)(_ => bulk(fields(Random.nextInt(fields.length))._1.bytes))
      case 2 | 3 =>
        val withValues = call.args.length == 3 && {
          if (call.keyword(2) != "WITHVALUES") throw new Reject(Errors.syntax)
          true
        }
        val picked     = Sampling.pick(fields, call.long(1))
        if (withValues) Frame.Array(picked.map((name, field) => Frame.Array(Vector(bulk(name.bytes), bulk(field.value)))))
        else keys(picked.map(_._1))
      case _     => throw new Reject(Errors.syntax)
    }
  }

  // Small hashes are scanned in one page on a real server, which is also what this store does: the cursor is always 0 afterwards.
  private def hScan(call: Call): Frame = {
    val (pattern, rest) = Scanning.options(call, 2, allowNoValues = true)
    Scanning.cursor(call, 1)
    val matching        = fieldsOf(call).filter((name, _) => pattern.forall(Glob.matches(_, name.bytes)))
    val items           =
      if (rest.noValues) matching.keys.toVector.map(name => bulk(name.bytes))
      else matching.toVector.flatMap((name, field) => Vector(bulk(name.bytes), bulk(field.value)))
    Frame.Array(Vector(bulk("0"), Frame.Array(items)))
  }

  private def hSet(call: Call): Frame = {
    if (call.args.length % 2 == 0) throw new Reject(Errors.arity(call.name))
    val key    = call.key(0)
    val fields = fieldsOf(call)
    val pairs  = call.args.drop(1).grouped(2).map(pair => (new Key(pair(0)), pair(1))).toVector
    val added  = pairs.map(_._1).distinct.count(!fields.contains(_))
    // a plain write replaces the field's value and clears its expiry
    store(call, key, pairs.foldLeft(fields)((acc, pair) => acc.updated(pair._1, Field(pair._2, Entry.NoExpiry))))
    int(added.toLong)
  }

  private def hSetNx(call: Call): Frame = {
    val key    = call.key(0)
    val fields = fieldsOf(call)
    if (fields.contains(call.key(1))) flag(false)
    else {
      store(call, key, fields.updated(call.key(1), Field(call.args(2), Entry.NoExpiry)))
      flag(true)
    }
  }

  private def hSetEx(call: Call): Frame = {
    val key       = call.key(0)
    var condition = ""
    var keepTtl   = false
    var expiry    = Option.empty[Long]
    var i         = 1
    while (i < call.args.length && call.keyword(i) != "FIELDS") {
      call.keyword(i) match {
        case word @ ("FNX" | "FXX") if condition.isEmpty                                                         => condition = word
        case "KEEPTTL" if !keepTtl && expiry.isEmpty                                                             => keepTtl = true
        case option if Expiry.options.contains(option) && expiry.isEmpty && !keepTtl && i + 1 < call.args.length =>
          i += 1
          expiry = Some(Expiry.absolute(call, option, i))
        case _                                                                                                   => throw new Reject(Errors.syntax)
      }
      i += 1
    }
    val pairs     = fieldsBlock(call, i, withValues = true)
    val fields    = fieldsOf(call)
    val skip      = condition match {
      case "FNX" => pairs.exists(pair => fields.contains(pair._1))
      case "FXX" => !pairs.forall(pair => fields.contains(pair._1))
      case _     => false
    }
    if (skip) flag(false)
    else {
      val written = pairs.foldLeft(fields) { (acc, pair) =>
        val at = expiry.getOrElse(if (keepTtl) acc.get(pair._1).fold(Entry.NoExpiry)(_.expiresAt) else Entry.NoExpiry)
        acc.updated(pair._1, Field(pair._2, at))
      }
      // an expiry time in the past deletes the fields right away
      store(call, key, if (expiry.exists(_ <= call.now)) written -- pairs.map(_._1) else written)
      flag(true)
    }
  }

  private def hExpire(call: Call, relative: Boolean, scale: Long): Frame = {
    val key       = call.key(0)
    val amount    = call.long(1)
    // at most one of NX, XX, GT and LT may come before FIELDS
    val condition = Option(call.keyword(2)).filter(_ != "FIELDS")
    if (condition.exists(c => !Set("NX", "XX", "GT", "LT").contains(c))) throw new Reject(Errors.syntax)
    val names     = fieldsBlock(call, if (condition.isDefined) 3 else 2, withValues = false).map(_._1)
    if (amount < 0) throw new Reject(Errors.invalidExpire(call.name))
    val at        =
      try {
        val scaled = Math.multiplyExact(amount, scale)
        if (relative) Math.addExact(call.now, scaled) else scaled
      } catch { case _: ArithmeticException => throw new Reject(Errors.invalidExpire(call.name)) }
    var fields    = fieldsOf(call)
    val results   = names.map { name =>
      fields.get(name) match {
        case None        => -2L
        case Some(field) =>
          val current = if (field.hasExpiry) field.expiresAt else Long.MaxValue
          val allowed = condition match {
            case Some("NX") => !field.hasExpiry
            case Some("XX") => field.hasExpiry
            case Some("GT") => at > current
            case Some("LT") => at < current
            case _          => true
          }
          if (!allowed) 0L
          else if (at <= call.now) {
            fields = fields - name
            2L
          } else {
            fields = fields.updated(name, field.copy(expiresAt = at))
            1L
          }
      }
    }
    if (results.exists(r => r == 1L || r == 2L)) store(call, key, fields)
    Frame.Array(results.map(int))
  }

  private def hTtl(call: Call, absolute: Boolean, millis: Boolean): Frame = {
    val fields = fieldsOf(call)
    val names  = fieldsBlock(call, 1, withValues = false).map(_._1)
    Frame.Array(names.map { name =>
      fields.get(name) match {
        case None                            => int(-2L)
        case Some(field) if !field.hasExpiry => int(-1L)
        case Some(field)                     =>
          val amount = if (absolute) field.expiresAt else field.expiresAt - call.now
          int(if (millis) amount else (amount + 500L) / 1000L)
      }
    })
  }

  private def hPersist(call: Call): Frame = {
    val key     = call.key(0)
    var fields  = fieldsOf(call)
    val names   = fieldsBlock(call, 1, withValues = false).map(_._1)
    val results = names.map { name =>
      fields.get(name) match {
        case None                            => -2L
        case Some(field) if !field.hasExpiry => -1L
        case Some(field)                     =>
          fields = fields.updated(name, field.copy(expiresAt = Entry.NoExpiry))
          1L
      }
    }
    if (results.contains(1L)) store(call, key, fields)
    Frame.Array(results.map(int))
  }
}

/**
  * Picks random elements the way `HRANDFIELD`, `SRANDMEMBER` and `ZRANDMEMBER` do: a positive count returns up to that many distinct
  * elements, and a negative count returns exactly that many elements, which may repeat.
  */
private[inmemory] object Sampling {

  def pick[A](items: Vector[A], count: Long): Vector[A] =
    if (items.isEmpty || count == 0) Vector.empty
    else if (count > 0) Random.shuffle(items).take(math.min(count, items.length.toLong).toInt)
    else Vector.fill(math.min(-count, Int.MaxValue.toLong).toInt)(items(Random.nextInt(items.length)))
}

/**
  * Parses the options of `HSCAN`, `SSCAN` and `ZSCAN`.
  */
private[inmemory] object Scanning {

  final case class Rest(noValues: Boolean)

  def cursor(call: Call, index: Int): Long =
    Numbers.parseLong(call.args(index)).filter(_ >= 0).getOrElse(throw new Reject(Errors.err("invalid cursor")))

  def options(call: Call, from: Int, allowNoValues: Boolean = false): (Option[Bytes], Rest) = {
    var pattern  = Option.empty[Bytes]
    var noValues = false
    var i        = from
    while (i < call.args.length)
      call.keyword(i) match {
        case "MATCH" if i + 1 < call.args.length =>
          pattern = Some(call.args(i + 1))
          i += 2
        case "COUNT" if i + 1 < call.args.length =>
          if (call.long(i + 1) < 1) throw new Reject(Errors.syntax)
          i += 2
        case "NOVALUES" if allowNoValues         =>
          noValues = true
          i += 1
        case _                                   => throw new Reject(Errors.syntax)
      }
    (pattern, Rest(noValues))
  }
}

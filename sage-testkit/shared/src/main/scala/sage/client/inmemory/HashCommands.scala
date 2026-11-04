package sage.client.inmemory

import scala.collection.immutable.VectorMap
import scala.util.Random

import sage.Bytes
import sage.protocol.Frame

private[inmemory] object HashCommands {
  import Replies.*
  import Value.Hash

  val specs: Map[String, Spec] = Map(
    "HDEL"         -> Spec(-3, call => int(removeFields(call, call.args.indices.drop(1).toVector.map(call.key)).count(_.isDefined).toLong)),
    "HEXISTS"      -> Spec(3, call => flag(fieldsOf(call).contains(call.key(1)))),
    "HGET"         -> Spec(3, call => optional(fieldsOf(call).get(call.key(1)))),
    "HGETALL"      -> Spec(2, call => Frame.Map(fieldsOf(call).iterator.map((name, value) => bulk(name.bytes) -> bulk(value)).toVector)),
    "HGETDEL"      -> Spec(-5, hGetDel),
    "HINCRBY"      -> Spec(4, hIncrBy),
    "HINCRBYFLOAT" -> Spec(4, hIncrByFloat),
    "HKEYS"        -> Spec(2, call => keys(fieldsOf(call).keys)),
    "HLEN"         -> Spec(2, call => int(fieldsOf(call).size.toLong)),
    "HMGET"        -> Spec(-3, hmGet),
    "HRANDFIELD"   -> Spec(-2, hRandField),
    "HSCAN"        -> Spec(-3, hScan),
    "HSET"         -> Spec(-4, hSet),
    "HSETNX"       -> Spec(4, hSetNx),
    "HSTRLEN"      -> Spec(3, call => int(fieldsOf(call).get(call.key(1)).fold(0L)(_.length.toLong))),
    "HVALS"        -> Spec(2, call => bulks(fieldsOf(call).values))
  )

  private def fieldsOf(call: Call): VectorMap[Key, Bytes] = call.typed[Hash](call.key(0)).fold(VectorMap.empty[Key, Bytes])(_.fields)

  private def hmGet(call: Call): Frame = {
    val fields = fieldsOf(call)
    Frame.Array(call.args.indices.drop(1).toVector.map(i => optional(fields.get(call.key(i)))))
  }

  // removes `names` from the hash at the key and returns the value each one had
  private def removeFields(call: Call, names: Vector[Key]): Vector[Option[Bytes]] = {
    var fields  = fieldsOf(call)
    val removed = names.map { name =>
      val value = fields.get(name)
      fields -= name
      value
    }
    if (removed.exists(_.isDefined)) call.put(call.key(0), Hash(fields))
    removed
  }

  // HGETDEL key FIELDS numfields field…
  private def hGetDel(call: Call): Frame = {
    if (call.keyword(1) != "FIELDS" || call.long(2) != call.args.length - 3) throw new Reject(Errors.syntax)
    Frame.Array(removeFields(call, call.args.indices.drop(3).toVector.map(call.key)).map(optional))
  }

  private def hIncrBy(call: Call): Frame = {
    val name    = call.key(1)
    val amount  = call.long(2)
    val fields  = fieldsOf(call)
    val updated = Numbers.added(fields.get(name), amount, Errors.err("hash value is not an integer"))
    call.put(call.key(0), Hash(fields.updated(name, Bytes.utf8(updated.toString))))
    int(updated)
  }

  private def hIncrByFloat(call: Call): Frame = {
    val name      = call.key(1)
    val increment = call.double(2)
    if (increment.isInfinite) throw new Reject(Errors.err("value is NaN or Infinity"))
    val fields    = fieldsOf(call)
    val text      = Numbers.addedFloat(fields.get(name), increment, Errors.err("hash value is not a float"))
    call.put(call.key(0), Hash(fields.updated(name, text)))
    bulk(text)
  }

  private def hRandField(call: Call): Frame = {
    val count  = Sampling.count(call, "WITHVALUES")
    val fields = fieldsOf(call).toVector
    count.fold(optional(Sampling.one(fields).map(_._1.bytes))) { (n, withValues) =>
      val picked = Sampling.pick(fields, n)
      if (withValues) Frame.Array(picked.map((name, value) => Frame.Array(Vector(bulk(name.bytes), bulk(value)))))
      else keys(picked.map(_._1))
    }
  }

  private def hScan(call: Call): Frame =
    Scanning.page(call, call.typed[Hash](call.key(0)), allowNoValues = true) { (hash, options) =>
      val matching = hash.fields.filter((name, _) => options.matches(name.bytes))
      if (options.noValues) matching.keys.toVector.map(name => bulk(name.bytes))
      else matching.toVector.flatMap((name, value) => Vector(bulk(name.bytes), bulk(value)))
    }

  private def hSet(call: Call): Frame = {
    val pairs  = call.pairs(1)
    val fields = fieldsOf(call)
    call.put(call.key(0), Hash(pairs.foldLeft(fields)((acc, pair) => acc.updated(pair._1, pair._2))))
    int(pairs.map(_._1).distinct.count(!fields.contains(_)).toLong)
  }

  private def hSetNx(call: Call): Frame = {
    val fields = fieldsOf(call)
    val absent = !fields.contains(call.key(1))
    if (absent) call.put(call.key(0), Hash(fields.updated(call.key(1), call.args(2))))
    flag(absent)
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
    else Vector.fill(Numbers.size(-count))(items(Random.nextInt(items.length)))

  def one[A](items: Vector[A]): Option[A] = Option.when(items.nonEmpty)(items(Random.nextInt(items.length)))

  // the `[count [word]]` of HRANDFIELD and ZRANDMEMBER, parsed before the key is read as on a server, with whether `word` was given
  def count(call: Call, word: String): Option[(Long, Boolean)] =
    Option.when(call.args.length > 1)((call.negatable(1), call.optionalWord(2, word)))
}

/**
  * Parses the cursor and options of `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN`.
  */
private[inmemory] object Scanning {

  final case class Options(pattern: Option[Bytes], count: Long, noValues: Boolean, ofType: Option[String]) {
    def matches(name: Bytes): Boolean = pattern.forall(Glob.matches(_, name))
  }

  def cursor(call: Call, index: Int): Long = call.bounded(index, Errors.err("invalid cursor"))(_ >= 0)

  def options(call: Call, from: Int, allowType: Boolean = false, allowNoValues: Boolean = false): Options = {
    var pattern  = Option.empty[Bytes]
    var count    = 10L
    var noValues = false
    var ofType   = Option.empty[String]
    call.options(from) {
      case "MATCH"             => pattern = Some(call.next())
      case "COUNT"             =>
        count = call.nextLong()
        if (count < 1) throw new Reject(Errors.syntax)
      case "TYPE" if allowType => ofType = Some(call.next().asUtf8String.toLowerCase(java.util.Locale.ROOT))
      case "NOVALUES"          =>
        if (!allowNoValues) throw new Reject(Errors.err("NOVALUES option can only be used in HSCAN"))
        noValues = true
      case _                   => throw new Reject(Errors.syntax)
    }
    Options(pattern, count, noValues, ofType)
  }

  /**
    * Scans the collection at the key in one page, so the next cursor is always 0. A real server also scans small collections in one page.
    * Like a server, it checks the cursor first, then the key, and parses the options only when the key exists.
    */
  def page[A](call: Call, value: => Option[A], allowNoValues: Boolean = false)(items: (A, Options) => Vector[Frame]): Frame = {
    cursor(call, 1)
    val found = value.fold(Vector.empty[Frame])(v => items(v, options(call, 2, allowNoValues = allowNoValues)))
    Frame.Array(Vector(Replies.bulk("0"), Frame.Array(found)))
  }
}

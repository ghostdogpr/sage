package sage.client.inmemory

import scala.collection.immutable.VectorMap

import sage.Bytes
import sage.protocol.Frame

private[inmemory] object HashCommands {
  import Replies.*
  import Value.Hash

  val specs: Map[String, Spec] = Map(
    "HDEL"         -> Spec(-3, hDel),
    "HEXISTS"      -> Spec(3, call => flag(fieldsOf(call).contains(call.key(1)))),
    "HGET"         -> Spec(3, call => optional(fieldsOf(call).get(call.key(1)))),
    "HGETALL"      -> Spec(2, call => Frame.Map(fieldsOf(call).iterator.map((name, value) => bulk(name.bytes) -> bulk(value)).toVector)),
    "HINCRBY"      -> Spec(4, hIncrBy),
    "HINCRBYFLOAT" -> Spec(4, hIncrByFloat),
    "HKEYS"        -> Spec(2, call => keys(fieldsOf(call).keys)),
    "HLEN"         -> Spec(2, call => int(fieldsOf(call).size.toLong)),
    "HMGET"        -> Spec(-3, hmGet),
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

  private def hDel(call: Call): Frame = {
    val fields  = fieldsOf(call)
    val removed = call.args.indices.drop(1).map(call.key).distinct.filter(fields.contains)
    if (removed.nonEmpty) call.put(call.key(0), Hash(fields -- removed))
    int(removed.length.toLong)
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
  * Parses the cursor and options of `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN`.
  */
private[inmemory] object Scanning {

  final case class Options(pattern: Option[Bytes], count: Long, noValues: Boolean, ofType: Option[String]) {
    def matches(name: Bytes): Boolean = pattern.forall(Glob.matches(_, name))
  }

  def cursor(call: Call, index: Int): Long = call.nonNegative(index, Errors.err("invalid cursor"))

  def options(call: Call, from: Int, allowType: Boolean = false, allowNoValues: Boolean = false): Options = {
    var pattern  = Option.empty[Bytes]
    var count    = 10L
    var noValues = false
    var ofType   = Option.empty[String]
    call.options(from) {
      case "MATCH"                     => pattern = Some(call.next())
      case "COUNT"                     =>
        count = call.nextLong()
        if (count < 1) throw new Reject(Errors.syntax)
      case "TYPE" if allowType         => ofType = Some(call.next().asUtf8String.toLowerCase(java.util.Locale.ROOT))
      case "NOVALUES" if allowNoValues => noValues = true
      case _                           => throw new Reject(Errors.syntax)
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

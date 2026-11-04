package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

/**
  * Applies Redis start and stop indexes, which are inclusive and count from the end when negative.
  */
private[inmemory] object Ranges {

  def slice[A](items: Vector[A], start: Long, stop: Long): Vector[A] = {
    val length = items.length.toLong
    val from   = math.max(0L, if (start < 0) length + start else start)
    val to     = math.min(length - 1, if (stop < 0) length + stop else stop)
    if (from <= to) items.slice(from.toInt, to.toInt + 1) else Vector.empty
  }
}

private[inmemory] object ListCommands {
  import Replies.*
  import Value.Lst

  val specs: Map[String, Spec] = Map(
    "LINDEX"  -> Spec(3, lIndex),
    "LINSERT" -> Spec(5, lInsert),
    "LLEN"    -> Spec(2, call => int(items(call, call.key(0)).length.toLong)),
    "LMOVE"   -> Spec(5, lMove),
    "LMPOP"   -> Spec(-4, call => lmPop(call, 0)),
    "BLMOVE"  -> Spec.blocking(6, lMove)(call => Timeouts.seconds(call.args(4), call.now)),
    "BLMPOP"  -> Spec.blocking(-5, lmPop(_, 1))(call => Timeouts.seconds(call.args(0), call.now)),
    "BLPOP"   -> Spec.blocking(-3, blockingPop(_, left = true))(call => Timeouts.seconds(call.args.last, call.now)),
    "BRPOP"   -> Spec.blocking(-3, blockingPop(_, left = false))(call => Timeouts.seconds(call.args.last, call.now)),
    "LPOP"    -> Spec(-2, call => pop(call, left = true)),
    "LPOS"    -> Spec(-3, lPos),
    "LPUSH"   -> Spec(-3, call => push(call, left = true, onlyIfExists = false)),
    "LPUSHX"  -> Spec(-3, call => push(call, left = true, onlyIfExists = true)),
    "LRANGE"  -> Spec(4, lRange),
    "LREM"    -> Spec(4, lRem),
    "LSET"    -> Spec(4, lSet),
    "LTRIM"   -> Spec(4, lTrim),
    "RPOP"    -> Spec(-2, call => pop(call, left = false)),
    "RPUSH"   -> Spec(-3, call => push(call, left = false, onlyIfExists = false)),
    "RPUSHX"  -> Spec(-3, call => push(call, left = false, onlyIfExists = true))
  )

  private def items(call: Call, key: Key): Vector[Bytes] = call.typed[Lst](key).fold(Vector.empty[Bytes])(_.items)

  private def push(call: Call, left: Boolean, onlyIfExists: Boolean): Frame = {
    val key     = call.key(0)
    val current = call.typed[Lst](key)
    if (onlyIfExists && current.isEmpty) int(0L)
    else {
      val values  = call.args.drop(1)
      // LPUSH inserts the values one at a time at the head, so the last value ends up first
      val updated = if (left) values.reverse ++ current.fold(Vector.empty[Bytes])(_.items) else current.fold(Vector.empty[Bytes])(_.items) ++ values
      call.put(key, Lst(updated))
      int(updated.length.toLong)
    }
  }

  // the `count` items at one end of `items` in the order they are popped, and the rest
  private def split(items: Vector[Bytes], count: Int, left: Boolean): (Vector[Bytes], Vector[Bytes]) =
    if (left) items.splitAt(count) else (items.takeRight(count).reverse, items.dropRight(count))

  // stores the rest of `items` at `key` and returns the taken items; popping nothing is not a write, as on a server
  private def take(call: Call, key: Key, items: Vector[Bytes], count: Int, left: Boolean): Vector[Bytes] = {
    val (taken, rest) = split(items, count, left)
    if (taken.nonEmpty) call.put(key, Lst(rest))
    taken
  }

  private def pop(call: Call, left: Boolean): Frame = {
    val key   = call.key(0)
    val count = call.optional(1, Errors.arity(call.name))(call.bounded(_, Errors.outOfRange)(_ >= 0))
    call.typed[Lst](key).fold(Frame.Null) { list =>
      count.fold(optional(take(call, key, list.items, 1, left).headOption))(n => bulks(take(call, key, list.items, Numbers.size(n), left)))
    }
  }

  // a server reads the index only when the list exists
  private def lIndex(call: Call): Frame =
    call.typed[Lst](call.key(0)).fold(Frame.Null) { list =>
      val index = call.long(1)
      val at    = if (index < 0) list.items.length + index else index
      if (at < 0 || at >= list.items.length) Frame.Null else bulk(list.items(at.toInt))
    }

  private def lInsert(call: Call): Frame = {
    val key    = call.key(0)
    val before = call.choice(1, "BEFORE", "AFTER")
    call.typed[Lst](key) match {
      case None       => int(0L)
      case Some(list) =>
        list.items.indexWhere(_.sameBytes(call.args(2))) match {
          case -1    => int(-1L)
          case pivot =>
            val at      = if (before) pivot else pivot + 1
            val updated = list.items.patch(at, Vector(call.args(3)), 0)
            call.put(key, Lst(updated))
            int(updated.length.toLong)
        }
    }
  }

  private def lRange(call: Call): Frame = {
    val (start, stop) = (call.long(1), call.long(2))
    bulks(Ranges.slice(items(call, call.key(0)), start, stop))
  }

  private def lRem(call: Call): Frame = {
    val key     = call.key(0)
    val count   = call.long(1)
    val target  = call.args(2)
    val list    = items(call, key)
    val limit   = if (count == 0) Long.MaxValue else math.abs(count)
    val ordered = if (count < 0) list.reverse else list
    var removed = 0L
    val kept    = ordered.filter { item =>
      val drop = removed < limit && item.sameBytes(target)
      if (drop) removed += 1
      !drop
    }
    if (removed > 0) call.put(key, Lst(if (count < 0) kept.reverse else kept))
    int(removed)
  }

  private def lSet(call: Call): Frame = {
    val key   = call.key(0)
    val list  = call.typed[Lst](key).getOrElse(throw new Reject(Errors.noSuchKey)).items
    val index = call.long(1)
    val at    = if (index < 0) list.length + index else index
    if (at < 0 || at >= list.length) throw new Reject(Errors.err("index out of range"))
    call.put(key, Lst(list.updated(at.toInt, call.args(2))))
    ok
  }

  private def lTrim(call: Call): Frame = {
    val key           = call.key(0)
    val (start, stop) = (call.long(1), call.long(2))
    val list          = items(call, key)
    if (list.nonEmpty) call.put(key, Lst(Ranges.slice(list, start, stop)))
    ok
  }

  private def lPos(call: Call): Frame = {
    var rank      = 1L
    var count     = Option.empty[Long]
    var maxLen    = 0L
    call.options(2) {
      case "RANK"   =>
        rank = call.negatable(call.nextIndex())
        if (rank == 0) throw new Reject(Errors.err("RANK can't be zero"))
      case "COUNT"  => count = Some(call.nextBounded(Errors.err("COUNT can't be negative"))(_ >= 0))
      case "MAXLEN" => maxLen = call.nextBounded(Errors.err("MAXLEN can't be negative"))(_ >= 0)
      case _        => throw new Reject(Errors.syntax)
    }
    val list      = items(call, call.key(0))
    val positions = if (rank > 0) list.indices else list.indices.reverse
    val scanned   = if (maxLen == 0) positions else positions.take(Numbers.size(maxLen))
    val matches   = scanned.iterator.filter(i => list(i).sameBytes(call.args(1))).drop(Numbers.size(math.abs(rank) - 1))
    count match {
      case None    => matches.nextOption().fold(Frame.Null)(i => int(i.toLong))
      case Some(n) => Frame.Array((if (n == 0) matches else matches.take(Numbers.size(n))).map(i => int(i.toLong)).toVector)
    }
  }

  private def lMove(call: Call): Frame = {
    val source      = call.key(0)
    val destination = call.key(1)
    val fromLeft    = call.choice(2, "LEFT", "RIGHT")
    val toLeft      = call.choice(3, "LEFT", "RIGHT")
    call.typed[Lst](source) match {
      case None       => Frame.Null
      case Some(list) =>
        // check the destination type before changing anything, as the server does
        call.typed[Lst](destination): Unit
        val (moved, rest) = split(list.items, 1, fromLeft)
        // moving within one list never empties it, so the key keeps its expiry
        if (source != destination) call.put(source, Lst(rest))
        val target        = if (source == destination) rest else items(call, destination)
        call.put(destination, Lst(if (toLeft) moved ++ target else target ++ moved))
        optional(moved.headOption)
    }
  }

  // BLPOP and BRPOP pop one element from the first non-empty list and reply with that list's key and the element
  private def blockingPop(call: Call, left: Boolean): Frame =
    firstPop(call.args.indices.init.iterator.map(call.key)) { key =>
      call.typed[Lst](key).map(list => Frame.Array(bulk(key.bytes) +: take(call, key, list.items, 1, left).map(bulk)))
    }

  // the reply of the first of `keys` that `pop` takes from, or null; the keys after it are not read, as on a server
  def firstPop(keys: Iterator[Key])(pop: Key => Option[Frame]): Frame = keys.flatMap(pop).nextOption().getOrElse(Frame.Null)

  // `at` is the index of numkeys, which BLMPOP puts after its timeout
  private def lmPop(call: Call, at: Int): Frame =
    multiPop(call, at, "LEFT", "RIGHT") { (key, left, count) =>
      call.typed[Lst](key).map(list => bulks(take(call, key, list.items, count, left)))
    }

  // `numkeys key… first | second [COUNT count]` of LMPOP and ZMPOP from `at`; `popFrom` returns None for an empty key
  def multiPop(call: Call, at: Int, first: String, second: String)(popFrom: (Key, Boolean, Int) => Option[Frame]): Frame = {
    val keys  = call.numKeys(at)
    val left  = call.choice(call.nextIndex(), first, second)
    val count = call.trailing("COUNT").fold(1L)(call.bounded(_, Errors.err("count should be greater than 0"))(_ > 0))
    firstPop(keys.iterator)(key => popFrom(key, left, Numbers.size(count)).map(popped => Frame.Array(Vector(bulk(key.bytes), popped))))
  }
}

package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

/**
  * Converts Redis start and stop indexes, where negative values count from the end, to an inclusive range of positions.
  */
private[inmemory] object Ranges {

  def inclusive(start: Long, stop: Long, length: Int): Option[(Int, Int)] = {
    val from = math.max(0L, if (start < 0) length + start else start)
    val to   = math.min(length - 1L, if (stop < 0) length + stop else stop)
    Option.when(from <= to && from < length)((from.toInt, to.toInt))
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
    // the blocking forms throw Blocked when there is nothing to return, and the client retries them after the next write
    "BLMOVE"  -> Spec(6, call => orBlock(lMove(call)), timeout = Some(args => Timeouts.seconds(args(4)))),
    "BLMPOP"  -> Spec(-5, call => orBlock(lmPop(call, 1)), timeout = Some(args => Timeouts.seconds(args(0)))),
    "BLPOP"   -> Spec(-3, call => blockingPop(call, left = true), timeout = Some(args => Timeouts.seconds(args.last))),
    "BRPOP"   -> Spec(-3, call => blockingPop(call, left = false), timeout = Some(args => Timeouts.seconds(args.last))),
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

  private def store(call: Call, key: Key, items: Vector[Bytes]): Unit = call.replace(key, Lst(items))

  private def side(call: Call, index: Int): Boolean =
    call.keyword(index) match {
      case "LEFT"  => true
      case "RIGHT" => false
      case _       => throw new Reject(Errors.syntax)
    }

  private def push(call: Call, left: Boolean, onlyIfExists: Boolean): Frame = {
    val key     = call.key(0)
    val current = call.typed[Lst](key)
    if (onlyIfExists && current.isEmpty) int(0L)
    else {
      val values  = call.args.drop(1)
      // LPUSH inserts the values one at a time at the head, so the last value ends up first
      val updated = if (left) values.reverse ++ current.fold(Vector.empty[Bytes])(_.items) else current.fold(Vector.empty[Bytes])(_.items) ++ values
      store(call, key, updated)
      int(updated.length.toLong)
    }
  }

  private def pop(call: Call, left: Boolean): Frame = {
    val key     = call.key(0)
    val current = call.typed[Lst](key)
    call.args.length match {
      case 1 =>
        current.fold(Frame.Null) { list =>
          val (taken, rest) = if (left) (list.items.head, list.items.tail) else (list.items.last, list.items.init)
          store(call, key, rest)
          bulk(taken)
        }
      case 2 =>
        val count = call.long(1)
        if (count < 0) throw new Reject(Errors.outOfRange)
        current.fold(Frame.Null) { list =>
          val n             = math.min(count, list.items.length.toLong).toInt
          val (taken, rest) =
            if (left) list.items.splitAt(n) else (list.items.takeRight(n).reverse, list.items.dropRight(n))
          store(call, key, rest)
          bulks(taken)
        }
      case _ => throw new Reject(Errors.syntax)
    }
  }

  private def lIndex(call: Call): Frame = {
    val list  = items(call, call.key(0))
    val index = call.long(1)
    val at    = if (index < 0) list.length + index else index
    if (at < 0 || at >= list.length) Frame.Null else bulk(list(at.toInt))
  }

  private def lInsert(call: Call): Frame = {
    val key    = call.key(0)
    val before = call.keyword(1) match {
      case "BEFORE" => true
      case "AFTER"  => false
      case _        => throw new Reject(Errors.syntax)
    }
    call.typed[Lst](key) match {
      case None       => int(0L)
      case Some(list) =>
        list.items.indexWhere(_.sameBytes(call.args(2))) match {
          case -1    => int(-1L)
          case pivot =>
            val at      = if (before) pivot else pivot + 1
            val updated = list.items.patch(at, Vector(call.args(3)), 0)
            store(call, key, updated)
            int(updated.length.toLong)
        }
    }
  }

  private def lRange(call: Call): Frame = {
    val list = items(call, call.key(0))
    bulks(Ranges.inclusive(call.long(1), call.long(2), list.length).fold(Vector.empty[Bytes])((from, to) => list.slice(from, to + 1)))
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
    if (removed > 0) store(call, key, if (count < 0) kept.reverse else kept)
    int(removed)
  }

  private def lSet(call: Call): Frame = {
    val key   = call.key(0)
    val list  = call.typed[Lst](key).getOrElse(throw new Reject(Errors.noSuchKey)).items
    val index = call.long(1)
    val at    = if (index < 0) list.length + index else index
    if (at < 0 || at >= list.length) throw new Reject(Errors.err("index out of range"))
    store(call, key, list.updated(at.toInt, call.args(2)))
    ok
  }

  private def lTrim(call: Call): Frame = {
    val key  = call.key(0)
    val list = items(call, key)
    if (list.nonEmpty)
      store(call, key, Ranges.inclusive(call.long(1), call.long(2), list.length).fold(Vector.empty[Bytes])((from, to) => list.slice(from, to + 1)))
    ok
  }

  private def lPos(call: Call): Frame = {
    var rank      = 1L
    var count     = Option.empty[Long]
    var maxLen    = 0L
    var i         = 2
    while (i < call.args.length) {
      if (i + 1 >= call.args.length) throw new Reject(Errors.syntax)
      call.keyword(i) match {
        case "RANK"   =>
          rank = call.long(i + 1)
          if (rank == 0)
            throw new Reject(
              Errors.err(
                "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the last match"
              )
            )
        case "COUNT"  =>
          count = Some(call.long(i + 1))
          if (count.exists(_ < 0)) throw new Reject(Errors.err("COUNT can't be negative"))
        case "MAXLEN" =>
          maxLen = call.long(i + 1)
          if (maxLen < 0) throw new Reject(Errors.err("MAXLEN can't be negative"))
        case _        => throw new Reject(Errors.syntax)
      }
      i += 2
    }
    val list      = items(call, call.key(0))
    val positions = if (rank > 0) list.indices else list.indices.reverse
    val scanned   = if (maxLen == 0) positions else positions.take(math.min(maxLen, Int.MaxValue.toLong).toInt)
    val matches   = scanned.iterator.filter(i => list(i).sameBytes(call.args(1))).drop((math.abs(rank) - 1).toInt)
    count match {
      case None    => matches.nextOption().fold(Frame.Null)(i => int(i.toLong))
      case Some(n) => Frame.Array((if (n == 0) matches else matches.take(n.toInt)).map(i => int(i.toLong)).toVector)
    }
  }

  private def lMove(call: Call): Frame = {
    val source      = call.key(0)
    val destination = call.key(1)
    val fromLeft    = side(call, 2)
    val toLeft      = side(call, 3)
    call.typed[Lst](source) match {
      case None       => Frame.Null
      case Some(list) =>
        // check the destination type before changing anything, as the server does
        call.typed[Lst](destination): Unit
        val item   = if (fromLeft) list.items.head else list.items.last
        store(call, source, if (fromLeft) list.items.tail else list.items.init)
        val target = items(call, destination)
        store(call, destination, if (toLeft) item +: target else target :+ item)
        bulk(item)
    }
  }

  def orBlock(reply: Frame): Frame = if (reply == Frame.Null) throw Blocked else reply

  // BLPOP and BRPOP pop one element from the first non-empty list and reply with that list's key and the element
  private def blockingPop(call: Call, left: Boolean): Frame =
    call.args.indices.init.map(call.key).find(key => items(call, key).nonEmpty).fold(throw Blocked) { key =>
      val list          = items(call, key)
      val (taken, rest) = if (left) (list.head, list.tail) else (list.last, list.init)
      store(call, key, rest)
      Frame.Array(Vector(bulk(key.bytes), bulk(taken)))
    }

  // `at` is the index of numkeys, which BLMPOP puts after its timeout
  private def lmPop(call: Call, at: Int): Frame = {
    val (keys, next) = call.numKeys(at)
    if (next >= call.args.length) throw new Reject(Errors.syntax)
    val left         = side(call, next)
    val count        = call.args.length - next match {
      case 1                                      => 1L
      case 3 if call.keyword(next + 1) == "COUNT" =>
        val n = call.long(next + 2)
        if (n <= 0) throw new Reject(Errors.err("count should be greater than 0"))
        n
      case _                                      => throw new Reject(Errors.syntax)
    }
    keys.iterator.map(key => key -> call.typed[Lst](key)).collectFirst { case (key, Some(list)) => key -> list } match {
      case None              => Frame.Null
      case Some((key, list)) =>
        val n             = math.min(count, list.items.length.toLong).toInt
        val (taken, rest) = if (left) list.items.splitAt(n) else (list.items.takeRight(n).reverse, list.items.dropRight(n))
        store(call, key, rest)
        Frame.Array(Vector(bulk(key.bytes), bulks(taken)))
    }
  }
}

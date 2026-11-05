package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

private[inmemory] object SortedSetCommands {
  import Replies.*
  import Value.Sorted

  val specs: Map[String, Spec] = Map(
    "ZADD"             -> Spec(-4, zAdd),
    "ZCARD"            -> Spec(2, call => int(sorted(call, call.key(0)).scores.size.toLong)),
    "ZCOUNT"           -> Spec(4, call => int(sorted(call, call.key(0)).ordered.count(range(call, 1, 2)).toLong)),
    "ZINCRBY"          -> Spec(4, call => add(call, Set("INCR"), Vector((call.double(1), call.key(2))))),
    "BZPOPMAX"         -> Spec(-3, blockingPop(_, max = true), blocking = true),
    "BZPOPMIN"         -> Spec(-3, blockingPop(_, max = false), blocking = true),
    "ZMSCORE"          -> Spec(-3, zmScore),
    "ZPOPMAX"          -> Spec(-2, call => popReply(call, max = true)),
    "ZPOPMIN"          -> Spec(-2, call => popReply(call, max = false)),
    "ZRANGE"           -> Spec(-4, zRange),
    "ZRANK"            -> Spec(-3, call => zRank(call, reverse = false)),
    "ZREM"             -> Spec(-3, zRem),
    "ZREMRANGEBYRANK"  -> Spec(4, zRemRangeByRank),
    "ZREMRANGEBYSCORE" -> Spec(4, call => int(remove(call, call.key(0))(_.ordered.filter(range(call, 1, 2))).size.toLong)),
    "ZREVRANK"         -> Spec(-3, call => zRank(call, reverse = true)),
    "ZSCAN"            -> Spec(-3, zScan),
    "ZSCORE"           -> Spec(3, call => sorted(call, call.key(0)).scores.get(call.key(1)).fold(Frame.Null)(Frame.Double(_)))
  )

  private type Scored = (Double, Key)

  private def sorted(call: Call, key: Key): Sorted = call.typed[Sorted](key).getOrElse(Sorted.empty)

  private def pairs(items: Iterable[Scored]): Frame =
    Frame.Array(items.iterator.map((score, member) => Frame.Array(Vector(bulk(member.bytes), Frame.Double(score)))).toVector)

  private def members(items: Iterable[Scored]): Frame = keys(items.view.map(_._2))

  private type Bound = Scored => Boolean

  private def scoreBound(call: Call, index: Int, lower: Boolean): Bound = {
    val text      = call.text(index)
    val exclusive = text.startsWith("(")
    val number    = Numbers
      .parseDouble(Bytes.utf8(if (exclusive) text.drop(1) else text))
      .getOrElse(throw new Reject(Errors.err("min or max is not a float")))
    if (lower) (score, _) => if (exclusive) score > number else score >= number
    else (score, _) => if (exclusive) score < number else score <= number
  }

  private def range(call: Call, min: Int, max: Int): Bound = {
    val low  = scoreBound(call, min, true)
    val high = scoreBound(call, max, false)
    item => low(item) && high(item)
  }

  private val zAddFlags: Set[String] = Set("NX", "XX", "GT", "LT", "CH", "INCR")

  private def zAdd(call: Call): Frame = {
    val words = call.args.indices.iterator.drop(1).map(call.keyword).takeWhile(zAddFlags).toVector
    val flags = words.toSet
    if (flags("NX") && (flags("XX") || flags("GT") || flags("LT")) || flags("GT") && flags("LT"))
      throw new Reject(Errors.err("XX, NX, GT and LT options at the same time are not compatible"))
    val from  = 1 + words.length
    val rest  = call.args.length - from
    if (rest == 0 || rest % 2 != 0) throw new Reject(Errors.syntax)
    if (flags("INCR") && rest != 2) throw new Reject(Errors.err("INCR option supports a single increment-element pair"))
    add(call, flags, Vector.tabulate(rest / 2)(i => (call.double(from + 2 * i), call.key(from + 2 * i + 1))))
  }

  private def add(call: Call, flags: Set[String], input: Vector[Scored]): Frame = {
    val key     = call.key(0)
    var set     = sorted(call, key)
    var added   = 0L
    var changed = 0L
    var last    = Option.empty[Double]
    input.foreach { (score, member) =>
      val old     = set.scores.get(member)
      val updated = if (flags("INCR")) old.getOrElse(0.0) + score else score
      if (updated.isNaN) throw new Reject(Errors.err("resulting score is not a number (NaN)"))
      val skip    =
        (flags("NX") && old.isDefined) || (flags("XX") && old.isEmpty) ||
          (flags("GT") && old.exists(updated <= _)) || (flags("LT") && old.exists(updated >= _))
      if (!skip) {
        if (old.isEmpty) added += 1
        if (!old.contains(updated)) changed += 1
        set = set.updated(member, updated)
        last = Some(updated)
      }
    }
    // as on a server, a ZADD that adds no member and changes no score is not a write, so it does not abort a WATCH
    if (changed > 0) call.put(key, set)
    if (flags("INCR")) last.fold(Frame.Null)(Frame.Double(_)) else int(if (flags("CH")) changed else added)
  }

  private def zmScore(call: Call): Frame = {
    val scores = sorted(call, call.key(0)).scores
    Frame.Array(call.args.indices.drop(1).toVector.map(i => scores.get(call.key(i)).fold(Frame.Null)(Frame.Double(_))))
  }

  private def zRank(call: Call, reverse: Boolean): Frame = {
    val withScore = call.optional(2, Errors.arity(call.name))(i => call.keyword(i) == "WITHSCORE" || (throw new Reject(Errors.syntax))).isDefined
    val set       = sorted(call, call.key(0))
    set.scores.get(call.key(1)).fold(Frame.Null) { score =>
      val ascending = set.ordered.iterator.indexWhere(_._2 == call.key(1)).toLong
      val rank      = if (reverse) set.scores.size - 1 - ascending else ascending
      if (withScore) Frame.Array(Vector(int(rank), Frame.Double(score))) else int(rank)
    }
  }

  private def zRem(call: Call): Frame = {
    val key   = call.key(0)
    val set   = sorted(call, key)
    val names = call.args.indices.drop(1).map(call.key).distinct.filter(set.scores.contains)
    if (names.nonEmpty) call.put(key, names.foldLeft(set)(_.removed(_)))
    int(names.length.toLong)
  }

  // removing nothing is not a write, as on a server
  private def remove[C <: Iterable[Scored]](call: Call, key: Key)(select: Sorted => C): C = {
    val set     = sorted(call, key)
    val removed = select(set)
    if (removed.nonEmpty) call.put(key, removed.foldLeft(set)((acc, item) => acc.removed(item._2)))
    removed
  }

  private def zRemRangeByRank(call: Call): Frame = {
    val (start, stop) = (call.long(1), call.long(2))
    int(remove(call, call.key(0))(set => Ranges.slice(set.ordered.toVector, start, stop)).length.toLong)
  }

  private def zRange(call: Call): Frame = {
    var byScore                                  = false
    var rev                                      = false
    var limit                                    = Option.empty[(Long, Long)]
    var withScores                               = false
    call.options(3) {
      case "BYSCORE"    => byScore = true
      case "BYLEX"      => call.unsupported("BYLEX")
      case "REV"        => rev = true
      case "WITHSCORES" => withScores = true
      case "LIMIT"      => limit = Some((call.nextLong(), call.nextLong()))
      case _            => throw new Reject(Errors.syntax)
    }
    if (limit.isDefined && !byScore)
      throw new Reject(Errors.err("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"))
    // a server parses the range before it reads the key
    val select: Vector[Scored] => Vector[Scored] =
      if (!byScore) {
        val (start, stop) = (call.long(1), call.long(2))
        Ranges.slice(_, start, stop)
      } else {
        // with REV the caller gives the upper bound first
        val bound = if (rev) range(call, 2, 1) else range(call, 1, 2)
        ordered =>
          val matching = ordered.filter(bound)
          // a negative count takes everything after the offset
          limit.fold(matching) { (offset, count) =>
            val skipped = if (offset < 0) Vector.empty else matching.drop(Numbers.size(offset))
            if (count < 0) skipped else skipped.take(Numbers.size(count))
          }
      }
    val all                                      = sorted(call, call.key(0)).ordered.toVector
    val selected                                 = select(if (rev) all.reverse else all)
    if (withScores) pairs(selected) else members(selected)
  }

  private def pop(call: Call, key: Key, count: Int, max: Boolean): Vector[Scored] =
    remove(call, key)(set => (if (max) set.ordered.toVector.reverse else set.ordered.toVector).take(count))

  private def popReply(call: Call, max: Boolean): Frame =
    call.optional(1)(call.nonNegative(_, Errors.outOfRange)) match {
      case None    =>
        // without a count the reply is a flat [member, score] pair, or an empty array when there is nothing to pop
        pop(call, call.key(0), 1, max).headOption.fold(Frame.Array(Vector.empty)) { (score, member) =>
          Frame.Array(Vector(bulk(member.bytes), Frame.Double(score)))
        }
      case Some(n) => pairs(pop(call, call.key(0), Numbers.size(n), max))
    }

  // BZPOPMIN and BZPOPMAX reply with the key, the member and its score
  private def blockingPop(call: Call, max: Boolean): Frame =
    ListCommands.firstPop(call.args.indices.init.iterator.map(call.key)) { key =>
      pop(call, key, 1, max).headOption.map((score, member) => Frame.Array(Vector(bulk(key.bytes), bulk(member.bytes), Frame.Double(score))))
    }

  private def zScan(call: Call): Frame =
    Scanning.page(call, call.typed[Sorted](call.key(0))) { (set, options) =>
      set.ordered.toVector
        .filter((_, member) => options.matches(member.bytes))
        .flatMap { (score, member) =>
          val text = if (score.isPosInfinity) "inf" else if (score.isNegInfinity) "-inf" else Numbers.formatDouble(score)
          Vector(bulk(member.bytes), bulk(text))
        }
    }
}

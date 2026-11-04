package sage.client.inmemory

import scala.util.Random

import sage.Bytes
import sage.protocol.Frame

private[inmemory] object SortedSetCommands {
  import Replies.*
  import Value.Sorted

  val specs: Map[String, Spec] = Map(
    "ZADD"             -> Spec(-4, zAdd),
    "ZCARD"            -> Spec(2, call => int(sorted(call, call.key(0)).scores.size.toLong)),
    "ZCOUNT"           -> Spec(4, zCount),
    "ZDIFF"            -> Spec(-3, call => combineReply(call, Combine.Diff)),
    "ZDIFFSTORE"       -> Spec(-4, call => combineStore(call, Combine.Diff)),
    "ZINCRBY"          -> Spec(4, zIncrBy),
    "ZINTER"           -> Spec(-3, call => combineReply(call, Combine.Inter)),
    "ZINTERCARD"       -> Spec(-3, zInterCard),
    "ZINTERSTORE"      -> Spec(-4, call => combineStore(call, Combine.Inter)),
    "ZLEXCOUNT"        -> Spec(4, zLexCount),
    "ZMPOP"            -> Spec(-4, call => zmPop(call, 0)),
    "BZMPOP"           -> Spec(-5, call => ListCommands.orBlock(zmPop(call, 1)), timeout = Some(args => Timeouts.seconds(args(0)))),
    "BZPOPMAX"         -> Spec(-3, call => blockingPop(call, max = true), timeout = Some(args => Timeouts.seconds(args.last))),
    "BZPOPMIN"         -> Spec(-3, call => blockingPop(call, max = false), timeout = Some(args => Timeouts.seconds(args.last))),
    "ZMSCORE"          -> Spec(-3, zmScore),
    "ZPOPMAX"          -> Spec(-2, call => popReply(call, max = true)),
    "ZPOPMIN"          -> Spec(-2, call => popReply(call, max = false)),
    "ZRANDMEMBER"      -> Spec(-2, zRandMember),
    "ZRANGE"           -> Spec(-4, zRange),
    "ZRANGESTORE"      -> Spec(-5, zRangeStore),
    "ZRANK"            -> Spec(-3, call => zRank(call, reverse = false)),
    "ZREM"             -> Spec(-3, zRem),
    "ZREMRANGEBYLEX"   -> Spec(4, call => removeWhere(call, lexRange(call, 1, 2))),
    "ZREMRANGEBYRANK"  -> Spec(4, zRemRangeByRank),
    "ZREMRANGEBYSCORE" -> Spec(4, call => removeWhere(call, scoreRange(call, 1, 2))),
    "ZREVRANK"         -> Spec(-3, call => zRank(call, reverse = true)),
    "ZSCAN"            -> Spec(-3, zScan),
    "ZSCORE"           -> Spec(3, call => sorted(call, call.key(0)).scores.get(call.key(1)).fold(Frame.Null)(Frame.Double(_))),
    "ZUNION"           -> Spec(-3, call => combineReply(call, Combine.Union)),
    "ZUNIONSTORE"      -> Spec(-4, call => combineStore(call, Combine.Union))
  )

  private type Scored = (Double, Key)

  private def sorted(call: Call, key: Key): Sorted = call.typed[Sorted](key).getOrElse(Sorted.empty)

  private def pairs(items: Iterable[Scored]): Frame =
    Frame.Array(items.iterator.map((score, member) => Frame.Array(Vector(bulk(member.bytes), Frame.Double(score)))).toVector)

  private def members(items: Iterable[Scored]): Frame = keys(items.map(_._2))

  // a range bound that selects members by score or by their bytes
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

  private def lexBound(call: Call, index: Int, lower: Boolean): Bound = {
    val raw = call.args(index).toIArray
    if (raw.length == 1 && raw(0) == '-') (_, _) => lower
    else if (raw.length == 1 && raw(0) == '+') (_, _) => !lower
    else if (raw.nonEmpty && (raw(0) == '[' || raw(0) == '(')) {
      val bound     = new Key(Bytes.wrap(raw.drop(1)))
      val inclusive = raw(0) == '['
      val ordering  = summon[Ordering[Key]]
      (_, member) => {
        val c = ordering.compare(member, bound)
        if (lower) c > 0 || (inclusive && c == 0) else c < 0 || (inclusive && c == 0)
      }
    } else throw new Reject(Errors.err("min or max not valid string range item"))
  }

  private def scoreRange(call: Call, min: Int, max: Int): Bound = {
    val low  = scoreBound(call, min, lower = true)
    val high = scoreBound(call, max, lower = false)
    item => low(item) && high(item)
  }

  private def lexRange(call: Call, min: Int, max: Int): Bound = {
    val low  = lexBound(call, min, lower = true)
    val high = lexBound(call, max, lower = false)
    item => low(item) && high(item)
  }

  private def zAdd(call: Call): Frame = {
    val key   = call.key(0)
    var flags = Set.empty[String]
    var i     = 1
    while (i < call.args.length && Set("NX", "XX", "GT", "LT", "CH", "INCR").contains(call.keyword(i))) {
      flags += call.keyword(i)
      i += 1
    }
    if (flags("NX") && flags("XX")) throw new Reject(Errors.err("XX and NX options at the same time are not compatible"))
    if ((flags("GT") && flags("LT")) || (flags("NX") && (flags("GT") || flags("LT"))))
      throw new Reject(Errors.err("GT, LT, and/or NX options at the same time are not compatible"))
    val rest  = call.args.length - i
    if (rest == 0 || rest % 2 != 0) throw new Reject(Errors.syntax)
    if (flags("INCR") && rest != 2) throw new Reject(Errors.err("INCR option supports a single increment-element pair"))
    val input   = Vector.tabulate(rest / 2)(p => (call.double(i + p * 2), call.key(i + p * 2 + 1)))
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
    call.replace(key, set)
    if (flags("INCR")) last.fold(Frame.Null)(Frame.Double(_)) else int(if (flags("CH")) changed else added)
  }

  private def zIncrBy(call: Call): Frame = {
    val key     = call.key(0)
    val set     = sorted(call, key)
    val member  = call.key(2)
    val updated = set.scores.getOrElse(member, 0.0) + call.double(1)
    if (updated.isNaN) throw new Reject(Errors.err("resulting score is not a number (NaN)"))
    call.replace(key, set.updated(member, updated))
    Frame.Double(updated)
  }

  private def zCount(call: Call): Frame = {
    val bound = scoreRange(call, 1, 2)
    int(sorted(call, call.key(0)).ordered.count(bound).toLong)
  }

  private def zLexCount(call: Call): Frame = {
    val bound = lexRange(call, 1, 2)
    int(sorted(call, call.key(0)).ordered.count(bound).toLong)
  }

  private def zmScore(call: Call): Frame = {
    val scores = sorted(call, call.key(0)).scores
    Frame.Array(call.args.indices.drop(1).toVector.map(i => scores.get(call.key(i)).fold(Frame.Null)(Frame.Double(_))))
  }

  private def zRank(call: Call, reverse: Boolean): Frame = {
    val withScore = call.args.length match {
      case 2                                   => false
      case 3 if call.keyword(2) == "WITHSCORE" => true
      case _                                   => throw new Reject(Errors.syntax)
    }
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
    if (names.nonEmpty) call.replace(key, names.foldLeft(set)(_.removed(_)))
    int(names.length.toLong)
  }

  private def removeWhere(call: Call, bound: Bound): Frame = {
    val key     = call.key(0)
    val set     = sorted(call, key)
    val removed = set.ordered.filter(bound)
    if (removed.nonEmpty) call.replace(key, removed.foldLeft(set)((acc, item) => acc.removed(item._2)))
    int(removed.size.toLong)
  }

  private def zRemRangeByRank(call: Call): Frame = {
    val key     = call.key(0)
    val set     = sorted(call, key)
    val all     = set.ordered.toVector
    val removed = Ranges.inclusive(call.long(1), call.long(2), all.length).fold(Vector.empty[Scored])((from, to) => all.slice(from, to + 1))
    if (removed.nonEmpty) call.replace(key, removed.foldLeft(set)((acc, item) => acc.removed(item._2)))
    int(removed.length.toLong)
  }

  /**
    * Parses the range of `ZRANGE` and `ZRANGESTORE`. `at` is the index of the start argument, and `allowScores` is false for
    * `ZRANGESTORE`, which has no `WITHSCORES` option. Returns the selected members in reply order and whether scores were requested.
    */
  private def selectRange(call: Call, source: Key, at: Int, allowScores: Boolean): (Vector[Scored], Boolean) = {
    var by         = "RANK"
    var rev        = false
    var limit      = Option.empty[(Long, Long)]
    var withScores = false
    var i          = at + 2
    while (i < call.args.length) {
      call.keyword(i) match {
        case "BYSCORE" if by == "RANK"           => by = "SCORE"
        case "BYLEX" if by == "RANK"             => by = "LEX"
        case "REV"                               => rev = true
        case "WITHSCORES" if allowScores         => withScores = true
        case "LIMIT" if i + 2 < call.args.length =>
          limit = Some((call.long(i + 1), call.long(i + 2)))
          i += 2
        case _                                   => throw new Reject(Errors.syntax)
      }
      i += 1
    }
    if (limit.isDefined && by == "RANK")
      throw new Reject(Errors.err("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"))
    if (withScores && by == "LEX") throw new Reject(Errors.err("syntax error, WITHSCORES not supported in combination with BYLEX"))
    val all        = sorted(call, source).ordered.toVector
    val ordered    = if (rev) all.reverse else all
    val selected   = by match {
      case "RANK" =>
        Ranges.inclusive(call.long(at), call.long(at + 1), ordered.length).fold(Vector.empty[Scored])((from, to) => ordered.slice(from, to + 1))
      case kind   =>
        // with REV the caller gives the upper bound first
        val (min, max) = if (rev) (at + 1, at) else (at, at + 1)
        val bound      = if (kind == "SCORE") scoreRange(call, min, max) else lexRange(call, min, max)
        val matching   = ordered.filter(bound)
        limit.fold(matching) { (offset, count) =>
          if (offset < 0) Vector.empty
          else {
            val skipped = matching.drop(math.min(offset, Int.MaxValue.toLong).toInt)
            if (count < 0) skipped else skipped.take(math.min(count, Int.MaxValue.toLong).toInt)
          }
        }
    }
    (selected, withScores)
  }

  private def zRange(call: Call): Frame = {
    val (selected, withScores) = selectRange(call, call.key(0), 1, allowScores = true)
    if (withScores) pairs(selected) else members(selected)
  }

  private def zRangeStore(call: Call): Frame = {
    val (selected, _) = selectRange(call, call.key(1), 2, allowScores = false)
    val destination   = call.key(0)
    call.store.delete(destination, call.now): Unit
    call.replace(destination, Sorted.of(selected.map(_.swap)))
    int(selected.length.toLong)
  }

  private def pop(call: Call, key: Key, count: Int, max: Boolean): Vector[Scored] = {
    val set   = sorted(call, key)
    val taken = (if (max) set.ordered.toVector.reverse else set.ordered.toVector).take(count)
    if (taken.nonEmpty) call.replace(key, taken.foldLeft(set)((acc, item) => acc.removed(item._2)))
    taken
  }

  private def popReply(call: Call, max: Boolean): Frame =
    call.args.length match {
      case 1 =>
        // without a count the reply is a flat [member, score] pair, or an empty array when there is nothing to pop
        pop(call, call.key(0), 1, max).headOption.fold(Frame.Array(Vector.empty)) { (score, member) =>
          Frame.Array(Vector(bulk(member.bytes), Frame.Double(score)))
        }
      case 2 =>
        val count = call.long(1)
        if (count < 0) throw new Reject(Errors.outOfRange)
        pairs(pop(call, call.key(0), math.min(count, Int.MaxValue.toLong).toInt, max))
      case _ => throw new Reject(Errors.syntax)
    }

  // BZPOPMIN and BZPOPMAX reply with the key, the member and its score
  private def blockingPop(call: Call, max: Boolean): Frame =
    call.args.indices.init.map(call.key).find(key => !sorted(call, key).isEmpty).fold(throw Blocked) { key =>
      val (score, member) = pop(call, key, 1, max).head
      Frame.Array(Vector(bulk(key.bytes), bulk(member.bytes), Frame.Double(score)))
    }

  // `at` is the index of numkeys, which BZMPOP puts after its timeout
  private def zmPop(call: Call, at: Int): Frame = {
    val (keys, next) = call.numKeys(at)
    if (next >= call.args.length) throw new Reject(Errors.syntax)
    val max          = call.keyword(next) match {
      case "MIN" => false
      case "MAX" => true
      case _     => throw new Reject(Errors.syntax)
    }
    val count        = call.args.length - next match {
      case 1                                      => 1L
      case 3 if call.keyword(next + 1) == "COUNT" =>
        val n = call.long(next + 2)
        if (n <= 0) throw new Reject(Errors.err("count should be greater than 0"))
        n
      case _                                      => throw new Reject(Errors.syntax)
    }
    keys.find(key => !sorted(call, key).isEmpty).fold(Frame.Null) { key =>
      Frame.Array(Vector(bulk(key.bytes), pairs(pop(call, key, math.min(count, Int.MaxValue.toLong).toInt, max))))
    }
  }

  private def zRandMember(call: Call): Frame = {
    val all = sorted(call, call.key(0)).ordered.toVector
    call.args.length match {
      case 1                                    => if (all.isEmpty) Frame.Null else bulk(all(Random.nextInt(all.length))._2.bytes)
      case 2                                    => members(Sampling.pick(all, call.long(1)))
      case 3 if call.keyword(2) == "WITHSCORES" => pairs(Sampling.pick(all, call.long(1)))
      case _                                    => throw new Reject(Errors.syntax)
    }
  }

  private def zScan(call: Call): Frame = {
    Scanning.cursor(call, 1)
    val (pattern, _) = Scanning.options(call, 2)
    val items        = sorted(call, call.key(0)).ordered.toVector.filter((_, member) => pattern.forall(Glob.matches(_, member.bytes)))
    Frame.Array(Vector(bulk("0"), Frame.Array(items.flatMap((score, member) => Vector(bulk(member.bytes), scoreText(score))))))
  }

  private enum Combine {
    case Union, Inter, Diff
  }

  // ZUNION, ZINTER and ZDIFF also accept plain sets, whose members count as having score 1
  private def scoresOf(call: Call, key: Key): Map[Key, Double] =
    call.lookup(key).map(_.value) match {
      case None                         => Map.empty
      case Some(set: Sorted)            => set.scores
      case Some(Value.Members(members)) => members.iterator.map(_ -> 1.0).toMap
      case Some(_)                      => throw new Reject(Errors.wrongType)
    }

  /**
    * Parses `numkeys key… [WEIGHTS w…] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]` starting at `at` and computes the result.
    */
  private def combined(call: Call, at: Int, kind: Combine, allowScores: Boolean): (Vector[Scored], Boolean) = {
    val (keys, next)                                    = call.numKeys(at)
    var weights                                         = Vector.fill(keys.length)(1.0)
    var aggregate                                       = "SUM"
    var withScores                                      = false
    var i                                               = next
    while (i < call.args.length) {
      call.keyword(i) match {
        case "WEIGHTS" if kind != Combine.Diff && i + keys.length < call.args.length =>
          weights = Vector.tabulate(keys.length)(w =>
            Numbers.parseDouble(call.args(i + 1 + w)).getOrElse(throw new Reject(Errors.err("weight value is not a float")))
          )
          i += keys.length
        case "AGGREGATE" if kind != Combine.Diff && i + 1 < call.args.length         =>
          aggregate = call.keyword(i + 1)
          if (!Set("SUM", "MIN", "MAX").contains(aggregate)) throw new Reject(Errors.syntax)
          i += 1
        case "WITHSCORES" if allowScores                                             => withScores = true
        case _                                                                       => throw new Reject(Errors.syntax)
      }
      i += 1
    }
    val inputs                                          = keys.map(scoresOf(call, _))
    // infinity times zero is NaN, which Redis turns into 0, and so is the sum of opposite infinities
    def weighted(score: Double, weight: Double): Double = { val w = score * weight; if (w.isNaN) 0.0 else w }
    def fold(a: Double, b: Double): Double              = aggregate match {
      case "MIN" => math.min(a, b)
      case "MAX" => math.max(a, b)
      case _     => val s = a + b; if (s.isNaN) 0.0 else s
    }
    val result                                          = kind match {
      case Combine.Diff  => inputs.head.filter((member, _) => !inputs.tail.exists(_.contains(member)))
      case Combine.Union =>
        inputs.zip(weights).foldLeft(Map.empty[Key, Double]) { case (acc, (scores, weight)) =>
          scores.foldLeft(acc) { case (inner, (member, score)) =>
            val value = weighted(score, weight)
            inner.updated(member, inner.get(member).fold(value)(fold(_, value)))
          }
        }
      case Combine.Inter =>
        inputs.head.keys
          .filter(member => inputs.forall(_.contains(member)))
          .map { member =>
            member -> inputs.zip(weights).map((scores, weight) => weighted(scores(member), weight)).reduce(fold)
          }
          .toMap
    }
    (Sorted.of(result).ordered.toVector, withScores)
  }

  private def combineReply(call: Call, kind: Combine): Frame = {
    val (result, withScores) = combined(call, 0, kind, allowScores = true)
    if (withScores) pairs(result) else members(result)
  }

  private def combineStore(call: Call, kind: Combine): Frame = {
    val (result, _) = combined(call, 1, kind, allowScores = false)
    val destination = call.key(0)
    call.store.delete(destination, call.now): Unit
    call.replace(destination, Sorted.of(result.map(_.swap)))
    int(result.length.toLong)
  }

  private def zInterCard(call: Call): Frame = {
    val (keys, next) = call.numKeys(0)
    val limit        = call.args.length - next match {
      case 0                                  => 0L
      case 2 if call.keyword(next) == "LIMIT" =>
        val n = call.long(next + 1)
        if (n < 0) throw new Reject(Errors.err("LIMIT can't be negative"))
        n
      case _                                  => throw new Reject(Errors.syntax)
    }
    val inputs       = keys.map(scoresOf(call, _))
    val size         = inputs.head.keys.count(member => inputs.forall(_.contains(member))).toLong
    int(if (limit == 0) size else math.min(size, limit))
  }
}

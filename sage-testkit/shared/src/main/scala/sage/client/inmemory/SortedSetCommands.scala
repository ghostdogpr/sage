package sage.client.inmemory

import sage.Bytes
import sage.protocol.Frame

private[inmemory] object SortedSetCommands {
  import Replies.*
  import Value.Sorted

  val specs: Map[String, Spec] = Map(
    "ZADD"             -> Spec(-4, zAdd),
    "ZCARD"            -> Spec(2, call => int(sorted(call, call.key(0)).scores.size.toLong)),
    "ZCOUNT"           -> Spec(4, call => countIn(call, range(call, 1, 2)(scoreBound))),
    "ZDIFF"            -> Spec(-3, combine(_, Combine.Diff, store = false)),
    "ZDIFFSTORE"       -> Spec(-4, combine(_, Combine.Diff, store = true)),
    "ZINCRBY"          -> Spec(4, call => add(call, Set("INCR"), Vector((call.double(1), call.key(2))))),
    "ZINTER"           -> Spec(-3, combine(_, Combine.Inter, store = false)),
    "ZINTERCARD"       -> Spec(-3, zInterCard),
    "ZINTERSTORE"      -> Spec(-4, combine(_, Combine.Inter, store = true)),
    "ZLEXCOUNT"        -> Spec(4, call => countIn(call, range(call, 1, 2)(lexBound))),
    "ZMPOP"            -> Spec(-4, call => zmPop(call, 0)),
    "BZMPOP"           -> Spec.blocking(-5, zmPop(_, 1))(call => Timeouts.seconds(call.args(0), call.now)),
    "BZPOPMAX"         -> Spec.blocking(-3, blockingPop(_, max = true))(call => Timeouts.seconds(call.args.last, call.now)),
    "BZPOPMIN"         -> Spec.blocking(-3, blockingPop(_, max = false))(call => Timeouts.seconds(call.args.last, call.now)),
    "ZMSCORE"          -> Spec(-3, zmScore),
    "ZPOPMAX"          -> Spec(-2, call => popReply(call, max = true)),
    "ZPOPMIN"          -> Spec(-2, call => popReply(call, max = false)),
    "ZRANDMEMBER"      -> Spec(-2, zRandMember),
    "ZRANGE"           -> Spec(-4, zRange(_, store = false)),
    "ZRANGESTORE"      -> Spec(-5, zRange(_, store = true)),
    "ZRANK"            -> Spec(-3, call => zRank(call, reverse = false)),
    "ZREM"             -> Spec(-3, zRem),
    "ZREMRANGEBYLEX"   -> Spec(4, call => removeWhere(call, range(call, 1, 2)(lexBound))),
    "ZREMRANGEBYRANK"  -> Spec(4, zRemRangeByRank),
    "ZREMRANGEBYSCORE" -> Spec(4, call => removeWhere(call, range(call, 1, 2)(scoreBound))),
    "ZREVRANK"         -> Spec(-3, call => zRank(call, reverse = true)),
    "ZSCAN"            -> Spec(-3, zScan),
    "ZSCORE"           -> Spec(3, call => sorted(call, call.key(0)).scores.get(call.key(1)).fold(Frame.Null)(Frame.Double(_))),
    "ZUNION"           -> Spec(-3, combine(_, Combine.Union, store = false)),
    "ZUNIONSTORE"      -> Spec(-4, combine(_, Combine.Union, store = true))
  )

  private type Scored = (Double, Key)

  private def sorted(call: Call, key: Key): Sorted = call.typed[Sorted](key).getOrElse(Sorted.empty)

  private def pairs(items: Iterable[Scored]): Frame =
    Frame.Array(items.iterator.map((score, member) => Frame.Array(Vector(bulk(member.bytes), Frame.Double(score)))).toVector)

  private def members(items: Iterable[Scored]): Frame = keys(items.view.map(_._2))

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

  private def range(call: Call, min: Int, max: Int)(bound: (Call, Int, Boolean) => Bound): Bound = {
    val low  = bound(call, min, true)
    val high = bound(call, max, false)
    item => low(item) && high(item)
  }

  private def zAdd(call: Call): Frame = {
    var flags = Set.empty[String]
    call.seek(1)
    while (call.hasNext && Set("NX", "XX", "GT", "LT", "CH", "INCR").contains(call.keyword(call.position))) flags += call.nextKeyword()
    if (flags("NX") && (flags("XX") || flags("GT") || flags("LT")) || flags("GT") && flags("LT"))
      throw new Reject(Errors.err("XX, NX, GT and LT options at the same time are not compatible"))
    val rest  = call.remaining
    if (rest == 0 || rest % 2 != 0) throw new Reject(Errors.syntax)
    if (flags("INCR") && rest != 2) throw new Reject(Errors.err("INCR option supports a single increment-element pair"))
    add(call, flags, Vector.fill(rest / 2)((call.nextDouble(), call.nextKey())))
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

  private def countIn(call: Call, bound: Bound): Frame = int(sorted(call, call.key(0)).ordered.count(bound).toLong)

  private def zmScore(call: Call): Frame = {
    val scores = sorted(call, call.key(0)).scores
    Frame.Array(call.args.indices.drop(1).toVector.map(i => scores.get(call.key(i)).fold(Frame.Null)(Frame.Double(_))))
  }

  private def zRank(call: Call, reverse: Boolean): Frame = {
    val withScore = call.optionalWord(2, "WITHSCORE", Errors.arity(call.name))
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

  private def removeWhere(call: Call, bound: Bound): Frame = int(remove(call, call.key(0))(_.ordered.filter(bound)).size.toLong)

  private def zRemRangeByRank(call: Call): Frame = {
    val (start, stop) = (call.long(1), call.long(2))
    int(remove(call, call.key(0))(set => Ranges.slice(set.ordered.toVector, start, stop)).length.toLong)
  }

  // ZRANGE, or ZRANGESTORE when `store` is true, which takes the destination first and has no WITHSCORES option
  private def zRange(call: Call, store: Boolean): Frame = {
    val at                                       = if (store) 2 else 1
    var by                                       = "RANK"
    var rev                                      = false
    var limit                                    = Option.empty[(Long, Long)]
    var withScores                               = false
    call.options(at + 2) {
      case "BYSCORE" if by == "RANK" => by = "SCORE"
      case "BYLEX" if by == "RANK"   => by = "LEX"
      case "REV"                     => rev = true
      case "WITHSCORES" if !store    => withScores = true
      case "LIMIT"                   =>
        call.need(2)
        limit = Some((call.nextLong(), call.nextLong()))
      case _                         => throw new Reject(Errors.syntax)
    }
    if (limit.isDefined && by == "RANK")
      throw new Reject(Errors.err("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"))
    if (withScores && by == "LEX") throw new Reject(Errors.err("syntax error, WITHSCORES not supported in combination with BYLEX"))
    // a server parses the range before it reads the key
    val select: Vector[Scored] => Vector[Scored] = by match {
      case "RANK" =>
        val (start, stop) = (call.long(at), call.long(at + 1))
        Ranges.slice(_, start, stop)
      case kind   =>
        // with REV the caller gives the upper bound first
        val (min, max) = if (rev) (at + 1, at) else (at, at + 1)
        val bound      = range(call, min, max)(if (kind == "SCORE") scoreBound else lexBound)
        ordered =>
          val matching = ordered.filter(bound)
          // a negative count takes everything after the offset
          limit.fold(matching) { (offset, count) =>
            val skipped = if (offset < 0) Vector.empty else matching.drop(Numbers.size(offset))
            if (count < 0) skipped else skipped.take(Numbers.size(count))
          }
    }
    val all                                      = sorted(call, call.key(at - 1)).ordered.toVector
    val selected                                 = select(if (rev) all.reverse else all)
    if (store) call.stored(call.key(0), Sorted.of(selected.map(_.swap)), selected.length)
    else if (withScores) pairs(selected)
    else members(selected)
  }

  private def pop(call: Call, key: Key, count: Int, max: Boolean): Vector[Scored] =
    remove(call, key)(set => (if (max) set.ordered.toVector.reverse else set.ordered.toVector).take(count))

  private def popReply(call: Call, max: Boolean): Frame =
    call.optional(1)(call.bounded(_, Errors.outOfRange)(_ >= 0)) match {
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

  // `at` is the index of numkeys, which BZMPOP puts after its timeout
  private def zmPop(call: Call, at: Int): Frame =
    ListCommands.multiPop(call, at, "MAX", "MIN") { (key, max, count) =>
      val taken = pop(call, key, count, max)
      Option.when(taken.nonEmpty)(pairs(taken))
    }

  private def zRandMember(call: Call): Frame = {
    val count = Sampling.count(call, "WITHSCORES")
    val all   = sorted(call, call.key(0)).ordered.toVector
    count.fold(optional(Sampling.one(all).map(_._2.bytes))) { (n, withScores) =>
      val picked = Sampling.pick(all, n)
      if (withScores) pairs(picked) else members(picked)
    }
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

  // the `numkeys key…` of ZUNION, ZINTER, ZDIFF, their STORE forms and ZINTERCARD, which need at least one key
  private def inputKeys(call: Call, at: Int): Vector[Key] = {
    val count = call.long(at)
    if (count < 1) throw new Reject(Errors.err(s"at least 1 input key is needed for '${call.name.toLowerCase(java.util.Locale.ROOT)}' command"))
    call.keys(at, count)
  }

  /**
    * Parses `numkeys key… [WEIGHTS w…] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]` and replies with the result, or stores it at the destination
    * before `numkeys` when `store` is true.
    */
  private def combine(call: Call, kind: Combine, store: Boolean): Frame = {
    val keys                                            = inputKeys(call, if (store) 1 else 0)
    // a server reads every input, with its type check, before the options
    val inputs                                          = keys.map(scoresOf(call, _))
    var weights                                         = Vector.fill(keys.length)(1.0)
    var aggregate                                       = "SUM"
    var withScores                                      = false
    call.options(call.position) {
      case "WEIGHTS" if kind != Combine.Diff   =>
        call.need(keys.length)
        weights = keys.map(_ => Numbers.parseDouble(call.next()).getOrElse(throw new Reject(Errors.err("weight value is not a float"))))
      case "AGGREGATE" if kind != Combine.Diff =>
        aggregate = call.nextKeyword()
        if (!Set("SUM", "MIN", "MAX", "COUNT").contains(aggregate)) throw new Reject(Errors.syntax)
      case "WITHSCORES" if !store              => withScores = true
      case _                                   => throw new Reject(Errors.syntax)
    }
    // infinity times zero is NaN, which Redis turns into 0, and so is the sum of opposite infinities; COUNT sums the weights alone
    def weighted(score: Double, weight: Double): Double = { val w = if (aggregate == "COUNT") weight else score * weight; if (w.isNaN) 0.0 else w }
    def fold(a: Double, b: Double): Double              = aggregate match {
      case "MIN" => math.min(a, b)
      case "MAX" => math.max(a, b)
      case _     => val s = a + b; if (s.isNaN) 0.0 else s
    }
    val result                                          = kind match {
      case Combine.Diff  => inputs.headOption.fold(Map.empty[Key, Double])(_.filter((member, _) => !inputs.drop(1).exists(_.contains(member))))
      case Combine.Union =>
        inputs.zip(weights).foldLeft(Map.empty[Key, Double]) { case (acc, (scores, weight)) =>
          scores.foldLeft(acc) { case (inner, (member, score)) =>
            val value = weighted(score, weight)
            inner.updated(member, inner.get(member).fold(value)(fold(_, value)))
          }
        }
      case Combine.Inter =>
        inputs
          .zip(weights)
          .foldLeft(Option.empty[Map[Key, Double]]) { case (acc, (scores, weight)) =>
            Some(acc.fold(scores.map((member, score) => member -> weighted(score, weight))) {
              _.flatMap((member, score) => scores.get(member).map(next => member -> fold(score, weighted(next, weight))))
            })
          }
          .getOrElse(Map.empty)
    }
    val set                                             = Sorted.of(result)
    if (store) call.stored(call.key(0), set, set.scores.size) else if (withScores) pairs(set.ordered) else members(set.ordered)
  }

  // unlike SINTERCARD, ZINTERCARD reads its inputs before its limit
  private def zInterCard(call: Call): Frame = {
    val inputs = inputKeys(call, 0).map(scoresOf(call, _))
    SetCommands.interCard(call)(inputs.headOption.fold(0L)(_.keys.count(member => inputs.forall(_.contains(member))).toLong))
  }
}

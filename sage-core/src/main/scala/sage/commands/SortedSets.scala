package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{Doubles, KeyCodec, ValueCodec}
import sage.commands.Args.{Ch, Gt, Lt, Nx, Rev, Xx}
import sage.protocol.Frame

/**
  * Conditions supported by `ZADD`. `Always` sends no condition. `IfNotExists` and `IfExists` send `NX` and `XX`. `IfGreater` and `IfLess`
  * send `GT` and `LT`; the combined cases add `XX`. The `changed` argument to [[SortedSets.zAdd]] controls `CH`. Use
  * [[SortedSets.zAddIncr]] for `INCR`.
  */
enum ZAddCondition {
  case Always
  case IfNotExists
  case IfExists
  case IfGreater
  case IfLess
  case IfExistsAndGreater
  case IfExistsAndLess
}

/**
  * Selects the score extreme for a multi-pop command. `ZMPOP` and `BZMPOP` use the same values.
  */
enum MinMax {
  case Min, Max
}

/**
  * How `ZUNION`/`ZINTER` (and their STORE forms) fold scores of shared members. `Sum` is the server default.
  */
enum Aggregate {
  case Sum, Min, Max
}

/**
  * A score boundary shared by `ZRANGE BYSCORE`, `ZCOUNT`, and `ZREMRANGEBYSCORE`.
  */
enum ScoreBoundary {
  case Inclusive(score: Double)
  case Exclusive(score: Double)
  case NegInf
  case PosInf
}

/**
  * A boundary in the lexicographic space, shared by every lex-ranged command (`ZRANGE BYLEX`, `ZLEXCOUNT`, `ZREMRANGEBYLEX`). `Min`/`Max`
  * are the open extremes (`-`/`+`); valid only when every member shares one score.
  */
enum LexBoundary[+V] {
  case Inclusive(value: V)
  case Exclusive(value: V)
  case Min
  case Max
}

/**
  * A `LIMIT offset count` clause for a range query: skip `offset` matches, then return at most `count` (a negative `count` means all).
  */
final case class Limit(offset: Long, count: Long)

object Limit {

  /**
    * The first match only: `LIMIT 0 1`.
    */
  val first: Limit = Limit(0, 1)

  /**
    * The first `count` matches from the start: `LIMIT 0 count`.
    */
  def take(count: Long): Limit = Limit(0, count)
}

/**
  * A `ZRANGE` query. The three modes contain only their supported options; a by-rank query has no `LIMIT`. Callers always provide `min` and
  * `max` from low to high. With `rev`, the encoder reverses them into the descending order required by `BYSCORE` and `BYLEX`. `ByLex` stores
  * the member type; `ByRank` and `ByScore` are `ZRange[Nothing]`.
  */
enum ZRange[+V] {
  case ByRank(start: Long, stop: Long, rev: Boolean = false)
  case ByScore(min: ScoreBoundary, max: ScoreBoundary, limit: Option[Limit] = None, rev: Boolean = false)
  case ByLex[V](min: LexBoundary[V], max: LexBoundary[V], limit: Option[Limit] = None, rev: Boolean = false) extends ZRange[V]
}

object ZRange {

  /**
    * Creates an inclusive score range `[min, max]`. Results use ascending order unless `rev` is true.
    */
  def scores(min: Double, max: Double, limit: Option[Limit] = None, rev: Boolean = false): ZRange[Nothing] =
    ByScore(ScoreBoundary.Inclusive(min), ScoreBoundary.Inclusive(max), limit, rev)

  /**
    * Selects every member whose score equals `value`.
    */
  def score(value: Double, limit: Option[Limit] = None, rev: Boolean = false): ZRange[Nothing] =
    scores(value, value, limit, rev)

  /**
    * Selects scores greater than or equal to `min`. Results use ascending order unless `rev` is true.
    */
  def atLeast(min: Double, limit: Option[Limit] = None, rev: Boolean = false): ZRange[Nothing] =
    ByScore(ScoreBoundary.Inclusive(min), ScoreBoundary.PosInf, limit, rev)

  /**
    * Selects scores less than or equal to `max`. Results use ascending order unless `rev` is true.
    */
  def atMost(max: Double, limit: Option[Limit] = None, rev: Boolean = false): ZRange[Nothing] =
    ByScore(ScoreBoundary.NegInf, ScoreBoundary.Inclusive(max), limit, rev)

  /**
    * An inclusive lexicographic band `[min, max]`, ascending unless `rev`. Valid only when every member shares one score.
    */
  def lex[V](min: V, max: V, limit: Option[Limit] = None, rev: Boolean = false): ZRange[V] =
    ByLex(LexBoundary.Inclusive(min), LexBoundary.Inclusive(max), limit, rev)
}

private[sage] object SortedSets {

  private val Incr          = Bytes.utf8("INCR")
  private val ByScore       = Bytes.utf8("BYSCORE")
  private val ByLex         = Bytes.utf8("BYLEX")
  private val WithScores    = Bytes.utf8("WITHSCORES")
  private val WithScore     = Bytes.utf8("WITHSCORE")
  private val WeightsWord   = Bytes.utf8("WEIGHTS")
  private val AggregateWord = Bytes.utf8("AGGREGATE")
  private val aggregateWord = Args.keywords(Aggregate.values)
  private val minMaxArg     = Args.keywords(MinMax.values)
  private val NegInfWord    = Bytes.utf8("-inf")
  private val PosInfWord    = Bytes.utf8("+inf")
  private val LexMin        = Bytes.utf8("-")
  private val LexMax        = Bytes.utf8("+")

  def zAdd[K, V](key: K, condition: ZAddCondition = ZAddCondition.Always, changed: Boolean = false)(
    first: (V, Double),
    rest: (V, Double)*
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command(
      "ZADD",
      Command.FirstKey,
      (keyCodec.encode(key) +: conditionArgs(condition)) ++ Args.flag(changed, Ch) ++ memberScoreArgs(first +: rest.toVector),
      Decode.long
    )

  def zAddIncr[K, V](key: K, condition: ZAddCondition = ZAddCondition.Always)(member: V, score: Double)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[Double]] =
    Command(
      "ZADD",
      Command.FirstKey,
      (keyCodec.encode(key) +: conditionArgs(condition)) ++ Vector(Incr, Args.double(score), valueCodec.encode(member)),
      Decode.optionalScore
    )

  def zCard[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("ZCARD", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def zScore[K, V](key: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[Double]] =
    Command.read("ZSCORE", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(member)), Decode.optionalScore)

  def zMScore[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[Double]]] =
    Command.read("ZMSCORE", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.vector(Decode.optionalScore))

  def zIncrBy[K, V](key: K, member: V, increment: Double)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Double] =
    Command("ZINCRBY", Command.FirstKey, Vector(keyCodec.encode(key), Args.double(increment), valueCodec.encode(member)), Decode.double)

  def zRank[K, V](key: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[Long]] =
    Command.read("ZRANK", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(member)), Decode.optionalLong)

  def zRankWithScore[K, V](key: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(Long, Double)]] =
    Command.read("ZRANK", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(member), WithScore), rankWithScore)

  def zRevRank[K, V](key: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[Long]] =
    Command.read("ZREVRANK", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(member)), Decode.optionalLong)

  def zRevRankWithScore[K, V](key: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(Long, Double)]] =
    Command.read("ZREVRANK", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(member), WithScore), rankWithScore)

  def zCount[K](key: K, min: ScoreBoundary, max: ScoreBoundary)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("ZCOUNT", Command.FirstKey, Vector(keyCodec.encode(key), scoreBoundaryArg(min), scoreBoundaryArg(max)), Decode.long)

  def zLexCount[K, V](key: K, min: LexBoundary[V], max: LexBoundary[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command.read("ZLEXCOUNT", Command.FirstKey, Vector(keyCodec.encode(key), lexBoundaryArg(min), lexBoundaryArg(max)), Decode.long)

  def zRange[K, V](key: K, range: ZRange[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command.read("ZRANGE", Command.FirstKey, keyCodec.encode(key) +: rangeArgs(range), Decode.vector(Decode.value[V]))

  def zRangeWithScores[K, V](key: K, range: ZRange[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[(V, Double)]] =
    Command.read("ZRANGE", Command.FirstKey, (keyCodec.encode(key) +: rangeArgs(range)) :+ WithScores, Decode.scoredMembers[V])

  def zRangeStore[K, V](destination: K, source: K, range: ZRange[V])(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command(
      "ZRANGESTORE",
      Vector(0, 1),
      Vector(keyCodec.encode(destination), keyCodec.encode(source)) ++ rangeArgs(range),
      Decode.long
    )

  def zRem[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Long] =
    Command("ZREM", Command.FirstKey, Args.keyThen(key, first, rest)(valueCodec.encode), Decode.long)

  def zRemRangeByRank[K](key: K, start: Long, stop: Long)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("ZREMRANGEBYRANK", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(start), Args.long(stop)), Decode.long)

  def zRemRangeByScore[K](key: K, min: ScoreBoundary, max: ScoreBoundary)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("ZREMRANGEBYSCORE", Command.FirstKey, Vector(keyCodec.encode(key), scoreBoundaryArg(min), scoreBoundaryArg(max)), Decode.long)

  def zRemRangeByLex[K, V](key: K, min: LexBoundary[V], max: LexBoundary[V])(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Long] =
    Command("ZREMRANGEBYLEX", Command.FirstKey, Vector(keyCodec.encode(key), lexBoundaryArg(min), lexBoundaryArg(max)), Decode.long)

  def zPopMin[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(V, Double)]] =
    Command("ZPOPMIN", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalScoredMember[V])

  def zPopMax[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(V, Double)]] =
    Command("ZPOPMAX", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalScoredMember[V])

  def zPopMinCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[(V, Double)]] =
    Command("ZPOPMIN", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count)), Decode.scoredMembers[V])

  def zPopMaxCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[(V, Double)]] =
    Command("ZPOPMAX", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count)), Decode.scoredMembers[V])

  def zMpop[K, V](first: K, rest: K*)(minMax: MinMax, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[(K, Vector[(V, Double)])]] =
    KeyArgs.multiPop("ZMPOP", None, first +: rest.toVector, minMaxArg(minMax), count, Decode.scoredMembers[V], MpopLabel)

  def bzPopMin[K, V](first: K, rest: K*)(
    timeout: BlockTimeout
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(K, V, Double)]] =
    KeyArgs.blockingPop("BZPOPMIN", first +: rest.toVector, timeout, poppedMember[K, V])

  def bzPopMax[K, V](first: K, rest: K*)(
    timeout: BlockTimeout
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(K, V, Double)]] =
    KeyArgs.blockingPop("BZPOPMAX", first +: rest.toVector, timeout, poppedMember[K, V])

  def bzMpop[K, V](first: K, rest: K*)(minMax: MinMax, timeout: BlockTimeout, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[(K, Vector[(V, Double)])]] =
    KeyArgs.multiPop("BZMPOP", Some(timeout), first +: rest.toVector, minMaxArg(minMax), count, Decode.scoredMembers[V], MpopLabel)

  def zRandMember[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command.readUncacheable("ZRANDMEMBER", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def zRandMemberCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command.readUncacheable("ZRANDMEMBER", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(count)), Decode.vector(Decode.value[V]))

  def zRandMemberWithScores[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[(V, Double)]] =
    Command.readUncacheable(
      "ZRANDMEMBER",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Args.long(count), WithScores),
      Decode.scoredMembers[V]
    )

  def zUnion[K, V](first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using KeyCodec[K],
    ValueCodec[V]
  ): Command[Vector[V]] =
    combine("ZUNION", first +: rest.toVector, weights, aggregate, withScores = false, Decode.vector(Decode.value[V]))

  def zUnionWithScores[K, V](first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using KeyCodec[K],
    ValueCodec[V]
  ): Command[Vector[(V, Double)]] =
    combine("ZUNION", first +: rest.toVector, weights, aggregate, withScores = true, Decode.scoredMembers[V])

  def zUnionStore[K](destination: K, first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using keyCodec: KeyCodec[K]
  ): Command[Long] = {
    val (indices, prefix) = KeyArgs.numKeyedAfter(keyCodec.encode(destination), first +: rest.toVector)
    Command("ZUNIONSTORE", 0 +: indices, prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate), Decode.long)
  }

  def zInter[K, V](first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using KeyCodec[K],
    ValueCodec[V]
  ): Command[Vector[V]] =
    combine("ZINTER", first +: rest.toVector, weights, aggregate, withScores = false, Decode.vector(Decode.value[V]))

  def zInterWithScores[K, V](first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using KeyCodec[K],
    ValueCodec[V]
  ): Command[Vector[(V, Double)]] =
    combine("ZINTER", first +: rest.toVector, weights, aggregate, withScores = true, Decode.scoredMembers[V])

  def zInterStore[K](destination: K, first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using keyCodec: KeyCodec[K]
  ): Command[Long] = {
    val (indices, prefix) = KeyArgs.numKeyedAfter(keyCodec.encode(destination), first +: rest.toVector)
    Command("ZINTERSTORE", 0 +: indices, prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate), Decode.long)
  }

  def zInterCard[K](first: K, rest: K*)(limit: Option[Long] = None)(using keyCodec: KeyCodec[K]): Command[Long] =
    KeyArgs.interCard("ZINTERCARD", first +: rest.toVector, limit)

  def zDiff[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] = {
    val (indices, prefix) = KeyArgs.numKeyed(first +: rest.toVector)
    Command.read("ZDIFF", indices, prefix, Decode.vector(Decode.value[V]))
  }

  def zDiffWithScores[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[(V, Double)]] = {
    val (indices, prefix) = KeyArgs.numKeyed(first +: rest.toVector)
    Command.read("ZDIFF", indices, prefix :+ WithScores, Decode.scoredMembers[V])
  }

  def zDiffStore[K](destination: K, first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] = {
    val (indices, prefix) = KeyArgs.numKeyedAfter(keyCodec.encode(destination), first +: rest.toVector)
    Command("ZDIFFSTORE", 0 +: indices, prefix, Decode.long)
  }

  def zScan[K, V](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[ScanPage[(V, Double)]] =
    KeyArgs.keyScan("ZSCAN", key, cursor, pattern, count)(Decode.scoredMembersFlat[V])

  private def poppedMember[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, (K, V, Double)] =
    Decode.array3(Decode.key[K], Decode.value[V], Decode.double, "key, member and score or null")((_, _, _))

  private val MpopLabel = "key and members or null"

  private val rankWithScore: Frame => Either[DecodeError, Option[(Long, Double)]] =
    Decode.nullable(Decode.array2(Decode.long, Decode.double, "rank/score pair or null")(_ -> _))

  private def memberScoreArgs[V](pairs: Vector[(V, Double)])(using valueCodec: ValueCodec[V]): Vector[Bytes] =
    pairs.flatMap { case (member, score) => Vector(Args.double(score), valueCodec.encode(member)) }

  private def conditionArgs(condition: ZAddCondition): Vector[Bytes] =
    condition match {
      case ZAddCondition.Always             => Vector.empty
      case ZAddCondition.IfNotExists        => Vector(Nx)
      case ZAddCondition.IfExists           => Vector(Xx)
      case ZAddCondition.IfGreater          => Vector(Gt)
      case ZAddCondition.IfLess             => Vector(Lt)
      case ZAddCondition.IfExistsAndGreater => Vector(Xx, Gt)
      case ZAddCondition.IfExistsAndLess    => Vector(Xx, Lt)
    }

  private def rangeArgs[V](range: ZRange[V])(using valueCodec: ValueCodec[V]): Vector[Bytes] =
    range match {
      case ZRange.ByRank(start, stop, rev)      =>
        Vector(Args.long(start), Args.long(stop)) ++ Args.flag(rev, Rev)
      case ZRange.ByScore(min, max, limit, rev) => bounded(scoreBoundaryArg(min), scoreBoundaryArg(max), ByScore, limit, rev)
      case ZRange.ByLex(min, max, limit, rev)   => bounded(lexBoundaryArg[V](min), lexBoundaryArg[V](max), ByLex, limit, rev)
    }

  private def bounded(min: Bytes, max: Bytes, by: Bytes, limit: Option[Limit], rev: Boolean): Vector[Bytes] =
    (if (rev) Vector(max, min, by, Rev) else Vector(min, max, by)) ++ Args.limit(limit)

  private def scoreBoundaryArg(boundary: ScoreBoundary): Bytes =
    boundary match {
      case ScoreBoundary.Inclusive(s) => Args.double(s)
      case ScoreBoundary.Exclusive(s) => Bytes.utf8("(" + Doubles.format(s))
      case ScoreBoundary.NegInf       => NegInfWord
      case ScoreBoundary.PosInf       => PosInfWord
    }

  private def lexBoundaryArg[V](boundary: LexBoundary[V])(using valueCodec: ValueCodec[V]): Bytes =
    boundary match {
      case LexBoundary.Inclusive(v) => prefixed('[', valueCodec.encode(v))
      case LexBoundary.Exclusive(v) => prefixed('(', valueCodec.encode(v))
      case LexBoundary.Min          => LexMin
      case LexBoundary.Max          => LexMax
    }

  private def aggregateArgs(aggregate: Aggregate): Vector[Bytes] =
    if (aggregate == Aggregate.Sum) Vector.empty else Vector(AggregateWord, aggregateWord(aggregate))

  private def combine[K: KeyCodec, Out](
    name: String,
    keys: Vector[K],
    weights: Option[Vector[Double]],
    aggregate: Aggregate,
    withScores: Boolean,
    decode: Frame => Either[DecodeError, Out]
  ): Command[Out] = {
    val (indices, prefix) = KeyArgs.numKeyed(keys)
    Command.read(name, indices, prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate) ++ Args.flag(withScores, WithScores), decode)
  }

  private def weightsArgs(weights: Option[Vector[Double]]): Vector[Bytes] =
    weights.toVector.flatMap(ws => WeightsWord +: ws.map(Args.double))

  private def prefixed(prefix: Char, value: Bytes): Bytes = {
    val src = value.unsafeArray
    val out = new Array[Byte](src.length + 1)
    out(0) = prefix.toByte
    System.arraycopy(src, 0, out, 1, src.length)
    Bytes.wrap(IArray.unsafeFromArray(out))
  }
}

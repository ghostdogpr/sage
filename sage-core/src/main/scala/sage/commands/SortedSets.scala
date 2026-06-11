package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.{Doubles, KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * ZADD's mutually-exclusive option space, flattened to exactly its legal states: NX/XX never combine, GT/LT never combine, NX forbids
  * any GT/LT, while XX may pair with either. CH (changed-count return) is a separate boolean; INCR (which changes the reply to the new
  * score) is the separate [[SortedSets.zAddIncr]] builder.
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
  * Which score extreme a multi-pop acts on. A cross-command domain primitive shared identically by `ZMPOP` and `BZMPOP`, like [[ListSide]].
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
  * A boundary in the score space, shared by every score-ranged command (`ZRANGE BYSCORE`, `ZCOUNT`, `ZREMRANGEBYSCORE`): one domain
  * primitive with one legal space wherever it appears, the [[ListSide]] exception to per-command enums.
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

final case class Limit(offset: Long, count: Long)

/**
  * A `ZRANGE` query. The three modes carry exactly their legal options — a by-rank query has no `LIMIT` — and `min`/`max` are always given
  * low-to-high: under `rev` the encoder emits them in the descending wire order `BYSCORE`/`BYLEX` require, so the bound-swap foot-gun is
  * unrepresentable. `ByLex` carries the member type; `ByRank`/`ByScore` are `ZRange[Nothing]`.
  */
enum ZRange[+V] {
  case ByRank(start: Long, stop: Long, rev: Boolean = false)
  case ByScore(min: ScoreBoundary, max: ScoreBoundary, limit: Option[Limit] = None, rev: Boolean = false)
  case ByLex[V](min: LexBoundary[V], max: LexBoundary[V], limit: Option[Limit] = None, rev: Boolean = false) extends ZRange[V]
}

private[sage] object SortedSets {

  private val Nx            = Bytes.utf8("NX")
  private val Xx            = Bytes.utf8("XX")
  private val Gt            = Bytes.utf8("GT")
  private val Lt            = Bytes.utf8("LT")
  private val Ch            = Bytes.utf8("CH")
  private val Incr          = Bytes.utf8("INCR")
  private val ByScore       = Bytes.utf8("BYSCORE")
  private val ByLex         = Bytes.utf8("BYLEX")
  private val Rev           = Bytes.utf8("REV")
  private val LimitWord     = Bytes.utf8("LIMIT")
  private val WithScores    = Bytes.utf8("WITHSCORES")
  private val WithScore     = Bytes.utf8("WITHSCORE")
  private val WeightsWord   = Bytes.utf8("WEIGHTS")
  private val AggregateWord = Bytes.utf8("AGGREGATE")
  private val MinWord       = Bytes.utf8("MIN")
  private val MaxWord       = Bytes.utf8("MAX")
  private val CountWord     = Bytes.utf8("COUNT")
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
      (keyCodec.encode(key) +: conditionArgs(condition)) ++ (if (changed) Vector(Ch) else Vector.empty) ++ memberScoreArgs(first +: rest.toVector),
      Decode.long
    )

  // returns the member's new score, or None when the condition (NX/XX/GT/LT) skipped the write
  def zAddIncr[K, V](key: K, member: V, score: Double, condition: ZAddCondition = ZAddCondition.Always)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[Double]] =
    Command(
      "ZADD",
      Command.FirstKey,
      (keyCodec.encode(key) +: conditionArgs(condition)) ++ Vector(Incr, scoreArg(score), valueCodec.encode(member)),
      Decode.optionalScore
    )

  def zCard[K](key: K)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("ZCARD", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.long)

  def zScore[K, V](key: K, member: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[Double]] =
    Command.read("ZSCORE", Command.FirstKey, Vector(keyCodec.encode(key), valueCodec.encode(member)), Decode.optionalScore)

  def zMScore[K, V](key: K, first: V, rest: V*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[Option[Double]]] =
    Command.read(
      "ZMSCORE",
      Command.FirstKey,
      keyCodec.encode(key) +: (first +: rest.toVector).map(valueCodec.encode),
      Decode.vector(Decode.optionalScore)
    )

  def zIncrBy[K, V](key: K, member: V, increment: Double)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Double] =
    Command("ZINCRBY", Command.FirstKey, Vector(keyCodec.encode(key), scoreArg(increment), valueCodec.encode(member)), Decode.score)

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
    Command("ZREM", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).map(valueCodec.encode), Decode.long)

  def zRemRangeByRank[K](key: K, start: Long, stop: Long)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command("ZREMRANGEBYRANK", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(start.toString), Bytes.utf8(stop.toString)), Decode.long)

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
    Command("ZPOPMIN", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.scoredMembers[V])

  def zPopMaxCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[(V, Double)]] =
    Command("ZPOPMAX", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.scoredMembers[V])

  def zMpop[K, V](first: K, rest: K*)(minMax: MinMax, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[(K, Vector[(V, Double)])]] = {
    val (indices, prefix) = numKeyed(first +: rest.toVector)
    Command("ZMPOP", indices, (prefix :+ minMaxArg(minMax)) ++ countArg(count), mpopReply[K, V])
  }

  def bzPopMin[K, V](first: K, rest: K*)(
    timeout: BlockTimeout
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(K, V, Double)]] =
    blockingPop("BZPOPMIN", first, rest.toVector, timeout)

  def bzPopMax[K, V](first: K, rest: K*)(
    timeout: BlockTimeout
  )(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[(K, V, Double)]] =
    blockingPop("BZPOPMAX", first, rest.toVector, timeout)

  def bzMpop[K, V](first: K, rest: K*)(minMax: MinMax, timeout: BlockTimeout, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[(K, Vector[(V, Double)])]] = {
    val keys = (first +: rest.toVector).map(keyCodec.encode)
    Command(
      "BZMPOP",
      keyIndices = Vector.tabulate(keys.size)(_ + 2),
      args = (BlockTimeout.wire(timeout) +: Bytes.utf8(keys.size.toString) +: keys :+ minMaxArg(minMax)) ++ countArg(count),
      decode = mpopReply[K, V],
      execution = Execution.Blocking
    )
  }

  def zRandMember[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command.readUncacheable("ZRANDMEMBER", Command.FirstKey, Vector(keyCodec.encode(key)), Decode.optionalValue)

  def zRandMemberCount[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] =
    Command.readUncacheable("ZRANDMEMBER", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(count.toString)), Decode.vector(Decode.value[V]))

  def zRandMemberWithScores[K, V](key: K, count: Long)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[(V, Double)]] =
    Command.readUncacheable(
      "ZRANDMEMBER",
      Command.FirstKey,
      Vector(keyCodec.encode(key), Bytes.utf8(count.toString), WithScores),
      Decode.scoredMembers[V]
    )

  def zUnion[K, V](first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[V]] = {
    val (indices, prefix) = numKeyed(first +: rest.toVector)
    Command.read("ZUNION", indices, prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate), Decode.vector(Decode.value[V]))
  }

  def zUnionWithScores[K, V](first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[(V, Double)]] = {
    val (indices, prefix) = numKeyed(first +: rest.toVector)
    Command.read("ZUNION", indices, (prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate)) :+ WithScores, Decode.scoredMembers[V])
  }

  def zUnionStore[K](destination: K, first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using keyCodec: KeyCodec[K]
  ): Command[Long] = {
    val (indices, prefix) = storeKeyed(destination, first +: rest.toVector)
    Command("ZUNIONSTORE", indices, prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate), Decode.long)
  }

  def zInter[K, V](first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[V]] = {
    val (indices, prefix) = numKeyed(first +: rest.toVector)
    Command.read("ZINTER", indices, prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate), Decode.vector(Decode.value[V]))
  }

  def zInterWithScores[K, V](first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Vector[(V, Double)]] = {
    val (indices, prefix) = numKeyed(first +: rest.toVector)
    Command.read("ZINTER", indices, (prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate)) :+ WithScores, Decode.scoredMembers[V])
  }

  def zInterStore[K](destination: K, first: K, rest: K*)(weights: Option[Vector[Double]] = None, aggregate: Aggregate = Aggregate.Sum)(
    using keyCodec: KeyCodec[K]
  ): Command[Long] = {
    val (indices, prefix) = storeKeyed(destination, first +: rest.toVector)
    Command("ZINTERSTORE", indices, prefix ++ weightsArgs(weights) ++ aggregateArgs(aggregate), Decode.long)
  }

  def zInterCard[K](first: K, rest: K*)(limit: Option[Long] = None)(using keyCodec: KeyCodec[K]): Command[Long] = {
    val (indices, prefix) = numKeyed(first +: rest.toVector)
    Command.read("ZINTERCARD", indices, prefix ++ limit.toVector.flatMap(n => Vector(LimitWord, Bytes.utf8(n.toString))), Decode.long)
  }

  def zDiff[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[V]] = {
    val (indices, prefix) = numKeyed(first +: rest.toVector)
    Command.read("ZDIFF", indices, prefix, Decode.vector(Decode.value[V]))
  }

  def zDiffWithScores[K, V](first: K, rest: K*)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Vector[(V, Double)]] = {
    val (indices, prefix) = numKeyed(first +: rest.toVector)
    Command.read("ZDIFF", indices, prefix :+ WithScores, Decode.scoredMembers[V])
  }

  def zDiffStore[K](destination: K, first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Long] = {
    val (indices, prefix) = storeKeyed(destination, first +: rest.toVector)
    Command("ZDIFFSTORE", indices, prefix, Decode.long)
  }

  def zScan[K, V](key: K, cursor: ScanCursor, pattern: Option[String] = None, count: Option[Long] = None)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[ScanPage[(V, Double)]] =
    Command.read(
      "ZSCAN",
      Command.FirstKey,
      Vector(keyCodec.encode(key), ScanCursor.bytes(cursor)) ++ ScanArgs.options(pattern, count),
      Decode.scanPage(Decode.scoredMembersFlat[V])
    )

  private def blockingPop[K, V](name: String, first: K, rest: Vector[K], timeout: BlockTimeout)(
    using keyCodec: KeyCodec[K],
    valueCodec: ValueCodec[V]
  ): Command[Option[(K, V, Double)]] = {
    val keys = (first +: rest).map(keyCodec.encode)
    Command(
      name,
      keyIndices = Vector.tabulate(keys.size)(identity),
      args = keys :+ BlockTimeout.wire(timeout),
      decode = {
        case Frame.Null                                             => Right(None)
        case Frame.Array(Vector(keyFrame, memberFrame, scoreFrame)) =>
          for {
            key    <- Decode.key[K](keyFrame)
            member <- Decode.value[V](memberFrame)
            s      <- Decode.score(scoreFrame)
          } yield Some((key, member, s))
        case other                                                  => Left(DecodeError("key, member and score or null", Frame.describe(other)))
      },
      execution = Execution.Blocking
    )
  }

  private def mpopReply[K, V](using KeyCodec[K], ValueCodec[V]): Frame => Either[DecodeError, Option[(K, Vector[(V, Double)])]] = {
    case Frame.Null                                  => Right(None)
    case Frame.Array(Vector(keyFrame, membersFrame)) =>
      for {
        key     <- Decode.key[K](keyFrame)
        members <- Decode.scoredMembers[V](membersFrame)
      } yield Some((key, members))
    case other                                       => Left(DecodeError("key and members or null", Frame.describe(other)))
  }

  private val rankWithScore: Frame => Either[DecodeError, Option[(Long, Double)]] = {
    case Frame.Null                                 => Right(None)
    case Frame.Array(Vector(rankFrame, scoreFrame)) =>
      for {
        rank <- Decode.long(rankFrame)
        s    <- Decode.score(scoreFrame)
      } yield Some((rank, s))
    case other                                      => Left(DecodeError("rank/score pair or null", Frame.describe(other)))
  }

  private def numKeyed[K](keys: Vector[K])(using keyCodec: KeyCodec[K]): (Vector[Int], Vector[Bytes]) = {
    val encoded = keys.map(keyCodec.encode)
    (Vector.tabulate(encoded.size)(_ + 1), Bytes.utf8(encoded.size.toString) +: encoded)
  }

  private def storeKeyed[K](destination: K, keys: Vector[K])(using keyCodec: KeyCodec[K]): (Vector[Int], Vector[Bytes]) = {
    val encoded = keys.map(keyCodec.encode)
    val indices = 0 +: Vector.tabulate(encoded.size)(_ + 2)
    (indices, keyCodec.encode(destination) +: Bytes.utf8(encoded.size.toString) +: encoded)
  }

  private def memberScoreArgs[V](pairs: Vector[(V, Double)])(using valueCodec: ValueCodec[V]): Vector[Bytes] =
    pairs.flatMap { case (member, score) => Vector(scoreArg(score), valueCodec.encode(member)) }

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
        Vector(Bytes.utf8(start.toString), Bytes.utf8(stop.toString)) ++ (if (rev) Vector(Rev) else Vector.empty)
      case ZRange.ByScore(min, max, limit, rev) =>
        val (a, b) = if (rev) (max, min) else (min, max)
        (Vector(scoreBoundaryArg(a), scoreBoundaryArg(b), ByScore) ++ (if (rev) Vector(Rev) else Vector.empty)) ++ limitArgs(limit)
      case ZRange.ByLex(min, max, limit, rev)   =>
        val (a, b) = if (rev) (max, min) else (min, max)
        (Vector(lexBoundaryArg[V](a), lexBoundaryArg[V](b), ByLex) ++ (if (rev) Vector(Rev) else Vector.empty)) ++ limitArgs(limit)
    }

  private def limitArgs(limit: Option[Limit]): Vector[Bytes] =
    limit.toVector.flatMap(l => Vector(LimitWord, Bytes.utf8(l.offset.toString), Bytes.utf8(l.count.toString)))

  private def scoreBoundaryArg(boundary: ScoreBoundary): Bytes =
    boundary match {
      case ScoreBoundary.Inclusive(s) => Bytes.utf8(formatScore(s))
      case ScoreBoundary.Exclusive(s) => Bytes.utf8("(" + formatScore(s))
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
    aggregate match {
      case Aggregate.Sum => Vector.empty
      case Aggregate.Min => Vector(AggregateWord, MinWord)
      case Aggregate.Max => Vector(AggregateWord, MaxWord)
    }

  private def weightsArgs(weights: Option[Vector[Double]]): Vector[Bytes] =
    weights.toVector.flatMap(ws => WeightsWord +: ws.map(w => Bytes.utf8(formatScore(w))))

  private def countArg(count: Option[Long]): Vector[Bytes] =
    count.toVector.flatMap(c => Vector(CountWord, Bytes.utf8(c.toString)))

  private def minMaxArg(minMax: MinMax): Bytes =
    minMax match {
      case MinMax.Min => MinWord
      case MinMax.Max => MaxWord
    }

  private def scoreArg(score: Double): Bytes = Bytes.utf8(formatScore(score))

  private def formatScore(value: Double): String = Doubles.format(value)

  private def prefixed(prefix: Char, value: Bytes): Bytes = {
    val src = value.unsafeArray
    val out = new Array[Byte](src.length + 1)
    out(0) = prefix.toByte
    System.arraycopy(src, 0, out, 1, src.length)
    Bytes.wrap(IArray.unsafeFromArray(out))
  }
}

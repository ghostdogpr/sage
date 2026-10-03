package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.{Aggregate, BlockTimeout, LexBoundary, Limit, MinMax, ScanCursor, ScoreBoundary, ZAddCondition, ZRange}
import sage.integration.BothServersSuite

class SortedSetsSuite extends BothServersSuite {

  clientTest("ZADD ZCARD ZSCORE ZMSCORE ZINCRBY and ZREM manage scored members") { client =>
    client.zAdd("zset-basic")(("a", 1.0), ("b", 2.0), ("c", 3.0)).is(3L) >>
      client.zCard("zset-basic").is(3L) >>
      client.zScore[String]("zset-basic", "b").is(Some(2.0)) >>
      client.zMScore[String]("zset-basic", "a", "missing", "c").is(Vector(Some(1.0), None, Some(3.0))) >>
      client.zIncrBy("zset-basic", "a", 1.5).is(2.5) >>
      client.zRem("zset-basic", "c", "missing").is(1L)
  }

  clientTest("ZADD conditions gate writes and ZADD INCR returns the new score or None") { client =>
    client.zAdd("zset-cond")(("a", 5.0)) >>
      client.zAdd("zset-cond", ZAddCondition.IfExists)(("b", 1.0)).is(0L) >>
      client.zAdd("zset-cond", ZAddCondition.IfNotExists)(("b", 1.0)).is(1L) >>
      client.zAddIncr("zset-cond", ZAddCondition.IfExists)("a", -1.0).is(Some(4.0)) >>
      client.zAddIncr("zset-cond", ZAddCondition.IfExists)("c", 1.0).is(None) >>
      client.zAddIncr("zset-cond", ZAddCondition.IfExistsAndGreater)("a", -1.0).is(None)
  }

  clientTest("ZRANK and ZREVRANK report position, with the score on request") { client =>
    client.zAdd("zset-rank")(("a", 1.0), ("b", 2.0), ("c", 3.0)) >>
      client.zRank[String]("zset-rank", "b").is(Some(1L)) >>
      client.zRevRank[String]("zset-rank", "b").is(Some(1L)) >>
      client.zRankWithScore[String]("zset-rank", "c").is(Some((2L, 3.0))) >>
      client.zRank[String]("zset-rank", "z").is(None)
  }

  clientTest("ZRANGE reads by rank and score, with scores, reversal, and a limit") { client =>
    client.zAdd("zset-range")(("a", 1.0), ("b", 2.0), ("c", 3.0)) >>
      client.zRange[String]("zset-range", ZRange.ByRank(0L, -1L)).is(Vector("a", "b", "c")) >>
      client.zRange[String]("zset-range", ZRange.ByRank(0L, -1L, rev = true)).is(Vector("c", "b", "a")) >>
      client.zRange[String]("zset-range", ZRange.ByScore(ScoreBoundary.Inclusive(2.0), ScoreBoundary.PosInf)).is(Vector("b", "c")) >>
      client
        .zRange[String]("zset-range", ZRange.ByScore(ScoreBoundary.Inclusive(1.0), ScoreBoundary.Inclusive(2.0), rev = true))
        .is(Vector("b", "a")) >>
      client.zRangeWithScores[String]("zset-range", ZRange.ByRank(0L, 1L)).is(Vector("a" -> 1.0, "b" -> 2.0)) >>
      client
        .zRange[String]("zset-range", ZRange.ByScore(ScoreBoundary.NegInf, ScoreBoundary.PosInf, limit = Some(Limit(1L, 1L))))
        .is(Vector("b"))
  }

  clientTest("ZRANGE BYLEX and ZLEXCOUNT operate on equal-score members") { client =>
    client.zAdd("zset-lex")(("a", 0.0), ("b", 0.0), ("c", 0.0)) >>
      client.zRange[String]("zset-lex", ZRange.ByLex(LexBoundary.Min, LexBoundary.Max)).is(Vector("a", "b", "c")) >>
      client.zRange[String]("zset-lex", ZRange.ByLex(LexBoundary.Inclusive("a"), LexBoundary.Exclusive("c"))).is(Vector("a", "b")) >>
      client.zLexCount[String]("zset-lex", LexBoundary.Min, LexBoundary.Max).is(3L)
  }

  clientTest("ZRANGESTORE copies a range into another key") { client =>
    client.zAdd("zrs-src")(("a", 1.0), ("b", 2.0), ("c", 3.0)) >>
      client.zRangeStore[String]("zrs-dst", "zrs-src", ZRange.ByScore(ScoreBoundary.Inclusive(2.0), ScoreBoundary.PosInf)).is(2L) >>
      client.zRange[String]("zrs-dst", ZRange.ByRank(0L, -1L)).is(Vector("b", "c"))
  }

  clientTest("ZCOUNT counts members within a score band") { client =>
    client.zAdd("zset-count")(("a", 1.0), ("b", 2.0), ("c", 3.0)) >>
      client.zCount("zset-count", ScoreBoundary.NegInf, ScoreBoundary.PosInf).is(3L) >>
      client.zCount("zset-count", ScoreBoundary.Exclusive(1.0), ScoreBoundary.Inclusive(3.0)).is(2L)
  }

  clientTest("ZPOPMIN ZPOPMAX and ZMPOP pop by score extreme") { client =>
    client.zAdd("zset-pop")(("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)) >>
      client.zPopMin[String]("zset-pop").is(Some(("a", 1.0))) >>
      client.zPopMax[String]("zset-pop").is(Some(("d", 4.0))) >>
      client.zPopMinCount[String]("zset-pop", 2L).is(Vector("b" -> 2.0, "c" -> 3.0)) >>
      client.zPopMin[String]("zset-pop").is(None) >>
      client.zAdd("zset-mpop")(("x", 1.0), ("y", 2.0)) >>
      client.zMpop[String]("zset-empty", "zset-mpop")(MinMax.Min, count = Some(2L)).is(Some(("zset-mpop", Vector("x" -> 1.0, "y" -> 2.0))))
  }

  clientTest("BZPOPMIN and BZMPOP pop present data and time out to None otherwise") { client =>
    client.zAdd("bz-data")(("a", 1.0), ("b", 2.0)) >>
      client.bzPopMin[String]("bz-data")(BlockTimeout.After(1.second)).is(Some(("bz-data", "a", 1.0))) >>
      client.bzMpop[String]("bz-empty", "bz-data")(MinMax.Max, BlockTimeout.After(1.second)).is(Some(("bz-data", Vector("b" -> 2.0)))) >>
      client.bzPopMin[String]("bz-missing")(BlockTimeout.After(100.millis)).is(None)
  }

  clientTest("ZRANDMEMBER draws a member, a count, and scored pairs") { client =>
    client.zAdd("zset-rand")(("a", 1.0), ("b", 2.0), ("c", 3.0)) >>
      client.zRandMember[String]("zset-rand").satisfies(_.exists(Set("a", "b", "c"))) >>
      client.zRandMemberCount[String]("zset-rand", -5L).map(_.size).is(5) >>
      client.zRandMemberWithScores[String]("zset-rand", 3L).map(_.toMap).is(Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0))
  }

  clientTest("ZUNION ZINTER ZDIFF combine sorted sets with weights and aggregation") { client =>
    client.zAdd("zops-a")(("x", 1.0), ("y", 2.0)) >>
      client.zAdd("zops-b")(("y", 3.0), ("z", 4.0)) >>
      client.zUnionWithScores[String]("zops-a", "zops-b")().map(_.toMap).is(Map("x" -> 1.0, "y" -> 5.0, "z" -> 4.0)) >>
      client
        .zUnionWithScores[String]("zops-a", "zops-b")(weights = Some(Vector(1.0, 2.0)))
        .map(_.toMap)
        .is(Map("x" -> 1.0, "y" -> 8.0, "z" -> 8.0)) >>
      client.zUnionWithScores[String]("zops-a", "zops-b")(aggregate = Aggregate.Max).map(_.toMap).is(Map("x" -> 1.0, "y" -> 3.0, "z" -> 4.0)) >>
      client.zInter[String]("zops-a", "zops-b")().is(Vector("y")) >>
      client.zDiff[String]("zops-a", "zops-b").is(Vector("x")) >>
      client.zInterCard("zops-a", "zops-b")().is(1L) >>
      client.zUnionStore("zops-union", "zops-a", "zops-b")().is(3L) >>
      client.zInterStore("zops-inter", "zops-a", "zops-b")().is(1L) >>
      client.zDiffStore("zops-diff", "zops-a", "zops-b").is(1L)
  }

  clientTest("ZREMRANGEBYRANK BYSCORE and BYLEX trim ranges") { client =>
    client.zAdd("zrem-rank")(("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)) >>
      client.zRemRangeByRank("zrem-rank", 0L, 0L).is(1L) >>
      client.zAdd("zrem-score")(("a", 1.0), ("b", 2.0), ("c", 3.0)) >>
      client.zRemRangeByScore("zrem-score", ScoreBoundary.Inclusive(2.0), ScoreBoundary.PosInf).is(2L) >>
      client.zAdd("zrem-lex")(("a", 0.0), ("b", 0.0), ("c", 0.0)) >>
      client.zRemRangeByLex[String]("zrem-lex", LexBoundary.Inclusive("a"), LexBoundary.Inclusive("b")).is(2L)
  }

  clientTest("ZSCAN streams member/score pairs") { client =>
    client.zAdd("zset-scan")(("a", 1.0), ("b", 2.0), ("c", 3.0)) >>
      client.zScan[String]("zset-scan", ScanCursor.start).map(_.items.toMap).is(Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0))
  }

  clientTest("ZUNION ZINTER WITHSCORES, ZDIFF WITHSCORES, and ZREVRANK WITHSCORE return members with their scores") { client =>
    client.zAdd("zgap-a")(("x", 1.0), ("y", 2.0)) >>
      client.zAdd("zgap-b")(("y", 3.0), ("z", 4.0)) >>
      client.zUnion[String]("zgap-a", "zgap-b")().map(_.toSet).is(Set("x", "y", "z")) >>
      client.zInterWithScores[String]("zgap-a", "zgap-b")().is(Vector("y" -> 5.0)) >>
      client.zDiffWithScores[String]("zgap-a", "zgap-b").is(Vector("x" -> 1.0)) >>
      client.zRevRankWithScore[String]("zgap-a", "x").is(Some((1L, 1.0)))
  }

  clientTest("ZPOPMAX with a count and BZPOPMAX pop the highest-scored members") { client =>
    client.zAdd("zgap-pop")(("a", 1.0), ("b", 2.0), ("c", 3.0)) >>
      client.zPopMaxCount[String]("zgap-pop", 2L).is(Vector("c" -> 3.0, "b" -> 2.0)) >>
      client.zAdd("zgap-bz")(("a", 1.0), ("b", 2.0)) >>
      client.bzPopMax[String]("zgap-bz")(BlockTimeout.After(1.second)).is(Some(("zgap-bz", "b", 2.0))) >>
      client.bzPopMax[String]("zgap-bz-missing")(BlockTimeout.After(100.millis)).is(None)
  }
}

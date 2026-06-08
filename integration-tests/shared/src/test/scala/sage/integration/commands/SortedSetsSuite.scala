package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.{Aggregate, BlockTimeout, LexBoundary, Limit, MinMax, ScanCursor, ScoreBoundary, ZAddCondition, ZRange}
import sage.integration.{Images, ServerSuite}

abstract class SortedSetsSuite(image: String) extends ServerSuite(image) {

  test("ZADD ZCARD ZSCORE ZMSCORE ZINCRBY and ZREM manage scored members") {
    withClient { client =>
      for {
        added   <- client.zAdd("zset-basic")(("a", 1.0), ("b", 2.0), ("c", 3.0))
        card    <- client.zCard("zset-basic")
        score   <- client.zScore[String, String]("zset-basic", "b")
        scores  <- client.zMScore[String, String]("zset-basic", "a", "missing", "c")
        bumped  <- client.zIncrBy("zset-basic", "a", 1.5)
        removed <- client.zRem("zset-basic", "c", "missing")
      } yield {
        assertEquals(added, 3L)
        assertEquals(card, 3L)
        assertEquals(score, Some(2.0))
        assertEquals(scores, Vector(Some(1.0), None, Some(3.0)))
        assertEquals(bumped, 2.5)
        assertEquals(removed, 1L)
      }
    }
  }

  test("ZADD conditions gate writes and ZADD INCR returns the new score or None") {
    withClient { client =>
      for {
        _        <- client.zAdd("zset-cond")(("a", 5.0))
        skipNew  <- client.zAdd("zset-cond", ZAddCondition.IfExists)(("b", 1.0))
        addedNew <- client.zAdd("zset-cond", ZAddCondition.IfNotExists)(("b", 1.0))
        lower    <- client.zAddIncr("zset-cond", "a", -1.0, ZAddCondition.IfExists)
        skipped  <- client.zAddIncr("zset-cond", "c", 1.0, ZAddCondition.IfExists)
        gtNoBump <- client.zAddIncr("zset-cond", "a", -1.0, ZAddCondition.IfExistsAndGreater)
      } yield {
        assertEquals(skipNew, 0L)
        assertEquals(addedNew, 1L)
        assertEquals(lower, Some(4.0))
        assertEquals(skipped, None)
        assertEquals(gtNoBump, None)
      }
    }
  }

  test("ZRANK and ZREVRANK report position, with the score on request") {
    withClient { client =>
      for {
        _         <- client.zAdd("zset-rank")(("a", 1.0), ("b", 2.0), ("c", 3.0))
        rank      <- client.zRank[String, String]("zset-rank", "b")
        rev       <- client.zRevRank[String, String]("zset-rank", "b")
        withScore <- client.zRankWithScore[String, String]("zset-rank", "c")
        missing   <- client.zRank[String, String]("zset-rank", "z")
      } yield {
        assertEquals(rank, Some(1L))
        assertEquals(rev, Some(1L))
        assertEquals(withScore, Some((2L, 3.0)))
        assertEquals(missing, None)
      }
    }
  }

  test("ZRANGE reads by rank and score, with scores, reversal, and a limit") {
    withClient { client =>
      for {
        _          <- client.zAdd("zset-range")(("a", 1.0), ("b", 2.0), ("c", 3.0))
        byRank     <- client.zRange[String, String]("zset-range", ZRange.ByRank(0L, -1L))
        revRank    <- client.zRange[String, String]("zset-range", ZRange.ByRank(0L, -1L, rev = true))
        byScore    <- client.zRange[String, String]("zset-range", ZRange.ByScore(ScoreBoundary.Inclusive(2.0), ScoreBoundary.PosInf))
        revScore   <-
          client.zRange[String, String]("zset-range", ZRange.ByScore(ScoreBoundary.Inclusive(1.0), ScoreBoundary.Inclusive(2.0), rev = true))
        withScores <- client.zRangeWithScores[String, String]("zset-range", ZRange.ByRank(0L, 1L))
        limited    <-
          client.zRange[String, String]("zset-range", ZRange.ByScore(ScoreBoundary.NegInf, ScoreBoundary.PosInf, limit = Some(Limit(1L, 1L))))
      } yield {
        assertEquals(byRank, Vector("a", "b", "c"))
        assertEquals(revRank, Vector("c", "b", "a"))
        assertEquals(byScore, Vector("b", "c"))
        assertEquals(revScore, Vector("b", "a"))
        assertEquals(withScores, Vector("a" -> 1.0, "b" -> 2.0))
        assertEquals(limited, Vector("b"))
      }
    }
  }

  test("ZRANGE BYLEX and ZLEXCOUNT operate on equal-score members") {
    withClient { client =>
      for {
        _     <- client.zAdd("zset-lex")(("a", 0.0), ("b", 0.0), ("c", 0.0))
        all   <- client.zRange[String, String]("zset-lex", ZRange.ByLex(LexBoundary.Min, LexBoundary.Max))
        range <- client.zRange[String, String]("zset-lex", ZRange.ByLex(LexBoundary.Inclusive("a"), LexBoundary.Exclusive("c")))
        count <- client.zLexCount[String, String]("zset-lex", LexBoundary.Min, LexBoundary.Max)
      } yield {
        assertEquals(all, Vector("a", "b", "c"))
        assertEquals(range, Vector("a", "b"))
        assertEquals(count, 3L)
      }
    }
  }

  test("ZRANGESTORE copies a range into another key") {
    withClient { client =>
      for {
        _      <- client.zAdd("zrs-src")(("a", 1.0), ("b", 2.0), ("c", 3.0))
        n      <- client.zRangeStore[String, String]("zrs-dst", "zrs-src", ZRange.ByScore(ScoreBoundary.Inclusive(2.0), ScoreBoundary.PosInf))
        stored <- client.zRange[String, String]("zrs-dst", ZRange.ByRank(0L, -1L))
      } yield {
        assertEquals(n, 2L)
        assertEquals(stored, Vector("b", "c"))
      }
    }
  }

  test("ZCOUNT counts members within a score band") {
    withClient { client =>
      for {
        _    <- client.zAdd("zset-count")(("a", 1.0), ("b", 2.0), ("c", 3.0))
        all  <- client.zCount("zset-count", ScoreBoundary.NegInf, ScoreBoundary.PosInf)
        band <- client.zCount("zset-count", ScoreBoundary.Exclusive(1.0), ScoreBoundary.Inclusive(3.0))
      } yield {
        assertEquals(all, 3L)
        assertEquals(band, 2L)
      }
    }
  }

  test("ZPOPMIN ZPOPMAX and ZMPOP pop by score extreme") {
    withClient { client =>
      for {
        _       <- client.zAdd("zset-pop")(("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0))
        min     <- client.zPopMin[String, String]("zset-pop")
        max     <- client.zPopMax[String, String]("zset-pop")
        twoMin  <- client.zPopMinCount[String, String]("zset-pop", 2L)
        emptied <- client.zPopMin[String, String]("zset-pop")
        _       <- client.zAdd("zset-mpop")(("x", 1.0), ("y", 2.0))
        mpopped <- client.zMpop[String, String]("zset-empty", "zset-mpop")(MinMax.Min, count = Some(2L))
      } yield {
        assertEquals(min, Some(("a", 1.0)))
        assertEquals(max, Some(("d", 4.0)))
        assertEquals(twoMin, Vector("b" -> 2.0, "c" -> 3.0))
        assertEquals(emptied, None)
        assertEquals(mpopped, Some(("zset-mpop", Vector("x" -> 1.0, "y" -> 2.0))))
      }
    }
  }

  test("BZPOPMIN and BZMPOP pop present data and time out to None otherwise") {
    withClient { client =>
      for {
        _      <- client.zAdd("bz-data")(("a", 1.0), ("b", 2.0))
        popped <- client.bzPopMin[String, String]("bz-data")(BlockTimeout.After(1.second))
        mpop   <- client.bzMpop[String, String]("bz-empty", "bz-data")(MinMax.Max, BlockTimeout.After(1.second))
        none   <- client.bzPopMin[String, String]("bz-missing")(BlockTimeout.After(100.millis))
      } yield {
        assertEquals(popped, Some(("bz-data", "a", 1.0)))
        assertEquals(mpop, Some(("bz-data", Vector("b" -> 2.0))))
        assertEquals(none, None)
      }
    }
  }

  test("ZRANDMEMBER draws a member, a count, and scored pairs") {
    withClient { client =>
      for {
        _      <- client.zAdd("zset-rand")(("a", 1.0), ("b", 2.0), ("c", 3.0))
        one    <- client.zRandMember[String, String]("zset-rand")
        dup    <- client.zRandMemberCount[String, String]("zset-rand", -5L)
        scored <- client.zRandMemberWithScores[String, String]("zset-rand", 3L)
      } yield {
        assert(one.exists(Set("a", "b", "c")))
        assertEquals(dup.size, 5)
        assertEquals(scored.toMap, Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0))
      }
    }
  }

  test("ZUNION ZINTER ZDIFF combine sorted sets with weights and aggregation") {
    withClient { client =>
      for {
        _        <- client.zAdd("zops-a")(("x", 1.0), ("y", 2.0))
        _        <- client.zAdd("zops-b")(("y", 3.0), ("z", 4.0))
        union    <- client.zUnionWithScores[String, String]("zops-a", "zops-b")()
        weighted <- client.zUnionWithScores[String, String]("zops-a", "zops-b")(weights = Some(Vector(1.0, 2.0)))
        maxAgg   <- client.zUnionWithScores[String, String]("zops-a", "zops-b")(aggregate = Aggregate.Max)
        inter    <- client.zInter[String, String]("zops-a", "zops-b")()
        diff     <- client.zDiff[String, String]("zops-a", "zops-b")
        card     <- client.zInterCard("zops-a", "zops-b")()
        unionN   <- client.zUnionStore("zops-union", "zops-a", "zops-b")()
        interN   <- client.zInterStore("zops-inter", "zops-a", "zops-b")()
        diffN    <- client.zDiffStore("zops-diff", "zops-a", "zops-b")
      } yield {
        assertEquals(union.toMap, Map("x" -> 1.0, "y" -> 5.0, "z" -> 4.0))
        assertEquals(weighted.toMap, Map("x" -> 1.0, "y" -> 8.0, "z" -> 8.0))
        assertEquals(maxAgg.toMap, Map("x" -> 1.0, "y" -> 3.0, "z" -> 4.0))
        assertEquals(inter, Vector("y"))
        assertEquals(diff, Vector("x"))
        assertEquals(card, 1L)
        assertEquals(unionN, 3L)
        assertEquals(interN, 1L)
        assertEquals(diffN, 1L)
      }
    }
  }

  test("ZREMRANGEBYRANK BYSCORE and BYLEX trim ranges") {
    withClient { client =>
      for {
        _       <- client.zAdd("zrem-rank")(("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0))
        byRank  <- client.zRemRangeByRank("zrem-rank", 0L, 0L)
        _       <- client.zAdd("zrem-score")(("a", 1.0), ("b", 2.0), ("c", 3.0))
        byScore <- client.zRemRangeByScore("zrem-score", ScoreBoundary.Inclusive(2.0), ScoreBoundary.PosInf)
        _       <- client.zAdd("zrem-lex")(("a", 0.0), ("b", 0.0), ("c", 0.0))
        byLex   <- client.zRemRangeByLex[String, String]("zrem-lex", LexBoundary.Inclusive("a"), LexBoundary.Inclusive("b"))
      } yield {
        assertEquals(byRank, 1L)
        assertEquals(byScore, 2L)
        assertEquals(byLex, 2L)
      }
    }
  }

  test("ZSCAN streams member/score pairs") {
    withClient { client =>
      for {
        _    <- client.zAdd("zset-scan")(("a", 1.0), ("b", 2.0), ("c", 3.0))
        page <- client.zScan[String, String]("zset-scan", ScanCursor.start)
      } yield assertEquals(page.items.toMap, Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0))
    }
  }
}

class RedisSortedSetsSuite extends SortedSetsSuite(Images.redis)

class ValkeySortedSetsSuite extends SortedSetsSuite(Images.valkey)

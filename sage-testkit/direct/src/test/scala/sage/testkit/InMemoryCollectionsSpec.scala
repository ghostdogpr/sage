package sage.testkit

import scala.concurrent.duration.*

import sage.*
import sage.SageException.ServerError
import sage.backend.*

class InMemoryCollectionsSpec extends munit.FunSuite {

  private def withClient[A](body: SageClient => A): A = {
    val client = InMemorySageClient()
    try body(client)
    finally client.close
  }

  private def wrongType(body: => Any): Unit = assertEquals(intercept[ServerError](body).code, "WRONGTYPE")

  test("hashes store, read, count and delete fields") {
    withClient { client =>
      assertEquals(client.hSet("user", "name" -> "Ada", "lang" -> "scala"), 2L)
      assertEquals(client.hSet("user", "name" -> "Ada L", "year" -> "1815"), 1L)
      assertEquals(client.hGet[String, String]("user", "name"), Some("Ada L"))
      assertEquals(client.hmGet[String, String]("user", "lang", "missing"), Vector(Some("scala"), None))
      assertEquals(client.hGetAll[String, String]("user"), Map("name" -> "Ada L", "lang" -> "scala", "year" -> "1815"))
      assertEquals(client.hKeys[String]("user"), Vector("name", "lang", "year"))
      assertEquals(client.hLen("user"), 3L)
      assertEquals(client.hStrLen("user", "lang"), 5L)
      assert(!client.hSetNx("user", "lang", "java"))
      assertEquals(client.hIncrBy("user", "year", 10L), 1825L)
      assertEquals(client.hIncrByFloat("user", "score", 1.5), 1.5)
      assertEquals(client.hDel("user", "name", "lang", "year", "score"), 4L)
      assertEquals(client.exists("user"), 0L)
      assertEquals(client.hScanAll[String, String]("missing").toVector, Vector.empty)
    }
  }

  test("hash fields expire independently") {
    withClockedClient { (client, clock) =>
      client.hSet("h", "a" -> "1", "b" -> "2"): Unit
      assertEquals(client.hExpire("h", 10.seconds)("a", "missing"), Vector(FieldExpiry.Updated, FieldExpiry.NoField))
      assertEquals(client.hTtl("h")("a", "b"), Vector(FieldTtl.Expires(10.seconds), FieldTtl.NoExpiry))
      assertEquals(client.hExpire("h", 20.seconds, ExpireCondition.IfLess)("a"), Vector(FieldExpiry.ConditionNotMet))
      clock.advance(10.seconds)
      assertEquals(client.hGetAll[String, String]("h"), Map("b" -> "2"))
      assertEquals(client.hSetEx("h", SetExpiry.In(5.seconds), HSetExCondition.IfNoneExist)("c" -> "3"), true)
      assertEquals(client.hPersist("h")("c", "b"), Vector(FieldPersist.Persisted, FieldPersist.NoExpiry))
      assertEquals(client.hGetEx[String, String]("h", GetExpiry.In(1.second))("b"), Vector(Some("2")))
      assertEquals(client.hGetDel[String, String]("h")("c"), Vector(Some("3")))
      clock.advance(1.second)
      assertEquals(client.exists("h"), 0L)
    }
  }

  test("lists push, pop, index and trim like Redis") {
    withClient { client =>
      assertEquals(client.rPush("l", "b", "c"), 2L)
      assertEquals(client.lPush("l", "a", "z"), 4L)
      assertEquals(client.lRange[String]("l", 0L, -1L), Vector("z", "a", "b", "c"))
      assertEquals(client.lPop[String]("l"), Some("z"))
      assertEquals(client.rPopCount[String]("l", 2L), Vector("c", "b"))
      assertEquals(client.lPushX("missing", "x"), 0L)
      client.rPush("l", "b", "a", "c", "a"): Unit
      assertEquals(client.lIndex[String]("l", -1L), Some("a"))
      assertEquals(client.lPos("l", "a", rank = Some(2L)), Some(2L))
      assertEquals(client.lPosCount("l", "a", count = 0L), Vector(0L, 2L, 4L))
      assertEquals(client.lInsert("l", InsertPosition.Before, "c", "x"), 6L)
      assertEquals(client.lRem("l", -1L, "a"), 1L)
      client.lSet("l", 0L, "first")
      client.lTrim("l", 0L, 2L)
      assertEquals(client.lRange[String]("l", 0L, -1L), Vector("first", "b", "a"))
      assertEquals(client.lMove[String]("l", "other", ListSide.Left, ListSide.Right), Some("first"))
      assertEquals(client.lMpop[String]("empty", "l")(ListSide.Right, Some(5L)), Some(("l", Vector("a", "b"))))
      assertEquals(client.exists("l"), 0L)
      assertEquals(intercept[ServerError](client.lSet("l", 0L, "x")).detail, "no such key")
    }
  }

  test("sets support membership and algebra") {
    withClient { client =>
      assertEquals(client.sAdd("a", 1, 2, 3, 3), 3L)
      client.sAdd("b", 2, 3, 4): Unit
      assertEquals(client.sMembers[Int]("a"), Set(1, 2, 3))
      assertEquals(client.sMisMember("a", 1, 5), Vector(true, false))
      assertEquals(client.sInter[Int]("a", "b"), Set(2, 3))
      assertEquals(client.sUnion[Int]("a", "b"), Set(1, 2, 3, 4))
      assertEquals(client.sDiff[Int]("a", "b"), Set(1))
      assertEquals(client.sInterCard("a", "b")(Some(1L)), 1L)
      assertEquals(client.sUnionStore("u", "a", "b"), 4L)
      assert(client.sMove("a", "b", 1))
      assertEquals(client.sCard("b"), 4L)
      assertEquals(client.sPopCount[Int]("a", 10L), Set(2, 3))
      assertEquals(client.exists("a"), 0L)
      assertEquals(client.sRandMemberCount[Int]("u", -6L).length, 6)
      assertEquals(client.sScanAll[Int]("u").toSet, Set(1, 2, 3, 4))
    }
  }

  test("sorted sets order members by score and support ranges") {
    withClient { client =>
      assertEquals(client.zAdd("z")("a" -> 1.0, "b" -> 2.0, "c" -> 3.0), 3L)
      assertEquals(client.zAdd("z", ZAddCondition.IfGreater, changed = true)("a" -> 0.5, "b" -> 5.0), 1L)
      assertEquals(client.zAddIncr("z")("c", 1.5), Some(4.5))
      assertEquals(client.zRangeWithScores[String]("z", ZRange.ByRank(0L, -1L)), Vector("a" -> 1.0, "c" -> 4.5, "b" -> 5.0))
      assertEquals(client.zRange[String]("z", ZRange.ByRank(0L, 0L, rev = true)), Vector("b"))
      assertEquals(client.zRange[String]("z", ZRange.ByScore(ScoreBoundary.Exclusive(1.0), ScoreBoundary.PosInf)), Vector("c", "b"))
      assertEquals(client.zRange[String]("z", ZRange.atLeast(1.0, limit = Some(Limit(1L, 1L)), rev = true)), Vector("c"))
      assertEquals(client.zScore("z", "c"), Some(4.5))
      assertEquals(client.zRank("z", "c"), Some(1L))
      assertEquals(client.zRevRankWithScore("z", "a"), Some((2L, 1.0)))
      assertEquals(client.zCount("z", ScoreBoundary.NegInf, ScoreBoundary.Inclusive(4.5)), 2L)
      assertEquals(client.zIncrBy("z", "a", 10.0), 11.0)
      assertEquals(client.zPopMin[String]("z"), Some("c" -> 4.5))
      assertEquals(client.zPopMaxCount[String]("z", 5L), Vector("a" -> 11.0, "b" -> 5.0))
      assertEquals(client.zPopMin[String]("z"), None)
      assertEquals(client.exists("z"), 0L)
    }
  }

  test("sorted sets support lexicographic ranges, removal and set operations") {
    withClient { client =>
      client.zAdd("letters")("a" -> 0.0, "b" -> 0.0, "c" -> 0.0, "d" -> 0.0): Unit
      assertEquals(client.zRange[String]("letters", ZRange.lex("b", "c")), Vector("b", "c"))
      assertEquals(client.zLexCount[String]("letters", LexBoundary.Exclusive("a"), LexBoundary.Max), 3L)
      assertEquals(client.zRemRangeByLex[String]("letters", LexBoundary.Min, LexBoundary.Inclusive("a")), 1L)
      assertEquals(client.zRemRangeByRank("letters", -1L, -1L), 1L)
      client.zAdd("x")("m" -> 1.0, "n" -> 2.0): Unit
      client.zAdd("y")("n" -> 10.0, "o" -> 20.0): Unit
      client.sAdd("plain", "m"): Unit
      assertEquals(client.zUnionWithScores[String]("x", "y")(), Vector("m" -> 1.0, "n" -> 12.0, "o" -> 20.0))
      assertEquals(client.zInterWithScores[String]("x", "y")(Some(Vector(2.0, 1.0)), Aggregate.Max), Vector("n" -> 10.0))
      assertEquals(client.zDiff[String]("x", "y"), Vector("m"))
      assertEquals(client.zUnionWithScores[String]("x", "plain")(aggregate = Aggregate.Sum), Vector("m" -> 2.0, "n" -> 2.0))
      assertEquals(client.zInterStore("both", "x", "y")(), 1L)
      assertEquals(client.zRangeStore[String]("top", "y", ZRange.ByRank(0L, 0L, rev = true)), 1L)
      assertEquals(client.zRange[String]("top", ZRange.ByRank(0L, -1L)), Vector("o"))
      assertEquals(client.zMpop[String]("missing", "x")(MinMax.Max), Some(("x", Vector("n" -> 2.0))))
      assertEquals(client.zScanAll[String]("y").toVector, Vector("n" -> 10.0, "o" -> 20.0))
    }
  }

  test("every collection command checks the type of the key") {
    withClient { client =>
      client.set("s", "v"): Unit
      wrongType(client.hGet[String, String]("s", "f"))
      wrongType(client.lPush("s", "x"))
      wrongType(client.sAdd("s", "x"))
      wrongType(client.zAdd("s")("x" -> 1.0))
      wrongType(client.get[String] { client.rPush("list", "x"): Unit; "list" })
      assertEquals(client.typeOf("list"), Some(RedisType.List))
      client.zAdd("zs")("x" -> 1.0): Unit
      assertEquals(client.typeOf("zs"), Some(RedisType.ZSet))
    }
  }

  test("collections keep the key's expiry when they change") {
    withClockedClient { (client, clock) =>
      client.rPush("l", "a"): Unit
      client.expire("l", 10.seconds): Unit
      client.rPush("l", "b"): Unit
      assertEquals(client.ttl("l"), Ttl.Expires(10.seconds))
      clock.advance(10.seconds)
      assertEquals(client.lLen("l"), 0L)
    }
  }

  private def withClockedClient[A](body: (SageClient, TestClock.Manual) => A): A = {
    val clock  = TestClock.manual()
    val client = InMemorySageClient(clock)
    try body(client, clock)
    finally client.close
  }
}

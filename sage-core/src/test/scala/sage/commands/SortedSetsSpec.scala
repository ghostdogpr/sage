package sage.commands

import sage.Bytes
import sage.protocol.Frame

class SortedSetsSpec extends munit.FunSuite {

  private def bulk(value: String): Frame                 = Frame.BulkString(Bytes.utf8(value))
  private def pair(member: String, score: Double): Frame = Frame.Array(Vector(bulk(member), Frame.Double(score)))

  test("ZSCORE decodes a RESP3 double, null as None, and rejects a bulk string") {
    assertEquals(Reply.run(SortedSets.zScore[String, String]("z", "a"), Frame.Double(1.5)), Right(Some(1.5)))
    assertEquals(Reply.run(SortedSets.zScore[String, String]("z", "a"), Frame.Null), Right(None))
    assert(Reply.run(SortedSets.zScore[String, String]("z", "a"), bulk("1.5")).isLeft)
  }

  test("ZADD INCR decodes the new score or None when the condition skipped the write") {
    assertEquals(Reply.run(SortedSets.zAddIncr("z", "a", 1.0), Frame.Double(3.0)), Right(Some(3.0)))
    assertEquals(Reply.run(SortedSets.zAddIncr("z", "a", 1.0, ZAddCondition.IfNotExists), Frame.Null), Right(None))
  }

  test("ZMSCORE decodes the score array, keeping missing members as None") {
    val reply = Frame.Array(Vector(Frame.Double(1.0), Frame.Null, Frame.Double(2.5)))
    assertEquals(Reply.run(SortedSets.zMScore[String, String]("z", "a", "b", "c"), reply), Right(Vector(Some(1.0), None, Some(2.5))))
  }

  test("ZRANGE WITHSCORES decodes the nested member/score pairs") {
    val reply = Frame.Array(Vector(pair("a", 1.0), pair("b", 2.0)))
    assertEquals(Reply.run(SortedSets.zRangeWithScores[String, String]("z", ZRange.ByRank(0L, -1L)), reply), Right(Vector("a" -> 1.0, "b" -> 2.0)))
  }

  test("ZPOPMIN decodes a flat member/score, an empty array as None, and a count as nested pairs") {
    assertEquals(Reply.run(SortedSets.zPopMin[String, String]("z"), Frame.Array(Vector(bulk("a"), Frame.Double(1.0)))), Right(Some("a" -> 1.0)))
    assertEquals(Reply.run(SortedSets.zPopMin[String, String]("z"), Frame.Array(Vector.empty)), Right(None))
    val counted = Frame.Array(Vector(pair("a", 1.0), pair("b", 2.0)))
    assertEquals(Reply.run(SortedSets.zPopMinCount[String, String]("z", 2L), counted), Right(Vector("a" -> 1.0, "b" -> 2.0)))
  }

  test("ZRANK WITHSCORE decodes the rank/score pair or None") {
    val reply = Frame.Array(Vector(Frame.Integer(2), Frame.Double(5.0)))
    assertEquals(Reply.run(SortedSets.zRankWithScore[String, String]("z", "a"), reply), Right(Some((2L, 5.0))))
    assertEquals(Reply.run(SortedSets.zRankWithScore[String, String]("z", "a"), Frame.Null), Right(None))
  }

  test("ZMPOP decodes null as None and a key with its scored members") {
    assertEquals(Reply.run(SortedSets.zMpop[String, String]("a", "b")(MinMax.Min), Frame.Null), Right(None))
    val reply = Frame.Array(Vector(bulk("a"), Frame.Array(Vector(pair("x", 1.0)))))
    assertEquals(Reply.run(SortedSets.zMpop[String, String]("a", "b")(MinMax.Min), reply), Right(Some(("a", Vector("x" -> 1.0)))))
  }

  test("BZPOPMIN decodes null as None and the key, member, score triple") {
    assertEquals(Reply.run(SortedSets.bzPopMin[String, String]("a")(BlockTimeout.Forever), Frame.Null), Right(None))
    val reply = Frame.Array(Vector(bulk("a"), bulk("m"), Frame.Double(1.5)))
    assertEquals(Reply.run(SortedSets.bzPopMin[String, String]("a")(BlockTimeout.Forever), reply), Right(Some(("a", "m", 1.5))))
  }

  test("ZSCAN decodes the flat member/score array with string scores, infinity included") {
    val reply = Frame.Array(Vector(bulk("0"), Frame.Array(Vector(bulk("a"), bulk("1.5"), bulk("b"), bulk("inf")))))
    Reply.run(SortedSets.zScan[String, String]("z", ScanCursor.start), reply) match {
      case Right(page) =>
        assertEquals(page.items, Vector("a" -> 1.5, "b" -> Double.PositiveInfinity))
        assertEquals(page.next, None)
      case other       => fail(s"expected a page, got $other")
    }
  }

  test("blocking pops carry Blocking execution and multi-key commands route on every key") {
    assertEquals(SortedSets.bzPopMin[String, String]("a", "b")(BlockTimeout.Forever).execution, Execution.Blocking)
    assertEquals(SortedSets.bzMpop[String, String]("a")(MinMax.Min, BlockTimeout.Forever).execution, Execution.Blocking)
    assertEquals(SortedSets.zMpop[String, String]("a", "b")(MinMax.Min).keyIndices, Vector(1, 2))
    assertEquals(SortedSets.zUnionStore("d", "a", "b")().keyIndices, Vector(0, 2, 3))
    assertEquals(SortedSets.zRangeStore[String, String]("d", "z", ZRange.ByRank(0L, -1L)).keyIndices, Vector(0, 1))
  }
}

package sage.commands

import sage.protocol.Frame
import sage.protocol.Frames.{bulk, wire}

class SortedSetsSpec extends munit.FunSuite with BothProtocols {

  private def pair(member: String, score: Double): Frame = Frame.Array(Vector(bulk(member), Frame.Double(score)))

  test("ZSCORE decodes a RESP3 double, null as None, and rejects a bulk string that is not a number") {
    assertEquals(Reply.decode(SortedSets.zScore[String, String]("z", "a"), Frame.Double(1.5)).toEither, Right(Some(1.5)))
    assertEquals(Reply.decode(SortedSets.zScore[String, String]("z", "a"), Frame.Null).toEither, Right(None))
    assert(Reply.decode(SortedSets.zScore[String, String]("z", "a"), bulk("one")).toEither.isLeft)
    assertEquals(expectedShape(SortedSets.zScore[String, String]("z", "a"), Frame.Integer(1)), Some("double or null"))
  }

  test("ZSCORE, ZMSCORE, ZINCRBY and ZADD INCR decode the RESP2 bulk-string score like the RESP3 double") {
    decodesAlike(SortedSets.zScore[String, String]("z", "b"), ",2.5\r\n", Some(2.5))
    decodesAlike(SortedSets.zScore[String, String]("z", "b"), "_\r\n", None)
    decodesAlike(SortedSets.zScore[String, String]("z", "b"), ",-inf\r\n", Some(Double.NegativeInfinity))
    decodesAlike(SortedSets.zMScore[String, String]("z", "a", "x", "b"), "*3\r\n,1\r\n_\r\n,2.5\r\n", Vector(Some(1.0), None, Some(2.5)))
    decodesAlike(SortedSets.zIncrBy("z", "a", 1.5), ",2.5\r\n", 2.5)
    decodesAlike(SortedSets.zAddIncr("z")("b", 2.0), ",4.5\r\n", Some(4.5))
    decodesAlike(SortedSets.zAddIncr("z", ZAddCondition.IfNotExists)("b", 2.0), "_\r\n", None)
  }

  test("WITHSCORES replies decode the flat RESP2 array like the nested RESP3 pairs") {
    val resp2    = "*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$3\r\n2.5\r\n$1\r\nc\r\n$1\r\n3\r\n"
    val resp3    = "*3\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2.5\r\n*2\r\n$1\r\nc\r\n,3\r\n"
    val expected = Vector("a" -> 1.0, "b" -> 2.5, "c" -> 3.0)
    val commands = Vector(
      SortedSets.zRangeWithScores[String, String]("z", ZRange.ByRank(0L, -1L)),
      SortedSets.zPopMinCount[String, String]("z", 3L),
      SortedSets.zPopMaxCount[String, String]("z", 3L),
      SortedSets.zUnionWithScores[String, String]("z", "z2")(),
      SortedSets.zInterWithScores[String, String]("z", "z2")(),
      SortedSets.zDiffWithScores[String, String]("z", "z2"),
      SortedSets.zRandMemberWithScores[String, String]("z", 3L)
    )
    commands.foreach { command =>
      decodesAlike(command, resp2, resp3, expected)
      decodesAlike(command, "*0\r\n", Vector.empty)
    }
  }

  test("a WITHSCORES count of one stays a pair list under RESP2, and an odd flat array is rejected") {
    decodesAlike(
      SortedSets.zPopMinCount[String, String]("z", 1L),
      "*2\r\n$1\r\na\r\n$1\r\n1\r\n",
      "*1\r\n*2\r\n$1\r\na\r\n,1\r\n",
      Vector("a" -> 1.0)
    )
    assert(Reply.decode(SortedSets.zPopMinCount[String, String]("z", 2L), wire("*3\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n")).toEither.isLeft)
    assertEquals(
      expectedShape(SortedSets.zPopMinCount[String, String]("z", 1L), wire("*1\r\n*3\r\n$1\r\na\r\n,1\r\n,2\r\n")),
      Some("member/score pair")
    )
  }

  test("ZPOPMIN without a count, ZRANK WITHSCORE, ZMPOP and BZPOPMIN decode their RESP2 replies like RESP3") {
    decodesAlike(SortedSets.zPopMin[String, String]("z"), "*2\r\n$1\r\na\r\n,1\r\n", Some("a" -> 1.0))
    decodesAlike(SortedSets.zPopMax[String, String]("z"), "*0\r\n", None)
    decodesAlike(SortedSets.zRankWithScore[String, String]("z", "b"), "*2\r\n:1\r\n,2.5\r\n", Some((1L, 2.5)))
    decodesAlike(SortedSets.zRankWithScore[String, String]("z", "x"), "_\r\n", None)
    decodesAlike(
      SortedSets.zMpop[String, String]("z")(MinMax.Min, Some(2L)),
      "*2\r\n$1\r\nz\r\n*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2.5\r\n",
      Some(("z", Vector("a" -> 1.0, "b" -> 2.5)))
    )
    decodesAlike(SortedSets.bzMpop[String, String]("z")(MinMax.Max, BlockTimeout.Forever), "_\r\n", None)
    decodesAlike(SortedSets.bzPopMin[String, String]("z")(BlockTimeout.Forever), "*3\r\n$1\r\nz\r\n$1\r\na\r\n,1\r\n", Some(("z", "a", 1.0)))
    decodesAlike(SortedSets.bzPopMax[String, String]("z")(BlockTimeout.Forever), "_\r\n", None)
  }

  test("ZADD INCR decodes the new score or None when the condition skipped the write") {
    assertEquals(Reply.decode(SortedSets.zAddIncr("z")("a", 1.0), Frame.Double(3.0)).toEither, Right(Some(3.0)))
    assertEquals(Reply.decode(SortedSets.zAddIncr("z", ZAddCondition.IfNotExists)("a", 1.0), Frame.Null).toEither, Right(None))
  }

  test("ZMSCORE decodes the score array, keeping missing members as None") {
    val reply = Frame.Array(Vector(Frame.Double(1.0), Frame.Null, Frame.Double(2.5)))
    assertEquals(Reply.decode(SortedSets.zMScore[String, String]("z", "a", "b", "c"), reply).toEither, Right(Vector(Some(1.0), None, Some(2.5))))
  }

  test("ZRANGE WITHSCORES decodes the nested member/score pairs") {
    val reply = Frame.Array(Vector(pair("a", 1.0), pair("b", 2.0)))
    assertEquals(
      Reply.decode(SortedSets.zRangeWithScores[String, String]("z", ZRange.ByRank(0L, -1L)), reply).toEither,
      Right(Vector("a" -> 1.0, "b" -> 2.0))
    )
  }

  test("ZPOPMIN decodes a flat member/score, an empty array as None, and a count as nested pairs") {
    assertEquals(
      Reply.decode(SortedSets.zPopMin[String, String]("z"), Frame.Array(Vector(bulk("a"), Frame.Double(1.0)))).toEither,
      Right(Some("a" -> 1.0))
    )
    assertEquals(Reply.decode(SortedSets.zPopMin[String, String]("z"), Frame.Array(Vector.empty)).toEither, Right(None))
    val counted = Frame.Array(Vector(pair("a", 1.0), pair("b", 2.0)))
    assertEquals(Reply.decode(SortedSets.zPopMinCount[String, String]("z", 2L), counted).toEither, Right(Vector("a" -> 1.0, "b" -> 2.0)))
  }

  test("ZRANK WITHSCORE decodes the rank/score pair or None") {
    val reply = Frame.Array(Vector(Frame.Integer(2), Frame.Double(5.0)))
    assertEquals(Reply.decode(SortedSets.zRankWithScore[String, String]("z", "a"), reply).toEither, Right(Some((2L, 5.0))))
    assertEquals(Reply.decode(SortedSets.zRankWithScore[String, String]("z", "a"), Frame.Null).toEither, Right(None))
  }

  test("ZMPOP decodes null as None and a key with its scored members") {
    assertEquals(Reply.decode(SortedSets.zMpop[String, String]("a", "b")(MinMax.Min), Frame.Null).toEither, Right(None))
    val reply = Frame.Array(Vector(bulk("a"), Frame.Array(Vector(pair("x", 1.0)))))
    assertEquals(Reply.decode(SortedSets.zMpop[String, String]("a", "b")(MinMax.Min), reply).toEither, Right(Some(("a", Vector("x" -> 1.0)))))
  }

  test("BZPOPMIN decodes null as None and the key, member, score triple") {
    assertEquals(Reply.decode(SortedSets.bzPopMin[String, String]("a")(BlockTimeout.Forever), Frame.Null).toEither, Right(None))
    val reply = Frame.Array(Vector(bulk("a"), bulk("m"), Frame.Double(1.5)))
    assertEquals(Reply.decode(SortedSets.bzPopMin[String, String]("a")(BlockTimeout.Forever), reply).toEither, Right(Some(("a", "m", 1.5))))
  }

  test("ZSCAN decodes the flat member/score array with string scores, infinity included") {
    val reply = Frame.Array(Vector(bulk("0"), Frame.Array(Vector(bulk("a"), bulk("1.5"), bulk("b"), bulk("inf")))))
    Reply.decode(SortedSets.zScan[String, String]("z", ScanCursor.start), reply).toEither match {
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

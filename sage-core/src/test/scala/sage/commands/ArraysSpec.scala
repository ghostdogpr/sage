package sage.commands

import sage.protocol.Frame
import sage.protocol.Frames.{bulk, map}

class ArraysSpec extends munit.FunSuite {

  test("ARGET decodes a value and a null") {
    assertEquals(Reply.decode(Arrays.arGet[String, String]("a", 1L), bulk("b")).toEither, Right(Some("b")))
    assertEquals(Reply.decode(Arrays.arGet[String, String]("a", 1L), Frame.Null).toEither, Right(None))
  }

  test("ARMGET and ARGETRANGE keep nils for empty slots") {
    val reply = Frame.Array(Vector(bulk("x"), Frame.Null, bulk("y")))
    assertEquals(Reply.decode(Arrays.arMGet[String, String]("a", 0L, 1L, 2L), reply).toEither, Right(Vector(Some("x"), None, Some("y"))))
    assertEquals(Reply.decode(Arrays.arGetRange[String, String]("a", 0L, 2L), reply).toEither, Right(Vector(Some("x"), None, Some("y"))))
  }

  test("ARLASTITEMS decodes the items in order") {
    val reply = Frame.Array(Vector(bulk("d"), bulk("e")))
    assertEquals(Reply.decode(Arrays.arLastItems[String, String]("a", 2L), reply).toEither, Right(Vector("d", "e")))
  }

  test("ARNEXT decodes the next index and null when exhausted") {
    assertEquals(Reply.decode(Arrays.arNext("a"), Frame.Integer(2L)).toEither, Right(Some(2L)))
    assertEquals(Reply.decode(Arrays.arNext("a"), Frame.Null).toEither, Right(None))
  }

  test("ARSEEK decodes the cursor-set flag") {
    assertEquals(Reply.decode(Arrays.arSeek("a", 5L), Frame.Integer(1L)).toEither, Right(true))
    assertEquals(Reply.decode(Arrays.arSeek("a", 5L), Frame.Integer(0L)).toEither, Right(false))
  }

  test("ARSCAN and ARGREP WITHVALUES decode an array of [index, value] pairs") {
    val reply = Frame.Array(
      Vector(Frame.Array(Vector(Frame.Integer(0L), bulk("a"))), Frame.Array(Vector(Frame.Integer(5L), bulk("f"))))
    )
    assertEquals(Reply.decode(Arrays.arScan[String, String]("a", 0L, 10L), reply).toEither, Right(Vector(0L -> "a", 5L -> "f")))
    assertEquals(
      Reply.decode(Arrays.arGrepWithValues[String, String]("a", 0L, 10L)(ArMatch.Glob("*")), reply).toEither,
      Right(Vector(0L -> "a", 5L -> "f"))
    )
  }

  test("ARGREP decodes matching indices") {
    val reply = Frame.Array(Vector(Frame.Integer(0L), Frame.Integer(2L)))
    assertEquals(Reply.decode(Arrays.arGrep("a", 0L, 10L)(ArMatch.Glob("ap*")), reply).toEither, Right(Vector(0L, 2L)))
  }

  test("AROP SUM/MIN/MAX decode a numeric bulk string or null") {
    assertEquals(Reply.decode(Arrays.arOpSum("a", 0L, 2L), bulk("60")).toEither, Right(Some(60.0)))
    assertEquals(Reply.decode(Arrays.arOpMin("a", 0L, 2L), bulk("10")).toEither, Right(Some(10.0)))
    assertEquals(Reply.decode(Arrays.arOpMax("a", 0L, 2L), Frame.Null).toEither, Right(None))
  }

  test("AROP AND/OR/XOR decode an integer or null, MATCH/USED an integer") {
    assertEquals(Reply.decode(Arrays.arOpAnd("a", 0L, 2L), Frame.Integer(0L)).toEither, Right(Some(0L)))
    assertEquals(Reply.decode(Arrays.arOpXor("a", 0L, 2L), Frame.Null).toEither, Right(None))
    assertEquals(Reply.decode(Arrays.arOpUsed("a", 0L, 2L), Frame.Integer(3L)).toEither, Right(3L))
    assertEquals(Reply.decode(Arrays.arOpMatch("a", 0L, 2L, "v"), Frame.Integer(1L)).toEither, Right(1L))
  }

  test("ARINFO decodes the core fields and leniently fills the structural ones") {
    val reply = map(
      "count"             -> Frame.Integer(4L),
      "len"               -> Frame.Integer(101L),
      "next-insert-index" -> Frame.Integer(0L),
      "slices"            -> Frame.Integer(1L),
      "slice-size"        -> Frame.Integer(4096L)
    )
    assertEquals(
      Reply.decode(Arrays.arInfo("a"), reply).toEither,
      Right(ArrayInfo(4L, 101L, 0L, slices = Some(1L), directorySize = None, superDirEntries = None, sliceSize = Some(4096L)))
    )
  }

  test("ARINFO FULL decodes the avg-* fields as doubles") {
    val reply = map(
      "count"             -> Frame.Integer(4L),
      "len"               -> Frame.Integer(101L),
      "next-insert-index" -> Frame.Integer(0L),
      "sparse-slices"     -> Frame.Integer(1L),
      "avg-sparse-size"   -> Frame.Double(4.0)
    )
    Reply.decode(Arrays.arInfoFull("a"), reply).toEither match {
      case Right(info) =>
        assertEquals(info.count, 4L)
        assertEquals(info.sparseSlices, Some(1L))
        assertEquals(info.avgSparseSize, Some(4.0))
        assertEquals(info.avgDenseSize, None)
      case other       => fail(s"expected ArrayInfoFull, got $other")
    }
  }

  test("ARINFO fails when a core field is absent") {
    assert(Reply.decode(Arrays.arInfo("a"), map("len" -> Frame.Integer(1L))).toEither.isLeft)
  }
}

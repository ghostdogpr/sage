package sage.commands

import sage.Bytes
import sage.protocol.Frame

class ArraysSpec extends munit.FunSuite {

  private def bulk(s: String): Frame = Frame.BulkString(Bytes.utf8(s))

  test("ARGET decodes a value and a null") {
    assertEquals(Reply.run(Arrays.arGet[String, String]("a", 1L), bulk("b")), Right(Some("b")))
    assertEquals(Reply.run(Arrays.arGet[String, String]("a", 1L), Frame.Null), Right(None))
  }

  test("ARMGET and ARGETRANGE keep nils for empty slots") {
    val reply = Frame.Array(Vector(bulk("x"), Frame.Null, bulk("y")))
    assertEquals(Reply.run(Arrays.arMGet[String, String]("a", 0L, 1L, 2L), reply), Right(Vector(Some("x"), None, Some("y"))))
    assertEquals(Reply.run(Arrays.arGetRange[String, String]("a", 0L, 2L), reply), Right(Vector(Some("x"), None, Some("y"))))
  }

  test("ARLASTITEMS decodes the items in order") {
    val reply = Frame.Array(Vector(bulk("d"), bulk("e")))
    assertEquals(Reply.run(Arrays.arLastItems[String, String]("a", 2L), reply), Right(Vector("d", "e")))
  }

  test("ARNEXT decodes the next index and null when exhausted") {
    assertEquals(Reply.run(Arrays.arNext("a"), Frame.Integer(2L)), Right(Some(2L)))
    assertEquals(Reply.run(Arrays.arNext("a"), Frame.Null), Right(None))
  }

  test("ARSEEK decodes the cursor-set flag") {
    assertEquals(Reply.run(Arrays.arSeek("a", 5L), Frame.Integer(1L)), Right(true))
    assertEquals(Reply.run(Arrays.arSeek("a", 5L), Frame.Integer(0L)), Right(false))
  }

  test("ARSCAN and ARGREP WITHVALUES decode an array of [index, value] pairs") {
    val reply = Frame.Array(
      Vector(Frame.Array(Vector(Frame.Integer(0L), bulk("a"))), Frame.Array(Vector(Frame.Integer(5L), bulk("f"))))
    )
    assertEquals(Reply.run(Arrays.arScan[String, String]("a", 0L, 10L), reply), Right(Vector(0L -> "a", 5L -> "f")))
    assertEquals(Reply.run(Arrays.arGrepWithValues[String, String]("a", 0L, 10L)(ArMatch.Glob("*")), reply), Right(Vector(0L -> "a", 5L -> "f")))
  }

  test("ARGREP decodes matching indices") {
    val reply = Frame.Array(Vector(Frame.Integer(0L), Frame.Integer(2L)))
    assertEquals(Reply.run(Arrays.arGrep("a", 0L, 10L)(ArMatch.Glob("ap*")), reply), Right(Vector(0L, 2L)))
  }

  test("AROP SUM/MIN/MAX decode a numeric bulk string or null") {
    assertEquals(Reply.run(Arrays.arOpSum("a", 0L, 2L), bulk("60")), Right(Some(60.0)))
    assertEquals(Reply.run(Arrays.arOpMin("a", 0L, 2L), bulk("10")), Right(Some(10.0)))
    assertEquals(Reply.run(Arrays.arOpMax("a", 0L, 2L), Frame.Null), Right(None))
  }

  test("AROP AND/OR/XOR decode an integer or null, MATCH/USED an integer") {
    assertEquals(Reply.run(Arrays.arOpAnd("a", 0L, 2L), Frame.Integer(0L)), Right(Some(0L)))
    assertEquals(Reply.run(Arrays.arOpXor("a", 0L, 2L), Frame.Null), Right(None))
    assertEquals(Reply.run(Arrays.arOpUsed("a", 0L, 2L), Frame.Integer(3L)), Right(3L))
    assertEquals(Reply.run(Arrays.arOpMatch("a", 0L, 2L, "v"), Frame.Integer(1L)), Right(1L))
  }

  test("ARINFO decodes the core fields and leniently fills the structural ones") {
    val reply = Frame.Map(
      Vector(
        bulk("count")             -> Frame.Integer(4L),
        bulk("len")               -> Frame.Integer(101L),
        bulk("next-insert-index") -> Frame.Integer(0L),
        bulk("slices")            -> Frame.Integer(1L),
        bulk("slice-size")        -> Frame.Integer(4096L)
      )
    )
    assertEquals(
      Reply.run(Arrays.arInfo("a"), reply),
      Right(ArrayInfo(4L, 101L, 0L, slices = Some(1L), directorySize = None, superDirEntries = None, sliceSize = Some(4096L)))
    )
  }

  test("ARINFO FULL decodes the avg-* fields as doubles") {
    val reply = Frame.Map(
      Vector(
        bulk("count")             -> Frame.Integer(4L),
        bulk("len")               -> Frame.Integer(101L),
        bulk("next-insert-index") -> Frame.Integer(0L),
        bulk("sparse-slices")     -> Frame.Integer(1L),
        bulk("avg-sparse-size")   -> Frame.Double(4.0)
      )
    )
    Reply.run(Arrays.arInfoFull("a"), reply) match {
      case Right(info) =>
        assertEquals(info.count, 4L)
        assertEquals(info.sparseSlices, Some(1L))
        assertEquals(info.avgSparseSize, Some(4.0))
        assertEquals(info.avgDenseSize, None)
      case other       => fail(s"expected ArrayInfoFull, got $other")
    }
  }

  test("ARINFO fails when a core field is absent") {
    assert(Reply.run(Arrays.arInfo("a"), Frame.Map(Vector(bulk("len") -> Frame.Integer(1L)))).isLeft)
  }
}

package sage.commands

import sage.Bytes
import sage.protocol.Frame

class HashesSpec extends munit.FunSuite {

  private def bulk(value: String): Frame = Frame.BulkString(Bytes.utf8(value))

  test("HGETALL decodes a RESP3 map frame into a field-keyed map") {
    val reply = Frame.Map(Vector((bulk("f1"), bulk("v1")), (bulk("f2"), bulk("v2"))))
    assertEquals(Reply.run(Hashes.hGetAll[String, String, String]("h"), reply), Right(Map("f1" -> "v1", "f2" -> "v2")))
    assertEquals(Reply.run(Hashes.hGetAll[String, String, String]("h"), Frame.Map(Vector.empty)), Right(Map.empty[String, String]))
  }

  test("HMGET keeps missing fields as None positionally") {
    val reply = Frame.Array(Vector(bulk("v1"), Frame.Null, bulk("v3")))
    assertEquals(Reply.run(Hashes.hmGet[String, String, String]("h", "a", "b", "c"), reply), Right(Vector(Some("v1"), None, Some("v3"))))
  }

  test("HRANDFIELD decodes null as None and a single field as Some") {
    assertEquals(Reply.run(Hashes.hRandField[String, String]("h"), Frame.Null), Right(None))
    assertEquals(Reply.run(Hashes.hRandField[String, String]("h"), bulk("f")), Right(Some("f")))
  }

  test("HRANDFIELD WITHVALUES decodes the nested field/value pairs") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(bulk("f1"), bulk("v1"))), Frame.Array(Vector(bulk("f2"), bulk("v2")))))
    assertEquals(
      Reply.run(Hashes.hRandFieldWithValues[String, String, String]("h", 2L), reply),
      Right(Vector("f1" -> "v1", "f2" -> "v2"))
    )
  }

  test("HSCAN decodes the cursor and the flat field/value array into pairs") {
    val reply = Frame.Array(Vector(bulk("12"), Frame.Array(Vector(bulk("f1"), bulk("v1"), bulk("f2"), bulk("v2")))))
    Reply.run(Hashes.hScan[String, String, String]("h", ScanCursor.start), reply) match {
      case Right(page) =>
        assertEquals(page.items, Vector("f1" -> "v1", "f2" -> "v2"))
        assert(page.next.isDefined)
      case other       => fail(s"expected a page, got $other")
    }
  }

  test("HSCAN rejects an odd-length field/value array") {
    val reply = Frame.Array(Vector(bulk("0"), Frame.Array(Vector(bulk("f1"), bulk("v1"), bulk("f2")))))
    assert(Reply.run(Hashes.hScan[String, String, String]("h", ScanCursor.start), reply).isLeft)
  }

  test("HSCAN NOVALUES decodes a zero cursor as complete and the items as bare fields") {
    val reply = Frame.Array(Vector(bulk("0"), Frame.Array(Vector(bulk("f1"), bulk("f2")))))
    Reply.run(Hashes.hScanNoValues[String, String]("h", ScanCursor.start), reply) match {
      case Right(page) =>
        assertEquals(page.items, Vector("f1", "f2"))
        assertEquals(page.next, None)
      case other       => fail(s"expected a page, got $other")
    }
  }

  test("HINCRBYFLOAT decodes the bulk-string double") {
    assertEquals(Reply.run(Hashes.hIncrByFloat("h", "f", 1.5), bulk("10.75")), Right(10.75))
    assert(Reply.run(Hashes.hIncrByFloat("h", "f", 1.5), bulk("not-a-number")).isLeft)
  }

  test("every hash command routes on the hash key alone") {
    assertEquals(Hashes.hSet("h", ("f", "v")).keyIndices, Vector(0))
    assertEquals(Hashes.hmGet[String, String, String]("h", "a", "b").keyIndices, Vector(0))
    assertEquals(Hashes.hDel("h", "a", "b").keyIndices, Vector(0))
    assertEquals(Hashes.hScan[String, String, String]("h", ScanCursor.start).keyIndices, Vector(0))
  }
}

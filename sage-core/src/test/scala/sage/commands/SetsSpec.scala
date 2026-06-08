package sage.commands

import sage.Bytes
import sage.protocol.Frame

class SetsSpec extends munit.FunSuite {

  private def bulk(value: String): Frame = Frame.BulkString(Bytes.utf8(value))

  test("SMEMBERS decodes a RESP3 set frame into a Set, empty included") {
    assertEquals(Reply.run(Sets.sMembers[String, String]("s"), Frame.Set(Vector(bulk("a"), bulk("b")))), Right(Set("a", "b")))
    assertEquals(Reply.run(Sets.sMembers[String, String]("s"), Frame.Set(Vector.empty)), Right(Set.empty[String]))
  }

  test("a set reply rejects a plain array frame") {
    assert(Reply.run(Sets.sMembers[String, String]("s"), Frame.Array(Vector(bulk("a")))).isLeft)
  }

  test("SPOP decodes null as None and a bulk string as the member; a count reads a set") {
    assertEquals(Reply.run(Sets.sPop[String, String]("s"), Frame.Null), Right(None))
    assertEquals(Reply.run(Sets.sPop[String, String]("s"), bulk("a")), Right(Some("a")))
    assertEquals(Reply.run(Sets.sPopCount[String, String]("s", 2L), Frame.Set(Vector(bulk("a"), bulk("b")))), Right(Set("a", "b")))
  }

  test("SMISMEMBER decodes the 0/1 array positionally") {
    val reply = Frame.Array(Vector(Frame.Integer(1), Frame.Integer(0), Frame.Integer(1)))
    assertEquals(Reply.run(Sets.sMisMember("s", "a", "b", "c"), reply), Right(Vector(true, false, true)))
  }

  test("SRANDMEMBER with a count keeps duplicates as an ordered vector") {
    val reply = Frame.Array(Vector(bulk("a"), bulk("a"), bulk("b")))
    assertEquals(Reply.run(Sets.sRandMemberCount[String, String]("s", -3L), reply), Right(Vector("a", "a", "b")))
  }

  test("set commands route on their keys") {
    assertEquals(Sets.sAdd("s", "a").keyIndices, Vector(0))
    assertEquals(Sets.sMove("s", "d", "m").keyIndices, Vector(0, 1))
    assertEquals(Sets.sInterCard("a", "b")().keyIndices, Vector(1, 2))
    assertEquals(Sets.sUnionStore("d", "a", "b").keyIndices, Vector(0, 1, 2))
  }
}

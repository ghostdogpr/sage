package sage.commands

import sage.protocol.Frame
import sage.protocol.Frames.bulk

class SetsSpec extends munit.FunSuite with BothProtocols {

  test("SMEMBERS decodes a RESP3 set frame into a Set, empty included") {
    assertEquals(Reply.decode(Sets.sMembers[String, String]("s"), Frame.Set(Vector(bulk("a"), bulk("b")))).toEither, Right(Set("a", "b")))
    assertEquals(Reply.decode(Sets.sMembers[String, String]("s"), Frame.Set(Vector.empty)).toEither, Right(Set.empty[String]))
  }

  test("set replies decode the RESP2 array like the RESP3 set frame") {
    val resp2    = "*3\r\n$1\r\na\r\n$1\r\nc\r\n$1\r\nb\r\n"
    val resp3    = "~3\r\n$1\r\na\r\n$1\r\nc\r\n$1\r\nb\r\n"
    val commands = Vector(
      Sets.sMembers[String, String]("s"),
      Sets.sPopCount[String, String]("s", 3L),
      Sets.sDiff[String, String]("s"),
      Sets.sInter[String, String]("s", "t"),
      Sets.sUnion[String, String]("s", "t")
    )
    commands.foreach { command =>
      decodesAlike(command, resp2, resp3, Set("a", "b", "c"))
      decodesAlike(command, "*0\r\n", "~0\r\n", Set.empty[String])
    }
  }

  test("a set reply rejects a frame that is neither a set nor an array") {
    assert(Reply.decode(Sets.sMembers[String, String]("s"), bulk("a")).toEither.isLeft)
  }

  test("SPOP decodes null as None and a bulk string as the member; a count reads a set") {
    assertEquals(Reply.decode(Sets.sPop[String, String]("s"), Frame.Null).toEither, Right(None))
    assertEquals(Reply.decode(Sets.sPop[String, String]("s"), bulk("a")).toEither, Right(Some("a")))
    assertEquals(Reply.decode(Sets.sPopCount[String, String]("s", 2L), Frame.Set(Vector(bulk("a"), bulk("b")))).toEither, Right(Set("a", "b")))
  }

  test("SMISMEMBER decodes the 0/1 array positionally") {
    val reply = Frame.Array(Vector(Frame.Integer(1), Frame.Integer(0), Frame.Integer(1)))
    assertEquals(Reply.decode(Sets.sMisMember("s", "a", "b", "c"), reply).toEither, Right(Vector(true, false, true)))
  }

  test("SRANDMEMBER with a count keeps duplicates as an ordered vector") {
    val reply = Frame.Array(Vector(bulk("a"), bulk("a"), bulk("b")))
    assertEquals(Reply.decode(Sets.sRandMemberCount[String, String]("s", -3L), reply).toEither, Right(Vector("a", "a", "b")))
  }

  test("set commands route on their keys") {
    assertEquals(Sets.sAdd("s", "a").keyIndices, Vector(0))
    assertEquals(Sets.sMove("s", "d", "m").keyIndices, Vector(0, 1))
    assertEquals(Sets.sInterCard("a", "b")().keyIndices, Vector(1, 2))
    assertEquals(Sets.sUnionStore("d", "a", "b").keyIndices, Vector(0, 1, 2))
  }
}

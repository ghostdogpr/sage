package sage.commands

import sage.protocol.Frame
import sage.protocol.Frames.bulk

class ListsSpec extends munit.FunSuite {

  test("LPOP with a count collapses a missing list's null to an empty vector") {
    assertEquals(Reply.decode(Lists.lPopCount[String, String]("l", 2L), Frame.Null).toEither, Right(Vector.empty[String]))
    assertEquals(
      Reply.decode(Lists.lPopCount[String, String]("l", 2L), Frame.Array(Vector(bulk("a"), bulk("b")))).toEither,
      Right(Vector("a", "b"))
    )
  }

  test("LPOP without a count decodes null as None") {
    assertEquals(Reply.decode(Lists.lPop[String, String]("l"), Frame.Null).toEither, Right(None))
    assertEquals(Reply.decode(Lists.lPop[String, String]("l"), bulk("a")).toEither, Right(Some("a")))
  }

  test("LPOS decodes null as None, an integer as the index, and a count reply as a vector") {
    assertEquals(Reply.decode(Lists.lPos("l", "v"), Frame.Null).toEither, Right(None))
    assertEquals(Reply.decode(Lists.lPos("l", "v"), Frame.Integer(3)).toEither, Right(Some(3L)))
    assertEquals(Reply.decode(Lists.lPosCount("l", "v", 0L), Frame.Array(Vector(Frame.Integer(1), Frame.Integer(3)))).toEither, Right(Vector(1L, 3L)))
  }

  test("LMOVE decodes null as None when the source is empty") {
    assertEquals(Reply.decode(Lists.lMove[String, String]("s", "d", ListSide.Left, ListSide.Right), Frame.Null).toEither, Right(None))
    assertEquals(Reply.decode(Lists.lMove[String, String]("s", "d", ListSide.Left, ListSide.Right), bulk("a")).toEither, Right(Some("a")))
  }

  test("LMPOP decodes null as None and a key/values reply as the popped key and elements") {
    assertEquals(Reply.decode(Lists.lMpop[String, String]("a", "b")(ListSide.Left), Frame.Null).toEither, Right(None))
    val reply = Frame.Array(Vector(bulk("b"), Frame.Array(Vector(bulk("x"), bulk("y")))))
    assertEquals(Reply.decode(Lists.lMpop[String, String]("a", "b")(ListSide.Left), reply).toEither, Right(Some(("b", Vector("x", "y")))))
  }

  test("the list sides and insert positions encode to their wire words") {
    assertEquals(Lists.lMove[String, String]("s", "d", ListSide.Left, ListSide.Right).args.takeRight(2).map(_.asUtf8String), Vector("LEFT", "RIGHT"))
    assertEquals(Lists.lInsert("l", InsertPosition.Before, "p", "v").args(1).asUtf8String, "BEFORE")
    assertEquals(Lists.lInsert("l", InsertPosition.After, "p", "v").args(1).asUtf8String, "AFTER")
  }

  test("multi-key list commands mark the right positions for the slot engine") {
    assertEquals(Lists.lMove[String, String]("s", "d", ListSide.Left, ListSide.Right).keyIndices, Vector(0, 1))
    assertEquals(Lists.lMpop[String, String]("a", "b", "c")(ListSide.Left).keyIndices, Vector(1, 2, 3))
    assertEquals(Lists.lPush("l", "a").keyIndices, Vector(0))
  }
}

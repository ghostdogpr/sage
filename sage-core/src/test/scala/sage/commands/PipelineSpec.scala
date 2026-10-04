package sage.commands

import sage.SageException.{DecodeError, ServerError}

class PipelineSpec extends munit.FunSuite {

  test("tuple syntax preserves command order and arity") {
    val p = Pipeline.fromTuple((Connection.ping(), Strings.get[String, String]("k"), Strings.incr[String]("n")))
    assertEquals(p.commands.map(_.name), Vector("PING", "GET", "INCR"))
  }

  test("tuple pipeline assembles the all-success tuple") {
    val p = Pipeline.fromTuple((Connection.ping(), Strings.get[String, String]("k")))
    assertEquals(p.finish(Vector(Right("PONG"), Right(Some("v")))), Right(("PONG", Some("v"))))
  }

  test("tuple pipeline shapes per-position results, mixing success and failure") {
    val p       = Pipeline.fromTupleAttempt((Connection.ping(), Strings.incr[String]("n")))
    val results = p.finish(Vector(Right("PONG"), Left(ServerError("WRONGTYPE", ""))))
    assertEquals(results, Right((Right("PONG"), Left(ServerError("WRONGTYPE", "")))))
  }

  test("sequence preserves order and assembles a homogeneous vector") {
    val p = Pipeline.sequence(Vector("a", "b", "c").map(Strings.get[String, String]))
    assertEquals(p.commands.map(_.name), Vector("GET", "GET", "GET"))
    assertEquals(p.finish(Vector(Right(Some("1")), Right(None), Right(Some("3")))), Right(Vector(Some("1"), None, Some("3"))))
  }

  test("sequence shapes per-position results") {
    val p       = Pipeline.sequenceAttempt(Vector(Strings.get[String, String]("k"), Strings.get[String, String]("j")))
    val results = p.finish(Vector(Right(Some("v")), Left(DecodeError("bulk string", "integer"))))
    assertEquals(results, Right(Vector(Right(Some("v")), Left(DecodeError("bulk string", "integer")))))
  }

  test("a strict pipeline fails with the first error in position order") {
    val p = Pipeline.sequence(Vector("a", "b", "c").map(Strings.get[String, String]))
    assertEquals(p.finish(Vector(Right(Some("1")), Left(ServerError("ERR", "x")), Left(ServerError("ERR", "y")))), Left(ServerError("ERR", "x")))
  }

  test("an empty sequence carries no commands") {
    val p = Pipeline.sequence(Vector.empty[Command[Long]])
    assertEquals(p.commands, Vector.empty)
    assertEquals(p.finish(Vector.empty), Right(Vector.empty))
  }
}

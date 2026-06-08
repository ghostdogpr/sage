package sage.commands

import sage.SageException.{DecodeError, ServerError}
import sage.commands.Pipeline.pipeline

class PipelineSpec extends munit.FunSuite {

  test("tuple syntax preserves command order and arity") {
    val p = (Connection.ping(), Strings.get[String, String]("k"), Strings.incr[String]("n")).pipeline
    assertEquals(p.commands.map(_.name), Vector("PING", "GET", "INCR"))
  }

  test("tuple pipeline assembles the all-success tuple") {
    val p   = (Connection.ping(), Strings.get[String, String]("k")).pipeline
    val out = p.toOut(Vector("PONG", Some("v")))
    assertEquals(out, ("PONG", Some("v")))
  }

  test("tuple pipeline shapes per-position results, mixing success and failure") {
    val p       = (Connection.ping(), Strings.incr[String]("n")).pipeline
    val results = p.toResults(Vector(Right("PONG"), Left(ServerError("WRONGTYPE"))))
    assertEquals(results, (Right("PONG"), Left(ServerError("WRONGTYPE"))))
  }

  test("sequence preserves order and assembles a homogeneous vector") {
    val p = Pipeline.sequence(Vector("a", "b", "c").map(Strings.get[String, String]))
    assertEquals(p.commands.map(_.name), Vector("GET", "GET", "GET"))
    assertEquals(p.toOut(Vector(Some("1"), None, Some("3"))), Vector(Some("1"), None, Some("3")))
  }

  test("sequence shapes per-position results") {
    val p       = Pipeline.sequence(Vector(Strings.get[String, String]("k"), Strings.get[String, String]("j")))
    val results = p.toResults(Vector(Right(Some("v")), Left(DecodeError("bulk string", "integer"))))
    assertEquals(results, Vector(Right(Some("v")), Left(DecodeError("bulk string", "integer"))))
  }

  test("an empty sequence carries no commands") {
    val p = Pipeline.sequence(Vector.empty[Command[Long]])
    assertEquals(p.commands, Vector.empty)
    assertEquals(p.toOut(Vector.empty), Vector.empty)
  }
}

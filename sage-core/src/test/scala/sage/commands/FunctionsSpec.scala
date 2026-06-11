package sage.commands

import scala.concurrent.duration.*

import sage.Bytes
import sage.protocol.Frame

class FunctionsSpec extends munit.FunSuite {

  private def bulk(value: String): Frame            = Frame.BulkString(Bytes.utf8(value))
  private def map(entries: (String, Frame)*): Frame = Frame.Map(entries.toVector.map { case (k, v) => bulk(k) -> v })

  test("FCALL returns the raw frame and computes key indices") {
    assertEquals(Reply.run(Functions.fCall("f", Seq("k"), Seq("a")), Frame.Integer(7L)), Right(Frame.Integer(7L)))
    assertEquals(Functions.fCall("f", Seq("k1", "k2")).keyIndices, Vector(2, 3))
  }

  test("FUNCTION LOAD/DELETE/FLUSH/RESTORE are All-Masters Commands; KILL/DUMP/LIST/STATS are not") {
    assert(Functions.functionLoad("code").allMasters)
    assert(Functions.functionDelete("lib").allMasters)
    assert(Functions.functionFlush().allMasters)
    assert(Functions.functionRestore(Bytes.utf8("p")).allMasters)
    assert(!Functions.functionKill.allMasters)
    assert(!Functions.functionDump.allMasters)
    assert(!Functions.functionList().allMasters)
    assert(!Functions.functionStats.allMasters)
  }

  test("FUNCTION LIST decodes libraries with their functions and flags, tolerating extra keys") {
    val reply = Frame.Array(
      Vector(
        map(
          "library_name" -> bulk("mylib"),
          "engine"       -> bulk("LUA"),
          "unknown_key"  -> bulk("ignored"),
          "functions"    -> Frame.Array(
            Vector(
              map(
                "name"        -> bulk("myfunc"),
                "description" -> Frame.Null,
                "flags"       -> Frame.Set(Vector(bulk("no-writes")))
              )
            )
          )
        )
      )
    )
    assertEquals(
      Reply.run(Functions.functionList(), reply),
      Right(Vector(LibraryInfo("mylib", "LUA", Vector(FunctionInfo("myfunc", None, Set("no-writes"))), None)))
    )
  }

  test("FUNCTION LIST WITHCODE surfaces the library code") {
    val reply = Frame.Array(
      Vector(map("library_name" -> bulk("l"), "engine" -> bulk("LUA"), "functions" -> Frame.Array(Vector.empty), "library_code" -> bulk("#!lua")))
    )
    assertEquals(Reply.run(Functions.functionList(withCode = true), reply), Right(Vector(LibraryInfo("l", "LUA", Vector.empty, Some("#!lua")))))
  }

  test("FUNCTION STATS decodes a running script and per-engine counts; null running_script is None") {
    val reply = map(
      "running_script" -> map("name" -> bulk("f"), "command" -> Frame.Array(Vector(bulk("FCALL"), bulk("f"))), "duration_ms" -> Frame.Integer(12L)),
      "engines"        -> map("LUA" -> map("libraries_count" -> Frame.Integer(2L), "functions_count" -> Frame.Integer(5L)))
    )
    assertEquals(
      Reply.run(Functions.functionStats, reply),
      Right(FunctionStats(Some(RunningScript("f", Vector("FCALL", "f"), 12.millis)), Map("LUA" -> EngineStats(2L, 5L))))
    )
    val idle  = map("running_script" -> Frame.Null, "engines" -> Frame.Map(Vector.empty))
    assertEquals(Reply.run(Functions.functionStats, idle), Right(FunctionStats(None, Map.empty)))
  }

  test("FUNCTION DUMP decodes the opaque payload as bytes") {
    val payload = Bytes.utf8("\u0000binary")
    assertEquals(Reply.run(Functions.functionDump, Frame.BulkString(payload)).map(_.asUtf8String), Right(payload.asUtf8String))
  }
}

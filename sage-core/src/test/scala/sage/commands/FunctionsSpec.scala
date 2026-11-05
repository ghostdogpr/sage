package sage.commands

import scala.concurrent.duration.*

import sage.Bytes
import sage.protocol.Frame
import sage.protocol.Frames.{bulk, map}

class FunctionsSpec extends munit.FunSuite with BothProtocols {

  test("FCALL returns the raw frame and computes key indices") {
    assertEquals(Reply.decode(Functions.fCall("f", Seq("k"), Seq("a")), Frame.Integer(7L)).toEither, Right(Frame.Integer(7L)))
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
      Reply.decode(Functions.functionList(), reply).toEither,
      Right(Vector(LibraryInfo("mylib", "LUA", Vector(FunctionInfo("myfunc", None, Set("no-writes"))), None)))
    )
  }

  test("FUNCTION LIST WITHCODE surfaces the library code") {
    val reply = Frame.Array(
      Vector(map("library_name" -> bulk("l"), "engine" -> bulk("LUA"), "functions" -> Frame.Array(Vector.empty), "library_code" -> bulk("#!lua")))
    )
    assertEquals(
      Reply.decode(Functions.functionList(withCode = true), reply).toEither,
      Right(Vector(LibraryInfo("l", "LUA", Vector.empty, Some("#!lua"))))
    )
  }

  test("FUNCTION STATS decodes a running script and per-engine counts; null running_script is None") {
    val reply = map(
      "running_script" -> map("name" -> bulk("f"), "command" -> Frame.Array(Vector(bulk("FCALL"), bulk("f"))), "duration_ms" -> Frame.Integer(12L)),
      "engines"        -> map("LUA" -> map("libraries_count" -> Frame.Integer(2L), "functions_count" -> Frame.Integer(5L)))
    )
    assertEquals(
      Reply.decode(Functions.functionStats, reply).toEither,
      Right(FunctionStats(Some(RunningScript("f", Vector("FCALL", "f"), 12.millis)), Map("LUA" -> EngineStats(2L, 5L))))
    )
    val idle  = map("running_script" -> Frame.Null, "engines" -> Frame.Map(Vector.empty))
    assertEquals(Reply.decode(Functions.functionStats, idle).toEither, Right(FunctionStats(None, Map.empty)))
  }

  test("FUNCTION LIST and FUNCTION STATS decode the RESP2 flat arrays like the RESP3 maps") {
    val code = "#!lua name=mylib"
    decodesAlike(
      Functions.functionList(withCode = true),
      "*1\r\n*8\r\n$12\r\nlibrary_name\r\n$5\r\nmylib\r\n$6\r\nengine\r\n$3\r\nLUA\r\n$9\r\nfunctions\r\n*2\r\n*6\r\n$4\r\nname\r\n$5\r\nother\r\n" +
        "$11\r\ndescription\r\n$-1\r\n$5\r\nflags\r\n*0\r\n*6\r\n$4\r\nname\r\n$6\r\nmyfunc\r\n$11\r\ndescription\r\n$4\r\ndesc\r\n$5\r\nflags\r\n" +
        "*1\r\n+no-writes\r\n$12\r\nlibrary_code\r\n$16\r\n" + code + "\r\n",
      "*1\r\n%4\r\n$12\r\nlibrary_name\r\n$5\r\nmylib\r\n$6\r\nengine\r\n$3\r\nLUA\r\n$9\r\nfunctions\r\n*2\r\n%3\r\n$4\r\nname\r\n$5\r\nother\r\n" +
        "$11\r\ndescription\r\n_\r\n$5\r\nflags\r\n~0\r\n%3\r\n$4\r\nname\r\n$6\r\nmyfunc\r\n$11\r\ndescription\r\n$4\r\ndesc\r\n$5\r\nflags\r\n" +
        "~1\r\n+no-writes\r\n$12\r\nlibrary_code\r\n$16\r\n" + code + "\r\n",
      Vector(
        LibraryInfo(
          "mylib",
          "LUA",
          Vector(FunctionInfo("other", None, Set.empty), FunctionInfo("myfunc", Some("desc"), Set("no-writes"))),
          Some(code)
        )
      )
    )
    decodesAlike(
      Functions.functionStats,
      "*4\r\n$14\r\nrunning_script\r\n$-1\r\n$7\r\nengines\r\n*2\r\n$3\r\nLUA\r\n*4\r\n$15\r\nlibraries_count\r\n:1\r\n$15\r\nfunctions_count\r\n:2\r\n",
      "%2\r\n$14\r\nrunning_script\r\n_\r\n$7\r\nengines\r\n%1\r\n$3\r\nLUA\r\n%2\r\n$15\r\nlibraries_count\r\n:1\r\n$15\r\nfunctions_count\r\n:2\r\n",
      FunctionStats(None, Map("LUA" -> EngineStats(1L, 2L)))
    )
  }

  test("FUNCTION DUMP decodes the opaque payload as bytes") {
    val payload = Bytes.utf8("\u0000binary")
    assertEquals(Reply.decode(Functions.functionDump, Frame.BulkString(payload)).toEither.map(_.asUtf8String), Right(payload.asUtf8String))
  }
}

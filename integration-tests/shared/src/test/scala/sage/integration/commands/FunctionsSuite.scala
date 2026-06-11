package sage.integration.commands

import kyo.compat.*

import sage.commands.FlushMode
import sage.integration.{Images, ServerSuite}
import sage.protocol.Frame

abstract class FunctionsSuite(image: String) extends ServerSuite(image) {

  private val library =
    """#!lua name=saregg
      |redis.register_function('saregg_echo', function(keys, args) return args[1] end)
      |redis.register_function('saregg_one', function(keys, args) return 1 end)
      |""".stripMargin

  test("FUNCTION LOAD registers a library that FCALL then invokes") {
    withClient { client =>
      for {
        _    <- client.functionFlush(Some(FlushMode.Sync))
        name <- client.functionLoad(library)
        one  <- client.fCall("saregg_one")
        echo <- client.fCall("saregg_echo", Seq.empty[String], Seq("hello"))
      } yield {
        assertEquals(name, "saregg")
        assertEquals(one, Frame.Integer(1L))
        echo match {
          case Frame.BulkString(b) => assertEquals(b.asUtf8String, "hello")
          case other               => fail(s"expected bulk string, got $other")
        }
      }
    }
  }

  test("FUNCTION LIST and STATS describe loaded libraries; DELETE removes them") {
    withClient { client =>
      for {
        _         <- client.functionFlush(Some(FlushMode.Sync))
        _         <- client.functionLoad(library)
        libraries <- client.functionList()
        withCode  <- client.functionList(Some("saregg"), withCode = true)
        stats     <- client.functionStats
        _         <- client.functionDelete("saregg")
        afterDel  <- client.functionList()
      } yield {
        val lib = libraries.find(_.libraryName == "saregg")
        assert(lib.isDefined, libraries.toString)
        assertEquals(lib.map(_.engine), Some("LUA"))
        assertEquals(lib.map(_.functions.map(_.name).toSet), Some(Set("saregg_echo", "saregg_one")))
        assertEquals(withCode.flatMap(_.code).headOption.isDefined, true)
        assert(stats.engines.contains("LUA"), stats.toString)
        assert(afterDel.forall(_.libraryName != "saregg"))
      }
    }
  }

  test("FUNCTION DUMP and RESTORE round-trip the library payload") {
    withClient { client =>
      for {
        _        <- client.functionFlush(Some(FlushMode.Sync))
        _        <- client.functionLoad(library)
        payload  <- client.functionDump
        _        <- client.functionFlush(Some(FlushMode.Sync))
        empty    <- client.functionList()
        _        <- client.functionRestore(payload)
        restored <- client.functionList()
      } yield {
        assert(empty.isEmpty)
        assert(restored.exists(_.libraryName == "saregg"))
      }
    }
  }
}

class RedisFunctionsSuite  extends FunctionsSuite(Images.redis)
class ValkeyFunctionsSuite extends FunctionsSuite(Images.valkey)

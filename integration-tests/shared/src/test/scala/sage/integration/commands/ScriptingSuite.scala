package sage.integration.commands

import kyo.compat.*

import sage.commands.FlushMode
import sage.integration.{Images, ServerSuite}
import sage.protocol.Frame

abstract class ScriptingSuite(image: String) extends ServerSuite(image) {

  private val absent = "0" * 40

  test("EVAL returns the raw reply and passes keys and args to the script") {
    withClient { client =>
      for {
        one   <- client.eval("return 1")
        keys  <- client.eval("return #KEYS", Seq("a", "b"))
        arg   <- client.eval("return redis.call('set', KEYS[1], ARGV[1])", Seq("eval-k"), Seq("v"))
        value <- client.get[String]("eval-k")
      } yield {
        assertEquals(one, Frame.Integer(1L))
        assertEquals(keys, Frame.Integer(2L))
        assertEquals(arg, Frame.SimpleString("OK"))
        assertEquals(value, Some("v"))
      }
    }
  }

  test("EVAL_RO runs a read-only script") {
    withClient(client => client.evalRo("return 42").map(assertEquals(_, Frame.Integer(42L))))
  }

  test("SCRIPT LOAD returns a sha that EVALSHA then runs; SCRIPT EXISTS reports per-sha presence") {
    withClient { client =>
      for {
        sha    <- client.scriptLoad("return 7")
        ran    <- client.evalSha(sha)
        ranRo  <- client.evalShaRo(sha)
        exists <- client.scriptExists(sha, absent)
      } yield {
        assertEquals(sha.length, 40)
        assertEquals(ran, Frame.Integer(7L))
        assertEquals(ranRo, Frame.Integer(7L))
        assertEquals(exists, Vector(true, false))
      }
    }
  }

  test("SCRIPT FLUSH clears the cache") {
    withClient { client =>
      for {
        sha    <- client.scriptLoad("return 1")
        before <- client.scriptExists(sha)
        _      <- client.scriptFlush(Some(FlushMode.Sync))
        after  <- client.scriptExists(sha)
      } yield {
        assertEquals(before, Vector(true))
        assertEquals(after, Vector(false))
      }
    }
  }
}

class RedisScriptingSuite  extends ScriptingSuite(Images.redis)
class ValkeyScriptingSuite extends ScriptingSuite(Images.valkey)

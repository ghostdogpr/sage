package sage.integration.commands

import kyo.compat.*

import sage.commands.FlushMode
import sage.integration.BothServersSuite
import sage.protocol.Frame

class ScriptingSuite extends BothServersSuite {

  private val absent = "0" * 40

  clientTest("EVAL returns the raw reply and passes keys and args to the script") { client =>
    client.eval("return 1").is(Frame.Integer(1L)) >>
      client.eval("return #KEYS", Seq("a", "b")).is(Frame.Integer(2L)) >>
      client.eval("return redis.call('set', KEYS[1], ARGV[1])", Seq("eval-k"), Seq("v")).is(Frame.SimpleString("OK")) >>
      client.get[String]("eval-k").is(Some("v"))
  }

  clientTest("EVAL_RO runs a read-only script")(client => client.evalRo("return 42").is(Frame.Integer(42L)))

  clientTest("SCRIPT LOAD returns a sha that EVALSHA then runs; SCRIPT EXISTS reports per-sha presence") { client =>
    for {
      sha <- client.scriptLoad("return 7")
      _   <- client.evalSha(sha).is(Frame.Integer(7L))
      _   <- client.evalShaRo(sha).is(Frame.Integer(7L))
      _   <- client.scriptExists(sha, absent).is(Vector(true, false))
    } yield assertEquals(sha.length, 40)
  }

  clientTest("SCRIPT FLUSH clears the cache") { client =>
    for {
      sha <- client.scriptLoad("return 1")
      _   <- client.scriptExists(sha).is(Vector(true))
      _   <- client.scriptFlush(Some(FlushMode.Sync))
      _   <- client.scriptExists(sha).is(Vector(false))
    } yield ()
  }
}

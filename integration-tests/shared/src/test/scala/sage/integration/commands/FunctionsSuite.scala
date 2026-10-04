package sage.integration.commands

import kyo.compat.*

import sage.commands.FlushMode
import sage.integration.BothServersSuite
import sage.protocol.{Frame, Frames}

class FunctionsSuite extends BothServersSuite {

  private val library =
    """#!lua name=saregg
      |redis.register_function('saregg_echo', function(keys, args) return args[1] end)
      |redis.register_function('saregg_one', function(keys, args) return 1 end)
      |""".stripMargin

  clientTest("FUNCTION LOAD registers a library that FCALL then invokes") { client =>
    client.functionFlush(Some(FlushMode.Sync)) >>
      client.functionLoad(library).is("saregg") >>
      client.fCall("saregg_one").is(Frame.Integer(1L)) >>
      client.fCall("saregg_echo", Seq.empty[String], Seq("hello")).is(Frames.bulk("hello"))
  }

  clientTest("FCALL_RO invokes a function flagged no-writes") { client =>
    client.functionFlush(Some(FlushMode.Sync)) >>
      client.functionLoad(
        """#!lua name=saregg
      |redis.register_function{function_name='saregg_ro', callback=function(keys, args) return args[1] end, flags={'no-writes'}}
      |""".stripMargin
      ) >>
      client.fCallRo("saregg_ro", Seq.empty[String], Seq("hi")).is(Frames.bulk("hi"))
  }

  clientTest("FUNCTION LIST and STATS describe loaded libraries; DELETE removes them") { client =>
    client.functionFlush(Some(FlushMode.Sync)) >>
      client.functionLoad(library) >>
      client
        .functionList()
        .map(_.map(l => (l.libraryName, l.engine, l.functions.map(_.name).toSet)))
        .is(Vector(("saregg", "LUA", Set("saregg_echo", "saregg_one")))) >>
      client.functionList(Some("saregg"), withCode = true).map(_.map(_.code)).is(Vector(Some(library))) >>
      client.functionStats.satisfies(_.engines.contains("LUA")) >>
      client.functionDelete("saregg") >>
      client.functionList().is(Vector.empty)
  }

  clientTest("FUNCTION DUMP and RESTORE round-trip the library payload") { client =>
    for {
      _       <- client.functionFlush(Some(FlushMode.Sync))
      _       <- client.functionLoad(library)
      payload <- client.functionDump
      _       <- client.functionFlush(Some(FlushMode.Sync))
      _       <- client.functionList().is(Vector.empty)
      _       <- client.functionRestore(payload)
      _       <- client.functionList().map(_.map(_.libraryName)).is(Vector("saregg"))
    } yield ()
  }
}

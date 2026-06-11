package sage.integration.commands

import kyo.compat.*

import sage.commands.CommandLogType
import sage.integration.{Images, ServerSuite}

/**
  * COMMANDLOG is Valkey's command-log observability (the SLOWLOG successor), absent in Redis, so it has no cross-server counterpart (ADR-0026).
  */
class ValkeyServerExtrasSuite extends ServerSuite(Images.valkey) {

  test("COMMANDLOG GET/LEN/RESET over the slow log") {
    withClient { client =>
      for {
        _      <- client.configSet(("slowlog-log-slower-than", "0"))
        _      <- client.commandLogReset(CommandLogType.Slow)
        _      <- client.get[String, String]("cl-probe")
        len    <- client.commandLogLen(CommandLogType.Slow)
        recent <- client.commandLogGet(5L, CommandLogType.Slow)
        _      <- client.configSet(("slowlog-log-slower-than", "10000"))
        _      <- client.commandLogReset(CommandLogType.Slow)
        after  <- client.commandLogLen(CommandLogType.Slow)
      } yield {
        assert(len > 0L)
        assert(recent.nonEmpty)
        assert(recent.forall(_.command.nonEmpty))
        assertEquals(after, 0L)
      }
    }
  }

  test("COMMANDLOG LEN works for the large-request and large-reply types") {
    withClient { client =>
      for {
        req   <- client.commandLogLen(CommandLogType.LargeRequest)
        reply <- client.commandLogLen(CommandLogType.LargeReply)
      } yield {
        assert(req >= 0L)
        assert(reply >= 0L)
      }
    }
  }
}

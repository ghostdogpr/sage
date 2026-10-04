package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.{CommandLogType, Role}
import sage.integration.BothServersSuite

class ServerAdminSuite extends BothServersSuite {

  clientTest("CONFIG GET and SET read and write a parameter") { client =>
    for {
      before <- client.configGet("maxmemory")
      _      <- client.configSet(("maxmemory", "100mb"))
      after  <- client.configGet("maxmemory")
      _      <- client.configSet(("maxmemory", before.getOrElse("maxmemory", "0")))
    } yield {
      assert(before.contains("maxmemory"))
      assertEquals(after.get("maxmemory"), Some("104857600"))
    }
  }

  clientTest("DBSIZE, FLUSHDB, ECHO, TIME") { client =>
    client.set("admin-k", "v") >>
      client.dbSize.satisfies(_ >= 1L) >>
      client.flushDb() >>
      client.dbSize.is(0L) >>
      client.echo("ping").is("ping") >>
      client.time.satisfies(_.getEpochSecond > 1_000_000_000L)
  }

  clientTest("ROLE reports a standalone server as master") { client =>
    client.role.map {
      case Role.Master(_, _) => ()
      case other             => fail(s"expected master, got $other")
    }
  }

  clientTest("CLIENT ID/GETNAME/INFO/LIST and WAIT") { client =>
    client.clientId.satisfies(_ > 0L) >>
      client.clientGetName.is("") >>
      client.clientInfo.satisfies(_.contains("id=")) >>
      client.clientList.satisfies(_.contains("addr=")) >>
      client.waitReplicas(0L, 100.millis).is(0L)
  }

  clientTest("COMMAND COUNT/INFO/GETKEYS") { client =>
    client.commandCount.satisfies(_ > 100L) >>
      client.commandInfo("get", "set").map(_.map(_.name).toSet).is(Set("get", "set")) >>
      client.commandGetKeys("SET", "k", "v").is(Vector("k"))
  }

  clientTest("MEMORY USAGE, SLOWLOG, LATENCY, ACL reads") { client =>
    client.set("mem-k", "value") >>
      client.memoryUsage("mem-k").satisfies(_.exists(_ > 0L)) >>
      client.slowLogReset >>
      client.slowLogLen.is(0L) >>
      client.latencyLatest >>
      client.aclWhoAmI.is("default") >>
      client.aclUsers.satisfies(_.contains("default")) >>
      client.aclGetUser("default").satisfies(_.exists(_.flags.nonEmpty))
  }

  // COMMANDLOG exists only on Valkey.
  valkeyTest("COMMANDLOG GET/LEN/RESET over the slow log") { client =>
    client.configSet(("slowlog-log-slower-than", "0")) >>
      client.commandLogReset(CommandLogType.Slow) >>
      client.get[String]("cl-probe") >>
      client.commandLogLen(CommandLogType.Slow).satisfies(_ > 0L) >>
      client.commandLogGet(5L, CommandLogType.Slow).satisfies(recent => recent.nonEmpty && recent.forall(_.command.nonEmpty)) >>
      client.configSet(("slowlog-log-slower-than", "10000")) >>
      client.commandLogReset(CommandLogType.Slow) >>
      client.commandLogLen(CommandLogType.Slow).is(0L)
  }

  valkeyTest("COMMANDLOG LEN works for the large-request and large-reply types") { client =>
    client.commandLogLen(CommandLogType.LargeRequest).satisfies(_ >= 0L) >>
      client.commandLogLen(CommandLogType.LargeReply).satisfies(_ >= 0L)
  }
}

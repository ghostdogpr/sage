package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.Role
import sage.integration.{Images, ServerSuite}

abstract class ServerAdminSuite(image: String) extends ServerSuite(image) {

  test("CONFIG GET and SET read and write a parameter") {
    withClient { client =>
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
  }

  test("DBSIZE, FLUSHDB, ECHO, TIME") {
    withClient { client =>
      for {
        _     <- client.set("admin-k", "v")
        size  <- client.dbSize
        _     <- client.flushDb()
        empty <- client.dbSize
        echo  <- client.echo("ping")
        time  <- client.time
      } yield {
        assert(size >= 1L)
        assertEquals(empty, 0L)
        assertEquals(echo, "ping")
        assert(time.getEpochSecond > 1_000_000_000L)
      }
    }
  }

  test("ROLE reports a standalone server as master") {
    withClient { client =>
      client.role.map {
        case Role.Master(_, _) => ()
        case other             => fail(s"expected master, got $other")
      }
    }
  }

  test("CLIENT ID/GETNAME/INFO/LIST and WAIT") {
    withClient { client =>
      for {
        id     <- client.clientId
        name   <- client.clientGetName
        info   <- client.clientInfo
        list   <- client.clientList
        waited <- client.waitReplicas(0L, 100.millis)
      } yield {
        assert(id > 0L)
        assertEquals(name, "")
        assert(info.contains("id="))
        assert(list.contains("addr="))
        assertEquals(waited, 0L)
      }
    }
  }

  test("COMMAND COUNT/INFO/GETKEYS") {
    withClient { client =>
      for {
        count <- client.commandCount
        infos <- client.commandInfo("get", "set")
        keys  <- client.commandGetKeys("SET", "k", "v")
      } yield {
        assert(count > 100L)
        assertEquals(infos.map(_.name).toSet, Set("get", "set"))
        assertEquals(keys, Vector("k"))
      }
    }
  }

  test("MEMORY USAGE, SLOWLOG, LATENCY, ACL reads") {
    withClient { client =>
      for {
        _      <- client.set("mem-k", "value")
        usage  <- client.memoryUsage("mem-k")
        _      <- client.slowLogReset
        len    <- client.slowLogLen
        latest <- client.latencyLatest
        who    <- client.aclWhoAmI
        users  <- client.aclUsers
        user   <- client.aclGetUser("default")
      } yield {
        assert(usage.exists(_ > 0L))
        assertEquals(len, 0L)
        assert(latest.isEmpty || latest.nonEmpty)
        assertEquals(who, "default")
        assert(users.contains("default"))
        assert(user.exists(_.flags.nonEmpty))
      }
    }
  }
}

class RedisServerAdminSuite  extends ServerAdminSuite(Images.redis)
class ValkeyServerAdminSuite extends ServerAdminSuite(Images.valkey)

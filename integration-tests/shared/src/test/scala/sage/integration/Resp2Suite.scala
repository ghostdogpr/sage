package sage.integration

import scala.concurrent.duration.*

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.munit.TestContainersForAll
import kyo.compat.*
import munit.{Location, TestOptions}
import org.testcontainers.containers.wait.strategy.Wait

import sage.Message
import sage.SageException.{ServerError, UnsupportedServer}
import sage.client.{AuthConfig, RespProtocol, SageConfig}
import sage.client.internal.Client
import sage.commands.{BlockTimeout, Commands}

/**
  * RESP2 connections against Redis 5, which has no `HELLO`, and against the current Redis with RESP2 forced. Tests against Redis 5 must use
  * only commands that Redis 5 supports.
  */
class Resp2Suite extends ContainerClient with TestContainersForAll {

  override type Containers = GenericContainer and GenericContainer and GenericContainer

  // the default PING wait fails before authentication, so wait for the ready log line instead
  private val passwordServer: GenericContainer.Def[GenericContainer] =
    GenericContainer.Def(
      Images.preHelloRedis,
      exposedPorts = Seq(6379),
      command = Seq("--requirepass", "secret"),
      waitStrategy = Wait.forLogMessage(".*Ready to accept connections.*", 1)
    )

  override def startContainers(): Containers =
    serverDef(Images.preHelloRedis).start() and
      passwordServer.start() and
      serverDef(Images.redis, command = Seq("--user", "app", "on", ">apppass", "~*", "+@all")).start()

  // Redis 5 cannot use RESP3, so a RESP3 run connects to it with Auto
  private def legacy(server: GenericContainer): SageConfig =
    configOf(server).copy(protocol = if (TestProtocol.selected == RespProtocol.Resp3) RespProtocol.Auto else TestProtocol.selected)

  private def resp2(server: GenericContainer): SageConfig = configOf(server).copy(protocol = RespProtocol.Resp2)

  private def legacyTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit =
    containerTest(options) { case open and _ and _ => connectAndUse(legacy(open))(body) }

  private def currentTest(options: TestOptions)(body: GenericContainer => CIO[Any])(using Location): Unit =
    containerTest(options) { case _ and _ and current => body(current) }

  // includes hashes and sorted sets because their replies differ between RESP2 and RESP3
  private def basicCommands(client: Client[CIO, String], prefix: String): CIO[Unit] =
    client.ping().is("PONG") >>
      client.set(s"$prefix:greeting", "hello") >>
      client.get[String](s"$prefix:greeting").is(Some("hello")) >>
      client.get[String](s"$prefix:missing").is(None) >>
      client.incr(s"$prefix:counter").is(1L) >>
      client.hSet(s"$prefix:hash", ("a", "1"), ("b", "2")).is(2L) >>
      client.hGetAll[String, String](s"$prefix:hash").is(Map("a" -> "1", "b" -> "2")) >>
      client.zAdd[String](s"$prefix:zset")(("m", 1.5)).is(1L) >>
      client.zScore[String](s"$prefix:zset", "m").is(Some(1.5)) >>
      client
        .pipeline((Commands.get[String, String](s"$prefix:greeting"), Commands.incr[String](s"$prefix:counter")))
        .is((Some("hello"), 2L))

  private def usesResp(client: Client[CIO, String], version: Int): CIO[Unit] =
    client.clientInfo.satisfies(_.split("\\s+").contains(s"resp=$version"))

  private def cachedReadIsLatestOn(config: SageConfig, key: String): CIO[Unit] =
    connectAndUse(config)(reader => connectAndUse(config)(cachedReadIsLatest(reader, _, key)))

  containerTest("Auto falls back to RESP2 on a server without HELLO and runs basic commands") { case open and _ and _ =>
    connectAndUse(configOf(open).copy(protocol = RespProtocol.Auto))(basicCommands(_, "auto"))
  }

  containerTest("Resp3 fails with UnsupportedServer on a server without HELLO") { case open and _ and _ =>
    failsWith[UnsupportedServer](connectAndUse(configOf(open).copy(protocol = RespProtocol.Resp3))(_.ping()))
  }

  containerTest("a requirepass server accepts the password for the default user") { case _ and secured and _ =>
    connectAndUse(legacy(secured).copy(auth = Some(AuthConfig("secret")))) { client =>
      client.set("auth:key", "ok") >> client.get[String]("auth:key").is(Some("ok"))
    }
  }

  containerTest("a requirepass server rejects a wrong password") { case _ and secured and _ =>
    failsWith[ServerError](connectAndUse(legacy(secured).copy(auth = Some(AuthConfig("wrong"))))(_.ping()))
  }

  legacyTest("a subscription receives published messages") { client =>
    withSubscription(client.subscribeChannels[String]("legacy-news")) { sub =>
      client.publish("legacy-news", "hello").is(1L) >>
        sub.next.is(Some(Message("legacy-news", "hello"))) >>
        client.publish("legacy-news", "world") >>
        sub.next.is(Some(Message("legacy-news", "world")))
    }
  }

  // Redis 5 accepts only whole seconds as a BLPOP timeout
  legacyTest("BLPOP waits on a dedicated connection, returns a pushed element, and times out to None") { client =>
    CIO
      .zip(client.blPop[String]("legacy-queue")(BlockTimeout.After(5.seconds)), client.ping().flatMap(_ => client.rPush("legacy-queue", "job")))
      .is((Some(("legacy-queue", "job")), 1L)) >>
      client.blPop[String]("legacy-empty")(BlockTimeout.After(1.second)).is(None)
  }

  legacyTest("a MULTI/EXEC transaction guarded by WATCH commits typed results") { client =>
    client.set("legacy-tx", 5) >>
      client
        .transaction { tx =>
          for {
            _   <- tx.watch("legacy-tx")
            cur <- tx.get[Int]("legacy-tx")
            res <- tx.exec((Commands.set[String, Int]("legacy-tx", cur.getOrElse(0) + 1), Commands.incr[String]("legacy-tx")))
          } yield res
        }
        .is(Some((true, 7L)))
  }

  containerTest("a cached read on a server without HELLO returns the latest value after another client writes") { case open and _ and _ =>
    cachedReadIsLatestOn(legacy(open), "legacy-cached")
  }

  currentTest("Resp2 against the current Redis runs basic commands on a RESP2 connection") { current =>
    connectAndUse(resp2(current))(client => basicCommands(client, "forced") >> usesResp(client, 2))
  }

  currentTest("Resp2 against the current Redis authenticates an ACL user with AUTH and rejects a wrong password") { current =>
    connectAndUse(resp2(current).copy(auth = Some(AuthConfig("apppass", username = "app")))) { client =>
      client.aclWhoAmI.is("app") >> usesResp(client, 2)
    } >>
      failsWith[ServerError](connectAndUse(resp2(current).copy(auth = Some(AuthConfig("wrong", username = "app"))))(_.ping()))
  }

  currentTest("Resp2 against the current Redis disables client-side caching, so a cached read returns the latest value") { current =>
    cachedReadIsLatestOn(resp2(current), "forced-cached")
  }

  currentTest("connections negotiate the protocol that SAGE_IT_PROTOCOL selects") { current =>
    connectAndUse(configOf(current))(usesResp(_, if (TestProtocol.isResp2) 2 else 3))
  }
}

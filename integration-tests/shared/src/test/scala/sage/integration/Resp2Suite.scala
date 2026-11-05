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

  override type Containers = GenericContainer and GenericContainer

  // the default PING wait fails before authentication, so wait for the ready log line instead
  private val passwordServer: GenericContainer.Def[GenericContainer] =
    GenericContainer.Def(
      Images.preHelloRedis,
      exposedPorts = Seq(6379),
      command = Seq("--requirepass", "secret"),
      waitStrategy = Wait.forLogMessage(".*Ready to accept connections.*", 1)
    )

  override def startContainers(): Containers =
    passwordServer.start() and
      serverDef(Images.redis, command = Seq("--user", "app", "on", ">apppass", "~*", "+@all")).start()

  private def resp2(server: GenericContainer): SageConfig = configOf(server).copy(protocol = RespProtocol.Resp2)

  private def legacyTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit =
    containerTest(options) { case secured and _ => connectAndUse(configOf(secured).copy(auth = Some(AuthConfig("secret"))))(body) }

  private def currentTest(options: TestOptions)(body: GenericContainer => CIO[Any])(using Location): Unit =
    containerTest(options) { case _ and current => body(current) }

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

  containerTest("Auto falls back to RESP2 on a server without HELLO, authenticates the default user and runs basic commands") { case secured and _ =>
    connectAndUse(configOf(secured).copy(protocol = RespProtocol.Auto, auth = Some(AuthConfig("secret"))))(basicCommands(_, "auto"))
  }

  // Redis 5 looks a command up before it checks authentication, so it rejects HELLO as unknown without credentials
  containerTest("Resp3 fails with UnsupportedServer on a server without HELLO") { case secured and _ =>
    failsWith[UnsupportedServer](connectAndUse(configOf(secured).copy(protocol = RespProtocol.Resp3))(_.ping()))
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

  currentTest("Resp2 against the current Redis authenticates an ACL user with AUTH, runs basic commands and rejects a wrong password") { current =>
    connectAndUse(resp2(current).copy(auth = Some(AuthConfig("apppass", username = "app")))) { client =>
      basicCommands(client, "forced") >> client.aclWhoAmI.is("app") >> usesResp(client, 2)
    } >>
      failsWith[ServerError](connectAndUse(resp2(current).copy(auth = Some(AuthConfig("wrong", username = "app"))))(_.ping()))
  }

  currentTest("Resp2 against the current Redis disables client-side caching, so a cached read returns the latest value") { current =>
    connectAndUse(resp2(current)) { reader =>
      connectAndUse(resp2(current)) { writer =>
        val read = reader.cached(Commands.get[String, String]("forced-cached"), 1.minute)
        writer.set("forced-cached", "v1") >> read.is(Some("v1")) >> writer.set("forced-cached", "v2") >> read.is(Some("v2"))
      }
    }
  }

  currentTest("connections negotiate the protocol that SAGE_IT_PROTOCOL selects") { current =>
    connectAndUse(configOf(current))(usesResp(_, if (TestProtocol.selected == RespProtocol.Resp2) 2 else 3))
  }
}

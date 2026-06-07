package sage.integration

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

import kyo.compat.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.client.SageConfig
import sage.client.internal.Client
import sage.commands.Command
import sage.protocol.Frame

abstract class RoundTripSuite(image: String) extends ServerSuite(image) {

  // not private: only the Ox cell's unsafeRun consumes it, and a private given would be flagged unused on the other cells
  given ExecutionContext = munitExecutionContext

  private def withClient[A](body: Client[CIO] => CIO[A]): Future[A] =
    withContainers(server => connectAndUse(configOf(server))(body).unsafeRun)

  // CIO.acquireReleaseWith fails to compile on the Ox/Future cells when its type argument nests CIO (Client[CIO]); fold instead
  private def connectAndUse[A](config: SageConfig)(body: Client[CIO] => CIO[A]): CIO[A] =
    Client.connect(config).flatMap { client =>
      body(client).fold(
        result => client.close.map(_ => result),
        error => client.close.flatMap(_ => CIO.fail(error))
      )
    }

  test("ping round-trips") {
    withClient(client => client.ping().map(reply => assertEquals(reply, "PONG")))
  }

  test("set then get returns the value") {
    withClient { client =>
      for {
        _     <- client.set("greeting", "hello")
        value <- client.get[String, String]("greeting")
      } yield assertEquals(value, Some("hello"))
    }
  }

  test("heterogeneous value types coexist on one client") {
    withClient { client =>
      for {
        _     <- client.set("count", 42)
        _     <- client.set("flag", true)
        count <- client.get[String, Int]("count")
        flag  <- client.get[String, Boolean]("flag")
      } yield {
        assertEquals(count, Some(42))
        assertEquals(flag, Some(true))
      }
    }
  }

  test("get of a missing key is None") {
    withClient(client => client.get[String, String]("missing-key").map(value => assertEquals(value, None)))
  }

  test("concurrent fibers pipeline onto the Multiplexed Connection and match FIFO") {
    withClient { client =>
      CIO
        .foreach(1 to 200) { i =>
          for {
            _     <- client.set(s"key-$i", s"value-$i")
            value <- client.get[String, String](s"key-$i")
          } yield assertEquals(value, Some(s"value-$i"))
        }
        .unit
    }
  }

  test("closing the client releases its server connection") {
    withContainers { server =>
      connectAndUse(configOf(server)) { observer =>
        for {
          subject <- Client.connect(configOf(server))
          before  <- connectionCount(observer)
          _       <- subject.close
          _       <- awaitConnectionCount(observer, before - 1, attempts = 50)
        } yield ()
      }.unsafeRun
    }
  }

  private val clientList: Command[String] =
    Command(
      "CLIENT",
      keyIndices = Command.NoKeys,
      args = Vector(Bytes.utf8("LIST")),
      decode = {
        case Frame.BulkString(value)        => Right(value.asUtf8String)
        case Frame.VerbatimString(_, value) => Right(value.asUtf8String)
        case other                          => Left(DecodeError("bulk or verbatim string", Frame.describe(other)))
      }
    )

  private def connectionCount(client: Client[CIO]): CIO[Int] =
    client.run(clientList).map(_.linesIterator.count(_.nonEmpty))

  private def awaitConnectionCount(client: Client[CIO], expected: Int, attempts: Int): CIO[Unit] =
    connectionCount(client).flatMap { count =>
      if (count == expected) CIO.value(())
      else if (attempts <= 1) CIO.fail(new AssertionError(s"expected $expected connections, still $count"))
      else CIO.sleep(100.millis).flatMap(_ => awaitConnectionCount(client, expected, attempts - 1))
    }
}

class RedisRoundTripSuite extends RoundTripSuite("redis:8")

class ValkeyRoundTripSuite extends RoundTripSuite("valkey/valkey:8")

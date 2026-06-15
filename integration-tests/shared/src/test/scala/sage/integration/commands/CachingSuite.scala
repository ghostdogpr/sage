package sage.integration.commands

import scala.concurrent.Future
import scala.concurrent.duration.*

import kyo.compat.*

import sage.SageException.NotCacheable
import sage.client.internal.Client
import sage.commands.Commands
import sage.integration.{Images, ServerSuite}

abstract class CachingSuite(image: String) extends ServerSuite(image) {

  // a reader (whose cache we observe) plus a writer on a separate connection, so a write is a genuine server-side change
  private def withReaderAndWriter[A](body: (Client[CIO, String], Client[CIO, String]) => CIO[A]): Future[A] =
    withContainers { server =>
      val config = configOf(server)
      connectAndUse(config)(reader => connectAndUse(config)(writer => body(reader, writer))).unsafeRun
    }

  // cached reads served locally never re-contact the server, so an external write only shows up once its invalidation push lands; poll
  private def awaitCached(client: Client[CIO, String], key: String, expected: String, attempts: Int): CIO[Option[String]] =
    client.cached(Commands.get[String, String](key), 1.minute).flatMap { value =>
      if (value.contains(expected) || attempts <= 1) CIO.value(value)
      else CIO.sleep(100.millis).flatMap(_ => awaitCached(client, key, expected, attempts - 1))
    }

  test("a repeated cached read is served locally and a server-side write evicts it via invalidation") {
    withReaderAndWriter { (reader, writer) =>
      for {
        _       <- writer.set("csc:key", "v1")
        first   <- reader.cached(Commands.get[String, String]("csc:key"), 1.minute) // fetch + cache
        cached  <- reader.cached(Commands.get[String, String]("csc:key"), 1.minute) // local hit
        _       <- writer.set("csc:key", "v2")                                      // server-side write -> invalidation push
        evicted <- awaitCached(reader, "csc:key", "v2", attempts = 50)
      } yield {
        assertEquals(first, Some("v1"))
        assertEquals(cached, Some("v1"))
        assertEquals(evicted, Some("v2"))
      }
    }
  }

  test("cached rejects a non-read-only command with NotCacheable") {
    withClient { client =>
      client
        .cached(Commands.set[String, String]("csc:write", "v"), 1.minute)
        .fold(
          _ => CIO.value(false),
          {
            case _: NotCacheable => CIO.value(true)
            case _               => CIO.value(false)
          }
        )
        .map(rejected => assert(rejected, "expected cached on SET to fail with NotCacheable"))
    }
  }
}

class RedisCachingSuite extends CachingSuite(Images.redis)

class ValkeyCachingSuite extends CachingSuite(Images.valkey)

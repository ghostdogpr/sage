package sage.integration

import scala.concurrent.duration.*

import kyo.compat.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.client.internal.Client
import sage.commands.{Command, Pipeline, Strings}
import sage.commands.Pipeline.pipeline
import sage.protocol.Frame

abstract class RoundTripSuite(image: String) extends ServerSuite(image) {

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

  test("no reply misattribution under high fiber concurrency") {
    def pingLoop(client: Client[CIO], fiber: Int, i: Int): CIO[Unit] =
      if (i > 100) CIO.value(())
      else {
        val token = s"$fiber-$i"
        client.ping(Some(token)).flatMap { reply =>
          assertEquals(reply, token)
          pingLoop(client, fiber, i + 1)
        }
      }
    withClient(client => CIO.foreachDiscard(1 to 500)(fiber => pingLoop(client, fiber, 1)))
  }

  test("a pipeline yields one typed result per command in a single round-trip") {
    withClient { client =>
      for {
        _   <- client.set("p:a", "x")
        _   <- client.set("p:n", 10)
        out <- client.pipeline((Strings.get[String, String]("p:a"), Strings.incrBy[String]("p:n", 5)).pipeline)
      } yield assertEquals(out, (Some("x"), 15L))
    }
  }

  test("a command failure in a pipeline surfaces per-position without poisoning the rest") {
    withClient { client =>
      for {
        _       <- client.set("p:str", "hello")
        results <- client.pipelineAttempt(
                     (
                       Strings.get[String, String]("p:str"),
                       Strings.incr[String]("p:str"),
                       Strings.get[String, String]("p:str")
                     ).pipeline
                   )
      } yield {
        val (a, b, c) = results
        assertEquals(a, Right(Some("hello")))
        assert(b.isLeft, s"expected the INCR on a string to fail, got $b")
        assertEquals(c, Right(Some("hello")))
      }
    }
  }

  test("a large pipeline runs every command and returns one result per position") {
    withClient { client =>
      val n = 200
      client.pipeline(Pipeline.sequence(Vector.fill(n)(Strings.incr[String]("p:rtt")))).flatMap { results =>
        client.get[String, Int]("p:rtt").map { stored =>
          assertEquals(results.length, n)
          assertEquals(results, (1 to n).map(_.toLong).toVector)
          assertEquals(stored, Some(n))
        }
      }
    }
  }

  test("a transaction commits atomically and returns typed results") {
    withClient { client =>
      for {
        _   <- client.set("t:n", 10)
        out <- client.transaction(tx => tx.exec((Strings.incr[String]("t:n"), Strings.incrBy[String]("t:n", 5)).pipeline))
      } yield assertEquals(out, Some((11L, 16L)))
    }
  }

  test("a read-modify-write transaction commits when the watched key is unchanged") {
    withClient { client =>
      for {
        _      <- client.set("t:rmw", 5)
        out    <- client.transaction { tx =>
                    for {
                      _   <- tx.watch("t:rmw")
                      cur <- tx.run(Strings.get[String, Int]("t:rmw"))
                      res <- tx.exec(Pipeline.sequence(Vector(Strings.set[String, Int]("t:rmw", cur.getOrElse(0) + 1))))
                    } yield res
                  }
        stored <- client.get[String, Int]("t:rmw")
      } yield {
        assert(out.isDefined, s"expected a committed transaction, got $out")
        assertEquals(stored, Some(6))
      }
    }
  }

  test("WATCH aborts the transaction when a watched key is modified concurrently") {
    withContainers { server =>
      connectAndUse(configOf(server)) { client =>
        for {
          other  <- Client.connect(configOf(server))
          _      <- client.set("t:w", 1)
          out    <- client.transaction { tx =>
                      for {
                        _   <- tx.watch("t:w")
                        _   <- tx.run(Strings.get[String, Int]("t:w"))
                        _   <- other.set("t:w", 99) // a different connection changes the watched key before EXEC
                        res <- tx.exec(Pipeline.sequence(Vector(Strings.incr[String]("t:w"))))
                      } yield res
                    }
          _      <- other.close
          stored <- client.get[String, Int]("t:w")
        } yield {
          assertEquals(out, None)        // aborted
          assertEquals(stored, Some(99)) // the INCR never ran
        }
      }.unsafeRun
    }
  }

  test("an execution-phase error surfaces per-position while the other commands commit") {
    withClient { client =>
      for {
        _   <- client.set("t:str", "x")
        res <- client.transaction(tx => tx.execAttempt((Strings.incr[String]("t:fresh"), Strings.incr[String]("t:str")).pipeline))
        ok  <- client.get[String, Int]("t:fresh")
      } yield {
        val (a, b) = res.getOrElse(fail("expected a committed transaction"))
        assertEquals(a, Right(1L))
        assert(b.isLeft, s"expected the INCR on a string to fail, got $b")
        assertEquals(ok, Some(1)) // the first INCR committed despite the second erroring — Redis does not roll back
      }
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

class RedisRoundTripSuite extends RoundTripSuite(Images.redis)

class ValkeyRoundTripSuite extends RoundTripSuite(Images.valkey)

package sage.integration

import kyo.compat.*

import sage.SageException.ServerError
import sage.client.internal.Client
import sage.commands.Commands

class RoundTripSuite extends BothServersSuite {

  clientTest("ping round-trips")(client => client.ping().is("PONG"))

  clientTest("values round-trip per call type, and a missing key is None") { client =>
    client.set("greeting", "hello") >>
      client.set("count", 42) >>
      client.set("flag", true) >>
      client.get[String]("greeting").is(Some("hello")) >>
      client.get[Int]("count").is(Some(42)) >>
      client.get[Boolean]("flag").is(Some(true)) >>
      client.get[String]("missing-key").is(None)
  }

  clientTest("no reply misattribution under high fiber concurrency") { client =>
    def pingLoop(fiber: Int, i: Int): CIO[Unit] =
      if (i > 100) CIO.value(())
      else {
        val token = s"$fiber-$i"
        client.ping(Some(token)).is(token).flatMap(_ => pingLoop(fiber, i + 1))
      }
    CIO.foreachDiscard(1 to 500)(fiber => pingLoop(fiber, 1))
  }

  clientTest("a pipeline yields one typed result per command in a single round-trip") { client =>
    client.set("p:a", "x") >>
      client.set("p:n", 10) >>
      client.pipeline((Commands.get[String, String]("p:a"), Commands.incrBy[String]("p:n", 5))).is((Some("x"), 15L))
  }

  clientTest("a command failure in a pipeline surfaces per-position without poisoning the rest") { client =>
    client.set("p:str", "hello") >>
      client
        .pipelineAttempt(
          (
            Commands.get[String, String]("p:str"),
            Commands.incr[String]("p:str"),
            Commands.get[String, String]("p:str")
          )
        )
        .is((Right(Some("hello")), Left(ServerError("ERR", "value is not an integer or out of range")), Right(Some("hello"))))
  }

  clientTest("a large pipeline runs every command and returns one result per position") { client =>
    val n = 200
    client.pipeline(Vector.fill(n)(Commands.incr[String]("p:rtt"))).is((1 to n).map(_.toLong).toVector) >>
      client.get[Int]("p:rtt").is(Some(n))
  }

  clientTest("a transaction commits atomically and returns typed results") { client =>
    client.set("t:n", 10) >>
      client.transaction(tx => tx.exec((Commands.incr[String]("t:n"), Commands.incrBy[String]("t:n", 5)))).is(Some((11L, 16L)))
  }

  clientTest("a read-modify-write transaction commits when the watched key is unchanged") { client =>
    client.set("t:rmw", 5) >>
      client
        .transaction { tx =>
          for {
            _   <- tx.watch("t:rmw")
            cur <- tx.get[Int]("t:rmw")
            res <- tx.exec(Vector(Commands.set[String, Int]("t:rmw", cur.getOrElse(0) + 1)))
          } yield res
        }
        .is(Some(Vector(true))) >>
      client.get[Int]("t:rmw").is(Some(6))
  }

  clientsTest("WATCH aborts the transaction when a watched key is modified concurrently") { (client, other) =>
    client.set("t:w", 1) >>
      client
        .transaction { tx =>
          for {
            _   <- tx.watch("t:w")
            _   <- tx.get[Int]("t:w")
            _   <- other.set("t:w", 99) // a different connection changes the watched key before EXEC
            res <- tx.exec(Vector(Commands.incr[String]("t:w")))
          } yield res
        }
        .is(None) >>                      // aborted
      client.get[Int]("t:w").is(Some(99)) // the INCR never ran
  }

  clientTest("an execution-phase error surfaces per-position while the other commands commit") { client =>
    client.set("t:str", "x") >>
      client
        .transaction(tx => tx.execAttempt((Commands.incr[String]("t:fresh"), Commands.incr[String]("t:str"))))
        .is(Some((Right(1L), Left(ServerError("ERR", "value is not an integer or out of range"))))) >>
      client.get[Int]("t:fresh").is(Some(1)) // Redis does not roll back, so the first INCR remains committed after the second one fails.
  }

  serverTest("closing the client releases its server connection") { server =>
    connectAndUse(configOf(server)) { observer =>
      connectAndUse(configOf(server))(_ => connectionCount(observer)).flatMap(before => Eventually(50)(connectionCount(observer).is(before - 1)))
    }
  }

  private def connectionCount(client: Client[CIO, String]): CIO[Int] =
    client.clientList.map(_.linesIterator.count(_.nonEmpty))
}

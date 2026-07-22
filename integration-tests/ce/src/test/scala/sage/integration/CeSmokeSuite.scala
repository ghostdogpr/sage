package sage.integration

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*

import sage.*
import sage.backend.*

class CeSmokeSuite extends ServerSuite(Images.redis) {

  test("an end user connects and round-trips with native Cats Effect") {
    withContainers { server =>
      val config = configOf(server)

      val program: IO[Unit] =
        SageClient.resource(config).use { client =>
          for {
            pong   <- client.ping()
            _      <- (1 to 50).toList.parTraverse_(i => client.set(s"key-$i", s"value-$i"))
            values <- (1 to 50).toList.parTraverse(i => client.get[String](s"key-$i"))
          } yield {
            assertEquals(pong, "PONG")
            assertEquals(values, (1 to 50).toList.map(i => Some(s"value-$i")))
          }
        }

      program.unsafeRunSync()
    }
  }

  test("a pipeline returns a typed tuple natively, surfacing failures per position") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          for {
            _       <- client.set("pipe:a", "x")
            _       <- client.set("pipe:n", 10)
            out     <- client.pipeline((Commands.get[String, String]("pipe:a"), Commands.incrBy[String]("pipe:n", 5)))
            _       <- client.set("pipe:str", "hello")
            attempt <- client.pipelineAttempt((Commands.get[String, String]("pipe:str"), Commands.incr[String]("pipe:str")))
          } yield {
            assertEquals(out, (Some("x"), 15L))
            assert(attempt._1 == Right(Some("hello")), attempt._1)
            assert(attempt._2.isLeft, attempt._2)
          }
        }

      program.unsafeRunSync()
    }
  }

  test("a transaction commits atomically with native Cats Effect, guarded by WATCH") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          for {
            _   <- client.set("tx:n", 1)
            out <- client.transaction { tx =>
                     for {
                       _   <- tx.watch("tx:n")
                       _   <- tx.get[Int]("tx:n")
                       res <- tx.exec((Commands.incr[String]("tx:n"), Commands.incrBy[String]("tx:n", 4)))
                     } yield res
                   }
          } yield assertEquals(out, Some((2L, 6L)))
        }

      program.unsafeRunSync()
    }
  }

  test("scanAll streams every key as a native fs2 Stream") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          for {
            _    <- (1 to 50).toList.parTraverse_(i => client.set(s"scan-$i", "v"))
            keys <- client.scanAll(pattern = Some("scan-*"), count = Some(10L)).compile.toVector
          } yield assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)
        }

      program.unsafeRunSync()
    }
  }

  test("subscribe delivers published messages as a native fs2 Stream") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          client.subscribeResource[String]("smoke").use { stream =>
            for {
              _        <- (1 to 3).toList.traverse_(i => client.publish("smoke", s"m$i"))
              messages <- stream.take(3).compile.toVector
            } yield {
              assertEquals(messages.map(_.channel).toSet, Set("smoke"))
              assertEquals(messages.map(_.payload).toList, List("m1", "m2", "m3"))
            }
          }
        }

      program.unsafeRunSync()
    }
  }

  test("client.rateLimiter admits up to capacity then denies") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          val rl = client.rateLimiter[String](RateLimit(capacity = 2, refillTokens = 1, refillPeriod = 1.hour))
          for {
            first  <- rl.tryAcquire("smoke")
            second <- rl.tryAcquire("smoke")
            denied <- rl.tryAcquire("smoke")
          } yield {
            assert(first.isAllowed && second.isAllowed, "the first two are admitted")
            assert(!denied.isAllowed, "the third is denied once the bucket empties")
          }
        }

      program.unsafeRunSync()
    }
  }

  test("hScanAll streams every field/value pair as a native fs2 Stream") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          for {
            _     <- (1 to 50).toList.parTraverse_(i => client.hSet("hscan", (s"f$i", s"v$i")))
            pairs <- client.hScanAll[String, String]("hscan", count = Some(10L)).compile.toVector
          } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)
        }

      program.unsafeRunSync()
    }
  }

  test("sScanAll streams every member as a native fs2 Stream") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          for {
            _       <- (1 to 50).toList.parTraverse_(i => client.sAdd("sscan", s"m$i"))
            members <- client.sScanAll[String]("sscan", count = Some(10L)).compile.toVector
          } yield assertEquals(members.toSet, (1 to 50).map(i => s"m$i").toSet)
        }

      program.unsafeRunSync()
    }
  }

  test("zScanAll streams every member/score pair as a native fs2 Stream") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          for {
            _     <- (1 to 50).toList.parTraverse_(i => client.zAdd("zscan")((s"m$i", i.toDouble)))
            pairs <- client.zScanAll[String]("zscan", count = Some(10L)).compile.toVector
          } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)
        }

      program.unsafeRunSync()
    }
  }
}

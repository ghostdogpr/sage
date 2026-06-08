package sage.integration

import zio.*

import sage.commands.Pipeline.pipeline
import sage.commands.Strings
import sage.zio.*

class ZioSmokeSuite extends ServerSuite(Images.redis) {

  test("an end user connects and round-trips with native ZIO") {
    withContainers { server =>
      val config = configOf(server)

      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(config)
            pong   <- client.ping()
            _      <- ZIO.foreachParDiscard(1 to 50)(i => client.set(s"key-$i", s"value-$i"))
            values <- ZIO.foreachPar((1 to 50).toList)(i => client.get[String, String](s"key-$i"))
          } yield {
            assertEquals(pong, "PONG")
            assertEquals(values, (1 to 50).toList.map(i => Some(s"value-$i")))
          }
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("a pipeline returns a typed tuple natively, surfacing failures per position") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client  <- SageClient.scoped(configOf(server))
            _       <- client.set("pipe:a", "x")
            _       <- client.set("pipe:n", 10)
            out     <- client.pipeline((Strings.get[String, String]("pipe:a"), Strings.incrBy[String]("pipe:n", 5)).pipeline)
            _       <- client.set("pipe:str", "hello")
            attempt <- client.pipelineAttempt((Strings.get[String, String]("pipe:str"), Strings.incr[String]("pipe:str")).pipeline)
          } yield {
            assertEquals(out, (Some("x"), 15L))
            assert(attempt._1 == Right(Some("hello")), attempt._1)
            assert(attempt._2.isLeft, attempt._2)
          }
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("a transaction commits atomically with native ZIO, guarded by WATCH") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(configOf(server))
            _      <- client.set("tx:n", 1)
            out    <- client.transaction { tx =>
                        for {
                          _   <- tx.watch("tx:n")
                          _   <- tx.run(Strings.get[String, Int]("tx:n"))
                          res <- tx.exec((Strings.incr[String]("tx:n"), Strings.incrBy[String]("tx:n", 4)).pipeline)
                        } yield res
                      }
          } yield assertEquals(out, Some((2L, 6L)))
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("scanAll streams every key as a native ZStream") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(configOf(server))
            _      <- ZIO.foreachParDiscard(1 to 50)(i => client.set(s"scan-$i", "v"))
            keys   <- client.scanAll[String](pattern = Some("scan-*"), count = Some(10L)).runCollect
          } yield assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("hScanAll streams every field/value pair as a native ZStream") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(configOf(server))
            _      <- ZIO.foreachParDiscard(1 to 50)(i => client.hSet("hscan", (s"f$i", s"v$i")))
            pairs  <- client.hScanAll[String, String, String]("hscan", count = Some(10L)).runCollect
          } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("sScanAll streams every member as a native ZStream") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client  <- SageClient.scoped(configOf(server))
            _       <- ZIO.foreachParDiscard(1 to 50)(i => client.sAdd("sscan", s"m$i"))
            members <- client.sScanAll[String, String]("sscan", count = Some(10L)).runCollect
          } yield assertEquals(members.toSet, (1 to 50).map(i => s"m$i").toSet)
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("zScanAll streams every member/score pair as a native ZStream") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(configOf(server))
            _      <- ZIO.foreachParDiscard(1 to 50)(i => client.zAdd("zscan")((s"m$i", i.toDouble)))
            pairs  <- client.zScanAll[String, String]("zscan", count = Some(10L)).runCollect
          } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }
}

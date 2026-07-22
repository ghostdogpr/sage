package sage.integration

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import zio.*

import sage.*
import sage.backend.*

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
            values <- ZIO.foreachPar((1 to 50).toList)(i => client.get[String](s"key-$i"))
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
            out     <- client.pipeline((Commands.get[String, String]("pipe:a"), Commands.incrBy[String]("pipe:n", 5)))
            _       <- client.set("pipe:str", "hello")
            attempt <- client.pipelineAttempt((Commands.get[String, String]("pipe:str"), Commands.incr[String]("pipe:str")))
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
                          _   <- tx.get[Int]("tx:n")
                          res <- tx.exec((Commands.incr[String]("tx:n"), Commands.incrBy[String]("tx:n", 4)))
                        } yield res
                      }
          } yield assertEquals(out, Some((2L, 6L)))
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("an interrupted blocking command releases its pooled slot instead of leaking it") {
    withContainers { server =>
      val config              = configOf(server).copy(dedicatedPool =
        sage.client.DedicatedPoolConfig(maxConnections = 2, acquireTimeout = FiniteDuration(1L, TimeUnit.SECONDS))
      )
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(config)
            _      <- ZIO.foreachDiscard(1 to 4)(_ => client.blPop[String]("leak:empty")(BlockTimeout.Forever).timeout(Duration.fromMillis(150)))
            none   <- client.blPop[String]("leak:empty")(BlockTimeout.After(FiniteDuration(100L, TimeUnit.MILLISECONDS)))
          } yield assertEquals(none, None)
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("re-running the same blocking command value succeeds each round instead of hanging after the first") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(configOf(server))
            blPop   = client.blPop[String]("h1:reuse")(BlockTimeout.After(FiniteDuration(100L, TimeUnit.MILLISECONDS)))
            first  <- blPop
            second <- blPop.timeout(Duration.fromSeconds(5))
          } yield {
            assertEquals(first, None)
            assert(second.isDefined, "re-running the same blocking effect hung: its lease was captured and single-shot")
            assertEquals(second.flatten, None)
          }
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
            keys   <- client.scanAll(pattern = Some("scan-*"), count = Some(10L)).runCollect
          } yield assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("subscribe delivers published messages as a native ZStream") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client   <- SageClient.scoped(configOf(server))
            stream   <- client.subscribeScoped[String]("smoke")
            _        <- ZIO.foreachDiscard(1 to 3)(i => client.publish("smoke", s"m$i"))
            messages <- stream.take(3).runCollect
          } yield {
            assertEquals(messages.map(_.channel).toSet, Set("smoke"))
            assertEquals(messages.map(_.payload).toList, List("m1", "m2", "m3"))
          }
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
            pairs  <- client.hScanAll[String, String]("hscan", count = Some(10L)).runCollect
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
            members <- client.sScanAll[String]("sscan", count = Some(10L)).runCollect
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
            pairs  <- client.zScanAll[String]("zscan", count = Some(10L)).runCollect
          } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("xRangeAll pages every entry as a native ZStream") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client  <- SageClient.scoped(configOf(server))
            _       <- ZIO.foreachDiscard(1 to 50)(i => client.xAdd("xrangeall", XAddId.Explicit(StreamId(i.toLong, 0L)))(("f", s"v$i")))
            entries <- client.xRangeAll[String, String]("xrangeall", batch = 10L).runCollect
          } yield assertEquals(entries.map(_.id).toList, (1 to 50).map(i => StreamId(i.toLong, 0L)).toList)
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("xConsume tails a group and auto-acks each entry after the handler succeeds") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(configOf(server))
            _      <- ZIO.foreachDiscard(1 to 3)(i => client.xAdd("xconsume", XAddId.Explicit(StreamId(i.toLong, 0L)))(("f", s"v$i")))
            _      <- client.xGroupCreate("xconsume", "g", GroupStartId.At(StreamId(0L, 0L)))
            seen   <- Ref.make(Vector.empty[String])
            fiber  <- client
                        .xConsume[String, String](
                          "g",
                          "c",
                          "xconsume",
                          block = BlockTimeout.After(scala.concurrent.duration.FiniteDuration(200L, java.util.concurrent.TimeUnit.MILLISECONDS))
                        ) { entry =>
                          seen.update(_ :+ entry.fields.head._2)
                        }
                        .fork
            _      <- seen.get.repeatUntil(_.size >= 3).timeoutFail(new RuntimeException("xConsume did not deliver"))(Duration.fromSeconds(10))
            _      <- fiber.interrupt
            got    <- seen.get
            pend   <- client.xPending("xconsume", "g")
          } yield {
            assertEquals(got.sorted, Vector("v1", "v2", "v3"))
            assertEquals(pend.total, 0L)
          }
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }

  test("client.rateLimiter admits up to capacity then denies") {
    withContainers { server =>
      val program: Task[Unit] =
        ZIO.scoped {
          for {
            client <- SageClient.scoped(configOf(server))
            rl      = client.rateLimiter[String](RateLimit(capacity = 2, refillTokens = 1, refillPeriod = FiniteDuration(1L, TimeUnit.HOURS)))
            first  <- rl.tryAcquire("smoke")
            second <- rl.tryAcquire("smoke")
            denied <- rl.tryAcquire("smoke")
          } yield {
            assert(first.isAllowed && second.isAllowed, "the first two are admitted")
            assert(!denied.isAllowed, "the third is denied once the bucket empties")
          }
        }

      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    }
  }
}

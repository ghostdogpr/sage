package sage.integration

import kyo.*

import sage.commands.Commands
import sage.commands.Pipeline.pipeline
import sage.kyo.*

class KyoSmokeSuite extends ServerSuite(Images.redis) {

  test("an end user connects and round-trips with native Kyo") {
    withContainers { server =>
      val config = configOf(server)

      val program: Unit < (Scope & Abort[Throwable] & Async) =
        for {
          client <- SageClient.scoped(config)
          pong   <- client.ping()
          _      <- Async.foreachDiscard(1 to 50)(i => client.set(s"key-$i", s"value-$i"))
          values <- Async.foreach((1 to 50).toList)(i => client.get[String, String](s"key-$i"))
        } yield {
          assertEquals(pong, "PONG")
          assertEquals(values.toList, (1 to 50).toList.map(i => Some(s"value-$i")))
        }

      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
    }
  }

  test("a pipeline returns a typed tuple natively, surfacing failures per position") {
    withContainers { server =>
      val program: Unit < (Scope & Abort[Throwable] & Async) =
        for {
          client  <- SageClient.scoped(configOf(server))
          _       <- client.set("pipe:a", "x")
          _       <- client.set("pipe:n", 10)
          out     <- client.pipeline((Commands.get[String, String]("pipe:a"), Commands.incrBy[String]("pipe:n", 5)).pipeline)
          _       <- client.set("pipe:str", "hello")
          attempt <- client.pipelineAttempt((Commands.get[String, String]("pipe:str"), Commands.incr[String]("pipe:str")).pipeline)
        } yield {
          assertEquals(out, (Some("x"), 15L))
          assert(attempt._1 == Right(Some("hello")), attempt._1)
          assert(attempt._2.isLeft, attempt._2)
        }

      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
    }
  }

  test("a transaction commits atomically with native Kyo, guarded by WATCH") {
    withContainers { server =>
      val program: Unit < (Scope & Abort[Throwable] & Async) =
        for {
          client <- SageClient.scoped(configOf(server))
          _      <- client.set("tx:n", 1)
          out    <- client.transaction { tx =>
                      for {
                        _   <- tx.watch("tx:n")
                        _   <- tx.get[String, Int]("tx:n")
                        res <- tx.exec((Commands.incr[String]("tx:n"), Commands.incrBy[String]("tx:n", 4)).pipeline)
                      } yield res
                    }
        } yield assertEquals(out, Some((2L, 6L)))

      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
    }
  }

  test("scanAll streams every key as a native Kyo Stream") {
    withContainers { server =>
      val program: Unit < (Scope & Abort[Throwable] & Async) =
        for {
          client <- SageClient.scoped(configOf(server))
          _      <- Async.foreachDiscard(1 to 50)(i => client.set(s"scan-$i", "v"))
          keys   <- client.scanAll[String](pattern = Some("scan-*"), count = Some(10L)).run
        } yield assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)

      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
    }
  }

  test("subscribe delivers published messages as a native Kyo Stream") {
    withContainers { server =>
      val program: Unit < (Scope & Abort[Throwable] & Async) =
        for {
          client    <- SageClient.scoped(configOf(server))
          publisher <- Fiber.init {
                         for {
                           _ <- Async.sleep(300.millis)                                          // let SUBSCRIBE register before publishing
                           _ <- Kyo.foreachDiscard(1 to 3)(i => client.publish("smoke", s"m$i")) // sequential: preserves order
                         } yield ()
                       }
          chunk     <- client.subscribe[String]("smoke").take(3).run
          _         <- publisher.get
        } yield {
          val messages = chunk.toList
          assertEquals(messages.map(_.channel).toSet, Set("smoke"))
          assertEquals(messages.map(_.payload), List("m1", "m2", "m3"))
        }

      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
    }
  }

  test("hScanAll streams every field/value pair as a native Kyo Stream") {
    withContainers { server =>
      val program: Unit < (Scope & Abort[Throwable] & Async) =
        for {
          client <- SageClient.scoped(configOf(server))
          _      <- Async.foreachDiscard(1 to 50)(i => client.hSet("hscan", (s"f$i", s"v$i")))
          pairs  <- client.hScanAll[String, String, String]("hscan", count = Some(10L)).run
        } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)

      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
    }
  }

  test("sScanAll streams every member as a native Kyo Stream") {
    withContainers { server =>
      val program: Unit < (Scope & Abort[Throwable] & Async) =
        for {
          client  <- SageClient.scoped(configOf(server))
          _       <- Async.foreachDiscard(1 to 50)(i => client.sAdd("sscan", s"m$i"))
          members <- client.sScanAll[String, String]("sscan", count = Some(10L)).run
        } yield assertEquals(members.toSet, (1 to 50).map(i => s"m$i").toSet)

      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
    }
  }

  test("zScanAll streams every member/score pair as a native Kyo Stream") {
    withContainers { server =>
      val program: Unit < (Scope & Abort[Throwable] & Async) =
        for {
          client <- SageClient.scoped(configOf(server))
          _      <- Async.foreachDiscard(1 to 50)(i => client.zAdd("zscan")((s"m$i", i.toDouble)))
          pairs  <- client.zScanAll[String, String]("zscan", count = Some(10L)).run
        } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)

      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
    }
  }
}

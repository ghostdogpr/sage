package sage.integration

import zio.*

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
}

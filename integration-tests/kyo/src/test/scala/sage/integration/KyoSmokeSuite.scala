package sage.integration

import kyo.*

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
}

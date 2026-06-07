package sage.integration

import kyo.*

import sage.kyo.SageClient

class KyoSmokeSuite extends ServerSuite("redis:8") {

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
}

package sage.integration

import zio.*

import sage.zio.SageClient

class ZioSmokeSuite extends ServerSuite("redis:8") {

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
}

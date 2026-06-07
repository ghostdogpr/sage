package sage.integration

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*

import sage.ce.SageClient

class CeSmokeSuite extends ServerSuite("redis:8") {

  test("an end user connects and round-trips with native cats-effect") {
    withContainers { server =>
      val config = configOf(server)

      val program: IO[Unit] =
        SageClient.resource(config).use { client =>
          for {
            pong   <- client.ping()
            _      <- (1 to 50).toList.parTraverse_(i => client.set(s"key-$i", s"value-$i"))
            values <- (1 to 50).toList.parTraverse(i => client.get[String, String](s"key-$i"))
          } yield {
            assertEquals(pong, "PONG")
            assertEquals(values, (1 to 50).toList.map(i => Some(s"value-$i")))
          }
        }

      program.unsafeRunSync()
    }
  }
}

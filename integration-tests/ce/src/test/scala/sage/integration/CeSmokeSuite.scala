package sage.integration

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*

import sage.ce.*

class CeSmokeSuite extends ServerSuite(Images.redis) {

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

  test("scanAll streams every key as a native fs2 Stream") {
    withContainers { server =>
      val program: IO[Unit] =
        SageClient.resource(configOf(server)).use { client =>
          for {
            _    <- (1 to 50).toList.parTraverse_(i => client.set(s"scan-$i", "v"))
            keys <- client.scanAll[String](pattern = Some("scan-*"), count = Some(10L)).compile.toVector
          } yield assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)
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
            pairs <- client.hScanAll[String, String, String]("hscan", count = Some(10L)).compile.toVector
          } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)
        }

      program.unsafeRunSync()
    }
  }
}

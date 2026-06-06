package sage.integration

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import zio.*

import sage.client.SageConfig
import sage.zio.SageClient

class ZioSmokeSuite extends munit.FunSuite with TestContainerForAll {

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def("redis:8", exposedPorts = Seq(6379))

  test("an end user connects and round-trips with native ZIO") {
    withContainers { server =>
      val config = SageConfig(host = server.host, port = server.mappedPort(6379))

      val program: Task[Unit] =
        ZIO.acquireReleaseWith(SageClient.connect(config))(client => client.close.orDie) { client =>
          for {
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

package sage.integration

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import ox.{fork, supervised}

import sage.client.SageConfig
import sage.ox.SageClient

class OxSmokeSuite extends munit.FunSuite with TestContainerForAll {

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def("redis:8", exposedPorts = Seq(6379))

  test("an end user connects and round-trips with direct-style Ox") {
    withContainers { server =>
      val config = SageConfig(host = server.host, port = server.mappedPort(6379))
      supervised {
        val client = SageClient.connect(config)
        try {
          assertEquals(client.ping(), "PONG")
          val values = (1 to 50).toList
            .map(i =>
              fork {
                client.set(s"key-$i", s"value-$i")
                client.get[String, String](s"key-$i")
              }
            )
            .map(_.join())
          assertEquals(values, (1 to 50).toList.map(i => Some(s"value-$i")))
        } finally client.close
      }
    }
  }
}

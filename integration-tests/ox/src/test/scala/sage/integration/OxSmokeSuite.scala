package sage.integration

import ox.{fork, supervised}

import sage.ox.SageClient

class OxSmokeSuite extends ServerSuite("redis:8") {

  test("an end user connects and round-trips with direct-style Ox") {
    withContainers { server =>
      val config = configOf(server)
      supervised {
        val client = SageClient.scoped(config)
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
      }
    }
  }
}

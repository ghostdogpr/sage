package sage.integration

import ox.{fork, supervised}

import sage.ox.*

class OxSmokeSuite extends ServerSuite(Images.redis) {

  test("an end user connects and round-trips with direct-style Ox") {
    withContainers { server =>
      val config = configOf(server)
      supervised {
        val client = SageClient.scoped(config)
        assertEquals(client.ping(), "PONG")
        val values = (1 to 50).toList
          .map(i =>
            fork {
              val _ = client.set(s"key-$i", s"value-$i")
              client.get[String, String](s"key-$i")
            }
          )
          .map(_.join())
        assertEquals(values, (1 to 50).toList.map(i => Some(s"value-$i")))
      }
    }
  }

  test("scanAll streams every key as a native Ox Flow") {
    withContainers { server =>
      supervised {
        val client = SageClient.scoped(configOf(server))
        (1 to 50).foreach { i =>
          val _ = client.set(s"scan-$i", "v")
        }
        val keys   = client.scanAll[String](pattern = Some("scan-*"), count = Some(10L)).runToList()
        assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)
      }
    }
  }

  test("hScanAll streams every field/value pair as a native Ox Flow") {
    withContainers { server =>
      supervised {
        val client = SageClient.scoped(configOf(server))
        (1 to 50).foreach { i =>
          val _ = client.hSet("hscan", (s"f$i", s"v$i"))
        }
        val pairs  = client.hScanAll[String, String, String]("hscan", count = Some(10L)).runToList()
        assertEquals(pairs.toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)
      }
    }
  }

  test("sScanAll streams every member as a native Ox Flow") {
    withContainers { server =>
      supervised {
        val client  = SageClient.scoped(configOf(server))
        (1 to 50).foreach { i =>
          val _ = client.sAdd("sscan", s"m$i")
        }
        val members = client.sScanAll[String, String]("sscan", count = Some(10L)).runToList()
        assertEquals(members.toSet, (1 to 50).map(i => s"m$i").toSet)
      }
    }
  }

  test("zScanAll streams every member/score pair as a native Ox Flow") {
    withContainers { server =>
      supervised {
        val client = SageClient.scoped(configOf(server))
        (1 to 50).foreach { i =>
          val _ = client.zAdd("zscan")((s"m$i", i.toDouble))
        }
        val pairs  = client.zScanAll[String, String]("zscan", count = Some(10L)).runToList()
        assertEquals(pairs.toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)
      }
    }
  }
}

package sage.integration

import scala.concurrent.duration.*

import ox.{fork, supervised}

import sage.*
import sage.backend.*

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
              client.get[String](s"key-$i")
            }
          )
          .map(_.join())
        assertEquals(values, (1 to 50).toList.map(i => Some(s"value-$i")))
      }
    }
  }

  test("a pipeline returns a typed tuple natively, surfacing failures per position") {
    withContainers { server =>
      supervised {
        val client  = SageClient.scoped(configOf(server))
        val _       = client.set("pipe:a", "x")
        val _       = client.set("pipe:n", 10)
        val out     = client.pipeline((Commands.get[String, String]("pipe:a"), Commands.incrBy[String]("pipe:n", 5)))
        assertEquals(out, (Some("x"), 15L))
        val _       = client.set("pipe:str", "hello")
        val attempt = client.pipelineAttempt((Commands.get[String, String]("pipe:str"), Commands.incr[String]("pipe:str")))
        assert(attempt._1 == Right(Some("hello")), attempt._1)
        assert(attempt._2.isLeft, attempt._2)
      }
    }
  }

  test("a transaction commits atomically with direct-style Ox, guarded by WATCH") {
    withContainers { server =>
      supervised {
        val client = SageClient.scoped(configOf(server))
        val _      = client.set("tx:n", 1)
        val out    = client.transaction { tx =>
          val _ = tx.watch("tx:n")
          val _ = tx.get[Int]("tx:n")
          tx.exec((Commands.incr[String]("tx:n"), Commands.incrBy[String]("tx:n", 4)))
        }
        assertEquals(out, Some((2L, 6L)))
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
        val keys   = client.scanAll(pattern = Some("scan-*"), count = Some(10L)).runToList()
        assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)
      }
    }
  }

  test("subscribe delivers published messages as a native Ox Flow") {
    withContainers { server =>
      supervised {
        val client   = SageClient.scoped(configOf(server))
        val stream   = client.subscribeScoped[String]("smoke")
        (1 to 3).foreach { i =>
          val _ = client.publish("smoke", s"m$i")
        }
        val messages = stream.take(3).runToList()
        assertEquals(messages.map(_.channel).toSet, Set("smoke"))
        assertEquals(messages.map(_.payload), List("m1", "m2", "m3"))
      }
    }
  }

  test("a scoped subscription survives a flow run ending — take(1) must not unsubscribe, only scope close does") {
    withContainers { server =>
      supervised {
        val client = SageClient.scoped(configOf(server))
        val stream = client.subscribeScoped[String]("scoped-live")
        val _      = client.publish("scoped-live", "a")
        val first  = stream.take(1).runToList()
        val _      = client.publish("scoped-live", "b")
        val second = stream.take(1).runToList()
        assertEquals(first.map(_.payload), List("a"))
        assertEquals(second.map(_.payload), List("b"))
      }
    }
  }

  test("a plain subscribe Flow resubscribes on every run instead of yielding an empty stream on re-run") {
    withContainers { server =>
      supervised {
        val client    = SageClient.scoped(configOf(server))
        val stream    = client.subscribe[String]("rerun")
        // publish continuously so each run's subscription receives one; the second run must resubscribe, not complete empty
        val running   = new java.util.concurrent.atomic.AtomicBoolean(true)
        val publisher = fork {
          while (running.get()) {
            val _ = client.publish("rerun", "tick")
            Thread.sleep(50)
          }
        }
        try {
          val first  = stream.take(1).runToList()
          val second = stream.take(1).runToList()
          assertEquals(first.map(_.payload), List("tick"))
          assertEquals(second.map(_.payload), List("tick"))
        } finally {
          running.set(false)
          publisher.join()
        }
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
        val pairs  = client.hScanAll[String, String]("hscan", count = Some(10L)).runToList()
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
        val members = client.sScanAll[String]("sscan", count = Some(10L)).runToList()
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
        val pairs  = client.zScanAll[String]("zscan", count = Some(10L)).runToList()
        assertEquals(pairs.toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)
      }
    }
  }

  test("client.rateLimiter admits up to capacity then denies") {
    withContainers { server =>
      supervised {
        val client = SageClient.scoped(configOf(server))
        val rl     = client.rateLimiter[String](RateLimit(capacity = 2, refillTokens = 1, refillPeriod = 1.hour))
        val first  = rl.tryAcquire("smoke")
        val second = rl.tryAcquire("smoke")
        val denied = rl.tryAcquire("smoke")
        assert(first.isAllowed && second.isAllowed, "the first two are admitted")
        assert(!denied.isAllowed, "the third is denied once the bucket empties")
      }
    }
  }
}

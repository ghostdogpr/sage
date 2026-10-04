package sage.integration

import kyo.compat.*
import munit.{Location, TestOptions}
import ox.{fork, supervised, Ox}

import sage.*
import sage.backend.*

class OxSmokeSuite extends SmokeSuite {

  private def nativeTest(options: TestOptions)(body: Ox ?=> SageClient => Unit)(using Location): Unit =
    test(options)(withContainers(server => supervised(body(SageClient.scoped(configOf(server))))))

  private val lift: [A] => (Ox ?=> A) => CIO[A] = [A] => (run: Ox ?=> A) => CIO.lift(run)

  nativeTest("a distributed lock scopes native effects and skips contended bodies")(lockScopesNativeEffects(_)(lift).lower)

  nativeTest("an end user connects and round-trips with direct-style Ox") { client =>
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

  nativeTest("a transaction commits atomically with direct-style Ox, guarded by WATCH") { client =>
    val _   = client.set("tx:n", 1)
    val out = client.transaction { tx =>
      tx.watch("tx:n")
      val _ = tx.get[Int]("tx:n")
      tx.exec((Commands.incr[String]("tx:n"), Commands.incrBy[String]("tx:n", 4)))
    }
    assertEquals(out, Some((2L, 6L)))
  }

  nativeTest("scanAll streams every key as a native Ox Flow") { client =>
    scanAllFindsEveryKey(client)(lift)(client.scanAll(pattern = Some("scan-*"), count = Some(10L)).runToList()).lower
  }

  nativeTest("subscribe delivers published messages as a native Ox Flow") { client =>
    val stream   = client.subscribeScoped[String]("smoke")
    (1 to 3).foreach(i => client.publish("smoke", s"m$i"))
    val messages = stream.take(3).runToList()
    assertEquals(messages, List("m1", "m2", "m3").map(Message("smoke", _)))
  }

  nativeTest("a scoped subscription survives a flow run ending — take(1) must not unsubscribe, only scope close does") { client =>
    val stream = client.subscribeScoped[String]("scoped-live")
    val _      = client.publish("scoped-live", "a")
    val first  = stream.take(1).runToList()
    val _      = client.publish("scoped-live", "b")
    val second = stream.take(1).runToList()
    assertEquals(first.map(_.payload), List("a"))
    assertEquals(second.map(_.payload), List("b"))
  }

  nativeTest("a plain subscribe Flow resubscribes on every run instead of yielding an empty stream on re-run") { client =>
    val stream                              = client.subscribe[String]("rerun")
    def awaitSubscribers(count: Long): Unit =
      Eventually(100)(CIO.deferLift(client.pubsubNumSub("rerun").getOrElse("rerun", 0L)).is(count)).lower
    // each run subscribes on its own, so publish once that run's subscription is on the server
    def runOnce(): List[String]             = {
      val run = fork(stream.take(1).runToList())
      awaitSubscribers(1)
      val _   = client.publish("rerun", "tick")
      run.join().map(_.payload)
    }
    val first                               = runOnce()
    awaitSubscribers(0)
    assertEquals(first, List("tick"))
    assertEquals(runOnce(), List("tick"))
  }
}

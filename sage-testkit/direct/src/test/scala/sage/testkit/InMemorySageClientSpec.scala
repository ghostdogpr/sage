package sage.testkit

import scala.concurrent.duration.*

import sage.*
import sage.SageException.{NotConnected, ServerError, TransactionDiscarded}
import sage.commands.{Keys, Strings}

class InMemorySageClientSpec extends InMemorySuite {

  clocked.test("keys expire according to the test clock") { (client, clock) =>
    client.set("session", "token", expiry = SetExpiry.In(10.seconds)): Unit
    assertEquals(client.ttl("session"), Ttl.Expires(10.seconds))
    clock.advance(4.seconds)
    assertEquals(client.pTtl("session"), Ttl.Expires(6000.millis))
    clock.advance(6.seconds)
    assertEquals(client.get[String]("session"), None)
    assertEquals(client.ttl("session"), Ttl.NoKey)
  }

  clients.test("WATCH sees a missing key being created and ignores writes that change nothing, as on a server") { client =>
    val watchedMissing = client.transaction { tx =>
      tx.watch("fresh")
      client.set("fresh", "x"): Unit
      tx.exec(Seq(Strings.getDel[String, String]("fresh")))
    }
    assertEquals(watchedMissing, None)

    client.zAdd("z")(("a", 1.0)): Unit
    client.rPush("l", "a"): Unit
    val id        = client.xAdd("s")(("f", "v"))
    client.xGroupCreate("s", "g", GroupStartId.At(StreamId.Zero))
    val unchanged = client.transaction { tx =>
      tx.watch("z", "l", "s")
      assertEquals(client.zAdd("z", ZAddCondition.IfNotExists)(("a", 2.0)), 0L)
      assertEquals(client.zIncrBy("z", "a", 0.0), 1.0)
      assertEquals(client.lPopCount[String]("l", 0L), Vector.empty)
      // a server's WATCH does not see consumer-group changes
      client.xReadGroup[String, String]("g", "c")(("s", GroupReadId.New))(): Unit
      assertEquals(client.xAck("s", "g")(id), 1L)
      assert(client.xGroupCreateConsumer("s", "g", "c2"))
      tx.exec(Seq(Strings.set("other", "x")))
    }
    assertEquals(unchanged.map(_.length), Some(1))
    val added     = client.transaction { tx =>
      tx.watch("s")
      client.xAdd("s")(("f", "w")): Unit
      tx.exec(Seq(Strings.set("other", "x")))
    }
    assertEquals(added, None)
    assert(client.copy("s", "copy"))
    val copied    = client.transaction { tx =>
      tx.watch("copy")
      assert(client.copy("s", "copy", replace = true))
      tx.exec(Seq(Strings.set("other", "x")))
    }
    assertEquals(copied, None)
    val created   = client.transaction { tx =>
      tx.watch("mk")
      client.xGroupCreate("mk", "g", mkStream = true)
      tx.exec(Seq(Strings.set("other", "x")))
    }
    assertEquals(created, None)
  }

  clocked.test("EXEC aborts when a watched key expires or GETEX sets its expiry again, and commits when it had expired before") { (client, clock) =>
    client.set("k", "v", expiry = SetExpiry.In(1.second)): Unit
    val expiring = client.transaction { tx =>
      tx.watch("k")
      clock.advance(2.seconds)
      tx.exec(Seq(Strings.set("other", "x")))
    }
    assertEquals(expiring, None)
    client.set("gone", "v", expiry = SetExpiry.In(1.second)): Unit
    clock.advance(2.seconds)
    val expired  = client.transaction { tx =>
      tx.watch("gone")
      client.get[String]("gone"): Unit
      tx.exec(Seq(Strings.set("other", "y")))
    }
    assertEquals(expired.map(_.length), Some(1))
    client.set("same", "v", expiry = SetExpiry.In(1.second)): Unit
    val renewed  = client.transaction { tx =>
      tx.watch("same")
      client.getEx[String]("same", GetExpiry.In(1.second)): Unit
      tx.exec(Seq(Strings.set("other", "z")))
    }
    assertEquals(renewed, None)
  }

  clients.test("a transaction with an unsupported command is discarded before anything runs") { client =>
    intercept[TransactionDiscarded] {
      client.transaction(tx => tx.exec((Strings.set("k", "v"), Keys.objectEncoding("h"))))
    }: Unit
    assertEquals(client.get[String]("k"), None)
  }

  clients.test("unsupported commands fail with an ERR server error that names them") { client =>
    val error = intercept[ServerError](client.setBit("b", 0L, true))
    assertEquals(error.code, "ERR")
    assert(error.detail.contains("SETBIT"))
  }

  test("a closed client refuses commands") {
    val client = InMemorySageClient()
    client.close
    intercept[NotConnected](client.get[String]("k")): Unit
  }

  FunFixture.map2(clients, clients).test("each client has its own keyspace") { (first, second) =>
    first.set("k", "v"): Unit
    assertEquals(second.get[String]("k"), None)
  }
}

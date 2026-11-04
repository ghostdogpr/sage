package sage.testkit

import scala.concurrent.duration.*

import sage.*
import sage.SageException.{NotConnected, ServerError, TransactionDiscarded}
import sage.backend.*
import sage.commands.{Keys, Strings}

class InMemorySageClientSpec extends munit.FunSuite {

  private def withClient[A](body: SageClient => A): A = {
    val client = InMemorySageClient()
    try body(client)
    finally client.close
  }

  private def withClockedClient[A, C <: TestClock](clock: C)(body: (SageClient, C) => A): A = {
    val client = InMemorySageClient(clock)
    try body(client, clock)
    finally client.close
  }

  test("SET and GET round-trip values through their codecs") {
    withClient { client =>
      assert(client.set("name", "sage"))
      assertEquals(client.get[String]("name"), Some("sage"))
      assertEquals(client.get[String]("missing"), None)
      assert(client.set("count", 42L))
      assertEquals(client.get[Long]("count"), Some(42L))
    }
  }

  test("SET honors NX, XX and GET") {
    withClient { client =>
      assert(!client.set("k", "a", condition = SetCondition.IfExists))
      assert(client.set("k", "a", condition = SetCondition.IfNotExists))
      assert(!client.set("k", "b", condition = SetCondition.IfNotExists))
      assertEquals(client.setGet[String]("k", "c"), Some("a"))
      assertEquals(client.get[String]("k"), Some("c"))
    }
  }

  test("counters follow Redis integer semantics") {
    withClient { client =>
      assertEquals(client.incr("n"), 1L)
      assertEquals(client.incrBy("n", 10L), 11L)
      assertEquals(client.decrBy("n", 20L), -9L)
      assertEquals(client.incrByFloat("f", 1.5), 1.5)
      assertEquals(client.incrByFloat("f", 1.0), 2.5)
      client.set("text", "abc"): Unit
      val error = intercept[ServerError](client.incr("text"))
      assertEquals(error.code, "ERR")
      assertEquals(error.detail, "value is not an integer or out of range")
      client.set("max", Long.MaxValue): Unit
      intercept[ServerError](client.incr("max")): Unit
    }
  }

  test("string helpers edit stored bytes") {
    withClient { client =>
      assertEquals(client.append("s", "Hello"), 5L)
      assertEquals(client.append("s", " World"), 11L)
      assertEquals(client.strLen("s"), 11L)
      assertEquals(client.getRange[String]("s", 0L, 4L), "Hello")
      assertEquals(client.getRange[String]("s", -5L, -1L), "World")
      assertEquals(client.setRange("s", 6L, "Redis"), 11L)
      assertEquals(client.get[String]("s"), Some("Hello Redis"))
      assertEquals(client.setRange("pad", 3L, "x"), 4L)
      assertEquals(client.get[Array[Byte]]("pad").map(_.toVector), Some(Vector[Byte](0, 0, 0, 'x'.toByte)))
      assertEquals(client.getDel[String]("s"), Some("Hello Redis"))
      assertEquals(client.exists("s"), 0L)
    }
  }

  test("MSET, MSETNX and MGET work on several keys") {
    withClient { client =>
      client.mSet("a" -> "1", "b" -> "2")
      assertEquals(client.mGet[String]("a", "b", "c"), Vector(Some("1"), Some("2"), None))
      assert(!client.mSetNx("c" -> "3", "a" -> "x"))
      assertEquals(client.get[String]("c"), None)
      assert(client.mSetNx("c" -> "3", "d" -> "4"))
      assertEquals(client.del("a", "b", "missing"), 2L)
    }
  }

  test("keys expire according to the test clock") {
    withClockedClient(TestClock.manual()) { (client, clock) =>
      client.set("session", "token", expiry = SetExpiry.In(10.seconds)): Unit
      assertEquals(client.ttl("session"), Ttl.Expires(10.seconds))
      clock.advance(4.seconds)
      assertEquals(client.pTtl("session"), Ttl.Expires(6000.millis))
      clock.advance(6.seconds)
      assertEquals(client.get[String]("session"), None)
      assertEquals(client.ttl("session"), Ttl.NoKey)
    }
  }

  test("EXPIRE conditions and PERSIST match Redis") {
    withClockedClient(TestClock.manual()) { (client, clock) =>
      client.set("k", "v"): Unit
      assertEquals(client.ttl("k"), Ttl.NoExpiry)
      assert(!client.expire("k", 10.seconds, ExpireCondition.IfHasExpiry))
      assert(!client.expire("k", 10.seconds, ExpireCondition.IfGreater))
      assert(client.expire("k", 10.seconds, ExpireCondition.IfNoExpiry))
      assert(!client.expire("k", 5.seconds, ExpireCondition.IfGreater))
      assert(client.expire("k", 5.seconds, ExpireCondition.IfLess))
      assertEquals(client.pExpireTime("k"), ExpiryTime.At(java.time.Instant.ofEpochMilli(clock.nowMillis + 5000L)))
      assert(client.persist("k"))
      assert(!client.persist("k"))
      assert(client.expire("k", Duration.Zero))
      assertEquals(client.exists("k"), 0L)
      assert(!client.expire("missing", 1.second))
    }
  }

  test("SET with KEEPTTL and GETEX change the expiry") {
    withClockedClient(TestClock.manual()) { (client, _) =>
      client.set("k", "a", expiry = SetExpiry.In(1.minute)): Unit
      client.set("k", "b", expiry = SetExpiry.KeepTtl): Unit
      assertEquals(client.ttl("k"), Ttl.Expires(60.seconds))
      client.set("k", "c"): Unit
      assertEquals(client.ttl("k"), Ttl.NoExpiry)
      assertEquals(client.getEx[String]("k", GetExpiry.In(30.seconds)), Some("c"))
      assertEquals(client.ttl("k"), Ttl.Expires(30.seconds))
      assertEquals(client.getEx[String]("k", GetExpiry.Persist), Some("c"))
      assertEquals(client.ttl("k"), Ttl.NoExpiry)
    }
  }

  test("RENAME, COPY and TYPE handle keys and their expiry") {
    withClockedClient(TestClock.manual()) { (client, _) =>
      client.set("src", "v", expiry = SetExpiry.In(1.minute)): Unit
      assert(client.copy("src", "dst"))
      assert(!client.copy("src", "dst"))
      assertEquals(client.ttl("dst"), Ttl.Expires(60.seconds))
      client.rename("src", "moved")
      assertEquals(client.get[String]("moved"), Some("v"))
      assert(!client.renameNx("moved", "dst"))
      assertEquals(intercept[ServerError](client.rename("missing", "x")).detail, "no such key")
      assertEquals(client.typeOf("moved"), Some(RedisType.String))
      assertEquals(client.typeOf("missing"), None)
    }
  }

  test("KEYS and SCAN find keys by glob pattern") {
    withClient { client =>
      (1 to 25).foreach(i => client.set(s"user:$i", "x"): Unit)
      client.set("other", "x"): Unit
      assertEquals(client.keys("user:1?").toSet, (10 to 19).map(i => s"user:$i").toSet)
      assertEquals(client.keys("user:[2-3]").toSet, Set("user:2", "user:3"))
      assertEquals(client.scanAll(pattern = Some("user:*"), count = Some(4L)).toSet, (1 to 25).map(i => s"user:$i").toSet)
      assertEquals(client.scanAll().size, 26)
    }
  }

  test("pipelines return one result per command and keep per-command errors") {
    withClient { client =>
      assertEquals(client.pipeline((Strings.set("k", "v"), Strings.get[String, String]("k"))), (true, Some("v")))
      val (first, second) = client.pipelineAttempt((Strings.incr("k"), Strings.strLen("k")))
      assert(first.isLeft)
      assertEquals(second, Right(1L))
    }
  }

  test("transactions commit atomically and abort when a watched key changes") {
    withClient { client =>
      client.set("balance", 10L): Unit
      val committed = client.transaction { tx =>
        tx.watch("balance")
        val current = tx.get[Long]("balance").getOrElse(0L)
        tx.exec((Strings.set("balance", current + 5L), Strings.get[String, Long]("balance")))
      }
      assertEquals(committed, Some((true, Some(15L))))

      val aborted = client.transaction { tx =>
        tx.watch("balance")
        client.incr("balance"): Unit
        tx.exec(Seq(Strings.set("balance", 0L)))
      }
      assertEquals(aborted, None)
      assertEquals(client.get[Long]("balance"), Some(16L))

      val watchedMissing = client.transaction { tx =>
        tx.watch("fresh")
        client.set("fresh", "x"): Unit
        tx.exec(Seq(Strings.getDel[String, String]("fresh")))
      }
      assertEquals(watchedMissing, None)
    }
  }

  test("a transaction with an unsupported command is discarded before anything runs") {
    withClient { client =>
      intercept[TransactionDiscarded] {
        client.transaction(tx => tx.exec((Strings.set("k", "v"), Keys.objectEncoding("h"))))
      }: Unit
      assertEquals(client.get[String]("k"), None)
    }
  }

  test("unsupported commands fail with an ERR server error") {
    withClient { client =>
      val error = intercept[ServerError](client.objectEncoding("h"))
      assertEquals(error.code, "ERR")
      assert(error.detail.contains("OBJECT"))
    }
  }

  test("PING works and a closed client refuses commands") {
    val client = InMemorySageClient()
    assertEquals(client.ping(), "PONG")
    assertEquals(client.ping(Some("hi")), "hi")
    client.close
    intercept[NotConnected](client.get[String]("k")): Unit
  }

  test("each client has its own keyspace") {
    withClient { first =>
      withClient { second =>
        first.set("k", "v"): Unit
        assertEquals(second.get[String]("k"), None)
      }
    }
  }
}

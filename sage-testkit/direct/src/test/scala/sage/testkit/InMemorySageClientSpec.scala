package sage.testkit

import scala.concurrent.duration.*

import sage.*
import sage.SageException.{NotConnected, ServerError, TransactionDiscarded}
import sage.commands.{Keys, Strings}
import sage.ratelimit.{Decision, RateLimit}
import sage.ratelimit.Decision.{Allowed, Denied}

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

  // the decisions of RateLimiterSuite's injected-time tests, which the in-memory client cannot run because it has no EVAL
  clocked.test("rate limiters refill, handle a backward clock and keep the balance across policies as the server's script does") { (client, clock) =>
    val start                                            = clock.nowMillis
    def at(offsetMillis: Long): Unit                     = clock.set(java.time.Instant.ofEpochMilli(start + offsetMillis))
    val original                                         = client.rateLimiter[String](RateLimit(capacity = 10, refillTokens = 10, refillPeriod = 1.second))
    val tightened                                        = client.rateLimiter[String](RateLimit(capacity = 1, refillTokens = 1, refillPeriod = 1.second))
    def check(decisions: (Decision, Decision)*): Unit    = decisions.foreach((obtained, expected) => assertEquals(obtained, expected))
    check(
      original.tryAcquire("refill", 10)    -> Allowed(0, 1.second),
      original.tryAcquire("refill")        -> Denied(0, 100.millis),
      original.tryAcquire("clock", 10)     -> Allowed(0, 1.second),
      original.tryAcquire("policy")        -> Allowed(9, 100.millis),
      tightened.tryAcquire("policy")       -> Allowed(0, 1.second),
      original.tryAcquire("policy")        -> Denied(0, 100.millis),
      tightened.tryAcquire("policy")       -> Denied(0, 1.second)
    )
    at(-500)
    check(original.tryAcquire("clock")     -> Denied(0, 600.millis), original.tryAcquire("policy") -> Denied(0, 600.millis))
    at(100)
    check(original.tryAcquire("clock")     -> Allowed(0, 1.second), original.tryAcquire("clock")   -> Denied(0, 100.millis))
    at(300)
    check(original.tryAcquire("refill", 3) -> Allowed(0, 1.second), original.tryAcquire("refill")  -> Denied(0, 100.millis))
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
    val unchanged = client.transaction { tx =>
      tx.watch("z", "l")
      assertEquals(client.zAdd("z", ZAddCondition.IfNotExists)(("a", 2.0)), 0L)
      assertEquals(client.zIncrBy("z", "a", 0.0), 1.0)
      assertEquals(client.lPopCount[String]("l", 0L), Vector.empty)
      tx.exec(Seq(Strings.set("other", "x")))
    }
    assertEquals(unchanged.map(_.length), Some(1))
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

  clients.test("unsupported commands and options fail with an ERR server error that names them") { client =>
    client.set("k", "v"): Unit
    val command = intercept[ServerError](client.copy("k", "copy"))
    assertEquals((command.code, command.detail.contains("COPY")), ("ERR", true))
    val option  = intercept[ServerError](client.expire("k", 10.seconds, ExpireCondition.IfNoExpiry))
    assertEquals((option.code, option.detail.contains("NX")), ("ERR", true))
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

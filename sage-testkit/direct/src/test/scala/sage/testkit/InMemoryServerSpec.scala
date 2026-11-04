package sage.testkit

import scala.concurrent.duration.*

import sage.*
import sage.backend.*

class InMemoryServerSpec extends munit.FunSuite {

  test("clients of one server share the keyspace and pub/sub messages") {
    val server     = InMemoryServer()
    val publisher  = InMemorySageClient(server)
    val subscriber = InMemorySageClient(server)
    val messages   = subscriber.subscribe[String]("news")
    publisher.set("shared", "yes"): Unit
    assertEquals(subscriber.get[String]("shared"), Some("yes"))
    assertEquals(publisher.publish("news", "hello"), 1L)
    assertEquals(publisher.publish("elsewhere", "ignored"), 0L)
    assertEquals(messages.next(), Message("news", "hello"))
    messages.close()
    assertEquals(publisher.publish("news", "gone"), 0L)
  }

  test("pattern subscriptions name the pattern and the channel") {
    val client = InMemorySageClient()
    val events = client.pSubscribe[String]("orders.*")
    assertEquals(client.publish("orders.created", "42"), 1L)
    assertEquals(events.next(), PatternMessage("orders.*", "orders.created", "42"))
    events.close()
  }

  test("a blocking pop waits for another client's push") {
    val server   = InMemoryServer()
    val consumer = InMemorySageClient(server)
    val producer = InMemorySageClient(server)
    val popped   = new java.util.concurrent.CompletableFuture[Option[(String, String)]]()
    Thread.startVirtualThread(() => popped.complete(consumer.blPop[String]("jobs")(BlockTimeout.After(5.seconds))): Unit): Unit
    Thread.sleep(50)
    producer.rPush("jobs", "job-1"): Unit
    assertEquals(popped.get(5, java.util.concurrent.TimeUnit.SECONDS), Some(("jobs", "job-1")))
  }

  test("distributed locks exclude other clients of the same server") {
    val server = InMemoryServer()
    val first  = InMemorySageClient(server)
    val second = InMemorySageClient(server)
    val held   = first.lock[String]().tryWithLock("job") {
      assertEquals(second.lock[String]().tryWithLock("job")("second"), None)
      "first"
    }
    assertEquals(held, Some("first"))
    assertEquals(second.lock[String]().tryWithLock("job")("second"), Some("second"))
  }

  test("rate limiters allow up to capacity and then deny") {
    val client  = InMemorySageClient()
    val limiter = client.rateLimiter[String](RateLimit(capacity = 2, refillTokens = 1, refillPeriod = 1.minute))
    assert(limiter.tryAcquire("user").isAllowed)
    assert(limiter.tryAcquire("user").isAllowed)
    assert(!limiter.tryAcquire("user").isAllowed)
    limiter.reset("user")
    assert(limiter.tryAcquire("user").isAllowed)
  }
}

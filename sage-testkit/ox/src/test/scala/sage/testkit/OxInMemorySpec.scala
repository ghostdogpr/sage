package sage.testkit

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.duration.*

import ox.{forkCancellable, never, supervised}

import sage.*
import sage.backend.*

class OxInMemorySpec extends munit.FunSuite {

  test("the in-memory client runs direct-style Ox code") {
    supervised {
      val clock   = TestClock()
      val client  = InMemorySageClient(InMemoryServer(clock))
      val _       = client.set("session", "token", expiry = SetExpiry.In(10.seconds))
      assertEquals(client.get[String]("session"), Some("token"))
      clock.advance(10.seconds)
      assertEquals(client.get[String]("session"), None)
      val counted = client.transaction(tx => tx.exec((Commands.incr("visits"), Commands.incrBy("visits", 4L))))
      assertEquals(counted, Some((1L, 5L)))
      assertEquals(client.scanAll().runToList(), List("visits"))
      client.close
    }
  }

  test("a lock excludes another client of the same server until its holder is cancelled") {
    supervised {
      val server  = InMemoryServer()
      val first   = InMemorySageClient(server).lock[String]()
      val second  = InMemorySageClient(server).lock[String]()
      val started = new CountDownLatch(1)
      val holder  = forkCancellable(first.withLock("job", 5.seconds) { started.countDown(); never })
      assert(started.await(5, TimeUnit.SECONDS))
      assertEquals(second.tryWithLock("job")("ran"), None)
      intercept[SageException.TimedOut](second.withLock("job", 50.millis)("ran"))
      holder.cancel(): Unit
      assertEquals(second.tryWithLock("job")("ran"), Some("ran"))
    }
  }
}

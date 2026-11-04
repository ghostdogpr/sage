package sage.testkit

import scala.concurrent.duration.*

import ox.supervised

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
}

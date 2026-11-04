package sage.testkit

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global

import sage.*
import sage.backend.*

class CeInMemorySpec extends munit.FunSuite {

  test("expiry follows the virtual time of TestControl") {
    val client  = InMemorySageClient()
    val program = for {
      _      <- client.set("session", "token", expiry = SetExpiry.In(1.hour))
      _      <- IO.sleep(59.minutes)
      before <- client.get[String]("session")
      _      <- IO.sleep(1.minute)
      after  <- client.get[String]("session")
    } yield (before, after)
    // the hour passes instantly because TestControl advances its virtual clock whenever every fiber is sleeping
    assertEquals(TestControl.executeEmbed(program).unsafeRunSync(), (Some("token"), None))
  }

  test("a subscription read that times out leaves the next message for the following read") {
    val client  = InMemorySageClient()
    val program = client.subscribeResource[String]("news").use { messages =>
      for {
        missed   <- messages.head.compile.last.timeout(1.second).attempt
        _        <- client.publish("news", "hello")
        received <- messages.head.compile.last
      } yield (missed.isLeft, received)
    }
    assertEquals(TestControl.executeEmbed(program).unsafeRunSync(), (true, Some(Message("news", "hello"))))
  }

  test("transactions and scanAll run as Cats Effect programs") {
    val client  = InMemorySageClient()
    val program = for {
      counted <- client.transaction(tx => tx.exec((Commands.incr("visits"), Commands.incrBy("visits", 4L))))
      keys    <- client.scanAll().compile.toList
      _       <- client.close
    } yield (counted, keys)
    assertEquals(program.unsafeRunSync(), (Some((1L, 5L)), List("visits")))
  }
}

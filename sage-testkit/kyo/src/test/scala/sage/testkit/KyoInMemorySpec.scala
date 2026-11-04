package sage.testkit

import scala.concurrent.duration.*

import kyo.{<, Abort, AllowUnsafe, Async, Clock, KyoApp}

import sage.*
import sage.backend.*

class KyoInMemorySpec extends munit.FunSuite {

  private def run[A](program: A < (Abort[Throwable] & Async)): A = {
    import AllowUnsafe.embrace.danger
    KyoApp.Unsafe.runAndBlock(kyo.Duration.Infinity)(program).getOrThrow
  }

  test("expiry follows Kyo's controlled clock") {
    val client = InMemorySageClient()
    val result = run(Clock.withTimeControl { control =>
      for {
        _      <- client.set("session", "token", expiry = SetExpiry.In(10.seconds))
        _      <- control.advance(kyo.Duration.fromJava(java.time.Duration.ofSeconds(9)))
        before <- client.get[String]("session")
        _      <- control.advance(kyo.Duration.fromJava(java.time.Duration.ofSeconds(1)))
        after  <- client.get[String]("session")
      } yield (before, after)
    })
    assertEquals(result, (Some("token"), None))
  }

  test("transactions and scanAll run as Kyo programs") {
    val client = InMemorySageClient()
    val result = run(for {
      counted <- client.transaction(tx => tx.exec((Commands.incr("visits"), Commands.incrBy("visits", 4L))))
      keys    <- client.scanAll().run
      _       <- client.close
    } yield (counted, keys.toList))
    assertEquals(result, (Option((1L, 5L)), List("visits")))
  }
}

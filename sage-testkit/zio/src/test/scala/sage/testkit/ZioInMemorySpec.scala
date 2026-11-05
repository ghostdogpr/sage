package sage.testkit

import scala.concurrent.duration.*

import zio.{Promise, ZIO}
import zio.test.{TestClock as ZioTestClock, *}

import sage.*
import sage.backend.*

object ZioInMemorySpec extends ZIOSpecDefault {

  def spec = suite("the in-memory client with ZIO")(
    test("expiry follows zio-test's TestClock") {
      val client = InMemorySageClient()
      for {
        _      <- client.set("session", "token", expiry = SetExpiry.In(10.seconds))
        ttl    <- client.ttl("session")
        _      <- ZioTestClock.adjust(zio.Duration.fromSeconds(9))
        before <- client.get[String]("session")
        _      <- ZioTestClock.adjust(zio.Duration.fromSeconds(1))
        after  <- client.get[String]("session")
        now    <- client.time
      } yield assertTrue(
        ttl == Ttl.Expires(10.seconds),
        before.contains("token"),
        after.isEmpty,
        now == java.time.Instant.ofEpochSecond(10)
      )
    },
    test("subscribers receive messages from another client of the same server as a ZStream") {
      val server     = InMemoryServer()
      val publisher  = InMemorySageClient(server)
      val subscriber = InMemorySageClient(server)
      ZIO.scoped {
        for {
          stream    <- subscriber.subscribeScoped[String]("news")
          receivers <- publisher.publish("news", "hello")
          _         <- publisher.publish("news", "world")
          messages  <- stream.take(2).runCollect
        } yield assertTrue(receivers == 1L, messages.toList == List(Message("news", "hello"), Message("news", "world")))
      }
    } @@ TestAspect.timeout(zio.Duration.fromSeconds(5)),
    test("a lock excludes another client of the same server until its holder is interrupted") {
      val server = InMemoryServer()
      val first  = InMemorySageClient(server).lock[String]()
      val second = InMemorySageClient(server).lock[String]()
      for {
        started <- Promise.make[Nothing, Unit]
        holder  <- first.withLock("job", 5.seconds)(started.succeed(()) *> ZIO.never).fork
        _       <- started.await
        busy    <- second.tryWithLock("job")(ZIO.unit)
        _       <- holder.interrupt
        after   <- second.tryWithLock("job")(ZIO.succeed("ran"))
      } yield assertTrue(busy.isEmpty, after.contains("ran"))
    } @@ TestAspect.timeout(zio.Duration.fromSeconds(5)),
    test("transactions and scanAll run as ZIO effects") {
      val client = InMemorySageClient()
      for {
        counted <- client.transaction(tx => tx.exec((Commands.incr("visits"), Commands.incrBy("visits", 4L))))
        keys    <- client.scanAll().runCollect
        _       <- client.close
      } yield assertTrue(counted.contains((1L, 5L)), keys.toList == List("visits"))
    }
  )
}

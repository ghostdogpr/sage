package sage.testkit

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.scaladsl.Sink

import sage.*
import sage.backend.*

class PekkoInMemorySpec extends munit.FunSuite {

  private given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "in-memory-spec")
  private given ExecutionContext             = system.executionContext

  override def afterAll(): Unit = system.terminate()

  test("the in-memory client runs Future-based Pekko code") {
    val clock                                                                               = TestClock()
    val client                                                                              = InMemorySageClient(InMemoryServer(clock))
    val result: Future[(Option[String], Option[String], Option[(Long, Long)], Seq[String])] = for {
      _       <- client.set("session", "token", expiry = SetExpiry.In(10.seconds))
      before  <- client.get[String]("session")
      _        = clock.advance(10.seconds)
      after   <- client.get[String]("session")
      counted <- client.transaction(tx => tx.exec((Commands.incr("visits"), Commands.incrBy("visits", 4L))))
      keys    <- client.scanAll().runWith(Sink.seq)
      _       <- client.close
    } yield (before, after, counted, keys)
    result.map(assertEquals(_, (Some("token"), None, Some((1L, 5L)), Seq("visits"))))
  }
}

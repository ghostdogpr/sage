package sage.integration

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*

import kyo.compat.*
import munit.{Location, TestOptions}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import sage.*
import sage.backend.*

class PekkoSmokeSuite extends SmokeSuite {

  private def nativeTest(options: TestOptions)(body: ActorSystem[Nothing] ?=> SageClient => Future[Unit])(using Location): Unit =
    test(options)(withContainers { server =>
      given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "pekko-smoke")
      try Await.result(SageClient.use(configOf(server))(body)(using system.executionContext), 30.seconds)
      finally {
        system.terminate()
        Await.ready(system.whenTerminated, 10.seconds): Unit
      }
    })

  private val lift: [A] => Future[A] => CIO[A] = [A] => (future: Future[A]) => CIO.lift(future)

  nativeTest("a distributed lock scopes native effects and skips contended bodies")(lockScopesNativeEffects(_)(lift).unsafeRun)

  nativeTest("scanAll streams every key as a native Pekko Source") { client =>
    scanAllFindsEveryKey(client)(lift)(client.scanAll(pattern = Some("scan-*"), count = Some(10L)).runWith(Sink.seq)).unsafeRun
  }

  nativeTest("subscribe delivers published messages as a native Pekko Source") { client =>
    val (confirmed, received) =
      client.subscribe[String]("smoke").take(3).toMat(Sink.seq)(Keep.both).run()
    for {
      _        <- confirmed
      // publish sequentially so the asserted m1/m2/m3 order is deterministic
      _        <- (1 to 3).foldLeft(Future.successful(0L))((acc, i) => acc.flatMap(_ => client.publish("smoke", s"m$i")))
      messages <- received
    } yield assertEquals(messages.toList, List("m1", "m2", "m3").map(Message("smoke", _)))
  }

  nativeTest("tailing helpers surface an infinite block timeout through the effect because Future cannot interrupt it") { client =>
    val tailed   =
      client.xTail[String, String]("stream:forever", block = BlockTimeout.Forever).runWith(Sink.ignore).failed
    val consumed =
      client.xConsume[String, String]("workers", "w1", "stream:forever", block = BlockTimeout.Forever)(_ => Future.unit).completion.failed
    for {
      e1 <- tailed
      e2 <- consumed
    } yield {
      assert(e1.isInstanceOf[SageException.InvalidArgument], e1)
      assert(e2.isInstanceOf[SageException.InvalidArgument], e2)
    }
  }
}

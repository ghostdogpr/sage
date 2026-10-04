package sage.integration

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import kyo.compat.*
import munit.{Location, TestOptions}

import sage.*
import sage.backend.*

class CeSmokeSuite extends SmokeSuite {

  private def nativeTest(options: TestOptions)(body: SageClient => IO[Unit])(using Location): Unit =
    test(options)(withContainers(server => SageClient.resource(configOf(server)).use(body).unsafeRunSync()))

  private val lift: [A] => IO[A] => CIO[A] = [A] => (io: IO[A]) => CIO.lift(io)

  nativeTest("a distributed lock scopes native effects and skips contended bodies")(lockScopesNativeEffects(_)(lift).lower)

  nativeTest("scanAll streams every key as a native fs2 Stream") { client =>
    scanAllFindsEveryKey(client)(lift)(client.scanAll(pattern = Some("scan-*"), count = Some(10L)).compile.toVector).lower
  }

  nativeTest("subscribe delivers published messages as a native fs2 Stream") { client =>
    client.subscribeResource[String]("smoke").use { stream =>
      for {
        _        <- (1 to 3).toList.traverse_(i => client.publish("smoke", s"m$i"))
        messages <- stream.take(3).compile.toVector
      } yield assertEquals(messages.toList, List("m1", "m2", "m3").map(Message("smoke", _)))
    }
  }
}

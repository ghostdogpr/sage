package sage.integration

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import kyo.compat.*
import munit.{Location, TestOptions}
import zio.*

import sage.*
import sage.backend.*
import sage.client.{DedicatedPoolConfig, SageConfig}

class ZioSmokeSuite extends SmokeSuite {

  private def nativeTest(options: TestOptions, tune: SageConfig => SageConfig = identity)(
    body: SageClient => RIO[Scope, Unit]
  )(using Location): Unit =
    test(options)(withContainers { server =>
      val program: Task[Unit] = ZIO.scoped(SageClient.scoped(tune(configOf(server))).flatMap(body))
      Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(program).getOrThrowFiberFailure())
    })

  private val lift: [A] => IO[SageException, A] => CIO[A] = [A] => (io: IO[SageException, A]) => CIO.lift(io)

  nativeTest("a distributed lock scopes native effects and skips contended bodies")(lockScopesNativeEffects(_)(lift).lower)

  private val pool = DedicatedPoolConfig(maxConnections = 2, acquireTimeout = FiniteDuration(1L, TimeUnit.SECONDS))

  nativeTest("an interrupted blocking command releases its pooled slot instead of leaking it", _.copy(dedicatedPool = pool)) { client =>
    for {
      _    <- ZIO.foreachDiscard(1 to 4)(_ => client.blPop[String]("leak:empty")(BlockTimeout.Forever).timeout(Duration.fromMillis(150)))
      none <- client.blPop[String]("leak:empty")(BlockTimeout.After(FiniteDuration(100L, TimeUnit.MILLISECONDS)))
    } yield assertEquals(none, None)
  }

  nativeTest("re-running the same blocking command value succeeds each round instead of hanging after the first") { client =>
    val blPop = client.blPop[String]("h1:reuse")(BlockTimeout.After(FiniteDuration(100L, TimeUnit.MILLISECONDS)))
    for {
      first  <- blPop
      second <- blPop.timeout(Duration.fromSeconds(5))
    } yield {
      assertEquals(first, None)
      assertEquals(second, Some(None), "re-running the same blocking effect hung: its lease was captured and single-shot")
    }
  }

  nativeTest("scanAll streams every key as a native ZStream") { client =>
    scanAllFindsEveryKey(client)(lift)(client.scanAll(pattern = Some("scan-*"), count = Some(10L)).runCollect).lower
  }

  nativeTest("subscribe delivers published messages as a native ZStream") { client =>
    for {
      stream   <- client.subscribeScoped[String]("smoke")
      _        <- ZIO.foreachDiscard(1 to 3)(i => client.publish("smoke", s"m$i"))
      messages <- stream.take(3).runCollect
    } yield assertEquals(messages.toList, List("m1", "m2", "m3").map(Message("smoke", _)))
  }

  nativeTest("hScanAll streams every field/value pair as a native ZStream") { client =>
    for {
      _     <- ZIO.foreachParDiscard(1 to 50)(i => client.hSet("hscan", (s"f$i", s"v$i")))
      pairs <- client.hScanAll[String, String]("hscan", count = Some(10L)).runCollect
    } yield assertEquals(pairs.toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)
  }

  nativeTest("xRangeAll pages every entry as a native ZStream") { client =>
    for {
      _       <- ZIO.foreachDiscard(1 to 50)(i => client.xAdd("xrangeall", XAddId.Explicit(StreamId(i.toLong, 0L)))(("f", s"v$i")))
      entries <- client.xRangeAll[String, String]("xrangeall", batch = 10L).runCollect
    } yield assertEquals(entries.map(_.id).toList, (1 to 50).map(i => StreamId(i.toLong, 0L)).toList)
  }

  nativeTest("xConsume tails a group and auto-acks each entry after the handler succeeds") { client =>
    val block = BlockTimeout.After(FiniteDuration(200L, TimeUnit.MILLISECONDS))
    for {
      _     <- ZIO.foreachDiscard(1 to 3)(i => client.xAdd("xconsume", XAddId.Explicit(StreamId(i.toLong, 0L)))(("f", s"v$i")))
      _     <- client.xGroupCreate("xconsume", "g", GroupStartId.At(StreamId(0L, 0L)))
      seen  <- Ref.make(Vector.empty[String])
      fiber <- client.xConsume[String, String]("g", "c", "xconsume", block = block)(entry => seen.update(_ ++ entry.fields.map(_._2))).fork
      _     <- seen.get.repeatUntil(_.size >= 3).timeoutFail(new RuntimeException("xConsume did not deliver"))(Duration.fromSeconds(10))
      _     <- fiber.interrupt
      got   <- seen.get
      pend  <- client.xPending("xconsume", "g")
    } yield {
      assertEquals(got.sorted, Vector("v1", "v2", "v3"))
      assertEquals(pend.total, 0L)
    }
  }
}

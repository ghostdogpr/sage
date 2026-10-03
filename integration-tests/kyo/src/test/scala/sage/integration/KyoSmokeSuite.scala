package sage.integration

import kyo.*
import kyo.compat.*
import munit.{Location, TestOptions}

import sage.*
import sage.backend.*

class KyoSmokeSuite extends SmokeSuite {

  private def nativeTest(options: TestOptions, timeout: Duration = Duration.Infinity)(body: SageClient => Unit < (Scope & Abort[Throwable] & Async))(
    using Location
  ): Unit =
    test(options)(withContainers { server =>
      val program: Unit < (Scope & Abort[Throwable] & Async) = SageClient.scoped(configOf(server)).map(body)
      import AllowUnsafe.embrace.danger
      KyoApp.Unsafe.runAndBlock(timeout)(program).getOrThrow
    })

  private val lift: [A] => (A < (Abort[SageException] & Async)) => CIO[A] = [A] => (v: A < (Abort[SageException] & Async)) => CIO.lift(v)

  nativeTest("a distributed lock scopes native effects and skips contended bodies")(lockScopesNativeEffects(_)(lift).lower)

  nativeTest("a distributed lock preserves a native panic and releases ownership", 3L.seconds) { client =>
    val failure = new IllegalStateException("body panic")
    val locks   = client.lock[String]()
    for {
      result   <- Abort.run[SageException](locks.tryWithLock[Int]("native-panic")(Abort.panic(failure)))
      _         = assertEquals(result, Result.Panic(failure))
      exists   <- client.exists("4:lock:native-panic")
      acquired <- locks.tryWithLock("native-panic")(client.ping())
    } yield {
      assertEquals(exists, 0L)
      assertEquals(acquired, Some("PONG"))
    }
  }

  nativeTest("scanAll streams every key as a native Kyo Stream") { client =>
    scanAllFindsEveryKey(client)(lift)(client.scanAll(pattern = Some("scan-*"), count = Some(10L)).run).lower
  }

  nativeTest("subscribe delivers published messages as a native Kyo Stream") { client =>
    for {
      stream <- client.subscribeScoped[String]("smoke")
      _      <- Kyo.foreachDiscard(1 to 3)(i => client.publish("smoke", s"m$i"))
      chunk  <- stream.take(3).run
    } yield assertEquals(chunk.toList, List("m1", "m2", "m3").map(Message("smoke", _)))
  }

  // regression for the 4096-page rechunk that buffered unbounded streams; the timeout makes a recurrence fail rather than hang
  nativeTest("xTail emits replayed entries immediately instead of buffering them", 15L.seconds) { client =>
    for {
      _       <- Kyo.foreachDiscard(1 to 3)(i => client.xAdd("xtail", XAddId.Explicit(StreamId(i.toLong, 0L)))(("f", s"v$i")))
      entries <- client.xTail[String, String]("xtail").take(3).run
    } yield assertEquals(entries.toList.flatMap(_.fields.map(_._2)), List("v1", "v2", "v3"))
  }
}

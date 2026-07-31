package sage.integration

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import sage.*
import sage.backend.*

class PekkoSmokeSuite extends ServerSuite(Images.redis) {

  private def withNativeClient[A](body: (SageClient, Materializer, ActorSystem[Nothing]) => Future[A]): A =
    withContainers { server =>
      given system: ActorSystem[Nothing]      = ActorSystem(Behaviors.empty, "pekko-smoke")
      given scala.concurrent.ExecutionContext = system.executionContext
      val mat                                 = Materializer(system)
      try
        Await.result(
          SageClient.connect(configOf(server)).flatMap { client =>
            body(client, mat, system).transformWith(result => client.close.recover { case _ => () }.transform(_ => result))
          },
          30.seconds
        )
      finally {
        system.terminate()
        Await.ready(system.whenTerminated, 10.seconds): Unit
      }
    }

  test("an end user connects and round-trips with scala.concurrent.Future") {
    val values = withNativeClient { (client, _, _) =>
      for {
        pong <- client.ping()
        _    <- Future.traverse(1 to 50)(i => client.set(s"key-$i", s"value-$i"))
        got  <- Future.traverse(1 to 50)(i => client.get[String](s"key-$i"))
      } yield (pong, got)
    }
    assertEquals(values._1, "PONG")
    assertEquals(values._2.toList, (1 to 50).toList.map(i => Option(s"value-$i")))
  }

  test("a pipeline returns a typed tuple natively, surfacing failures per position") {
    val (out, attempt) = withNativeClient { (client, _, _) =>
      for {
        _       <- client.set("pipe:a", "x")
        _       <- client.set("pipe:n", 10)
        out     <- client.pipeline((Commands.get[String, String]("pipe:a"), Commands.incrBy[String]("pipe:n", 5)))
        _       <- client.set("pipe:str", "hello")
        attempt <- client.pipelineAttempt((Commands.get[String, String]("pipe:str"), Commands.incr[String]("pipe:str")))
      } yield (out, attempt)
    }
    assertEquals(out, (Some("x"), 15L))
    assert(attempt._1 == Right(Some("hello")), attempt._1)
    assert(attempt._2.isLeft, attempt._2)
  }

  test("a transaction commits atomically with Future, guarded by WATCH") {
    val out = withNativeClient { (client, _, _) =>
      for {
        _   <- client.set("tx:n", 1)
        res <- client.transaction { tx =>
                 for {
                   _   <- tx.watch("tx:n")
                   _   <- tx.get[Int]("tx:n")
                   res <- tx.exec((Commands.incr[String]("tx:n"), Commands.incrBy[String]("tx:n", 4)))
                 } yield res
               }
      } yield res
    }
    assertEquals(out, Some((2L, 6L)))
  }

  test("scanAll streams every key as a native Pekko Source") {
    val keys = withNativeClient { (client, mat, _) =>
      given Materializer = mat
      for {
        _    <- Future.traverse(1 to 50)(i => client.set(s"scan-$i", "v"))
        keys <- client.scanAll(pattern = Some("scan-*"), count = Some(10L)).runWith(Sink.seq)
      } yield keys
    }
    assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)
  }

  test("subscribe delivers published messages as a native Pekko Source") {
    val messages = withNativeClient { (client, mat, _) =>
      given Materializer        = mat
      val (confirmed, received) =
        client.subscribe[String]("smoke").take(3).toMat(Sink.seq)(Keep.both).run()
      for {
        _        <- confirmed
        // publish sequentially so the asserted m1/m2/m3 order is deterministic
        _        <- (1 to 3).foldLeft(Future.successful(0L))((acc, i) => acc.flatMap(_ => client.publish("smoke", s"m$i")))
        messages <- received
      } yield messages
    }
    assertEquals(messages.map(_.channel).toSet, Set("smoke"))
    assertEquals(messages.map(_.payload).toList, List("m1", "m2", "m3"))
  }

  test("hScanAll streams every field/value pair as a native Pekko Source") {
    val pairs = withNativeClient { (client, mat, _) =>
      given Materializer = mat
      for {
        _     <- Future.traverse(1 to 50)(i => client.hSet("hscan", (s"f$i", s"v$i")))
        pairs <- client.hScanAll[String, String]("hscan", count = Some(10L)).runWith(Sink.seq)
      } yield pairs
    }
    assertEquals(pairs.toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)
  }

  test("sScanAll streams every member as a native Pekko Source") {
    val members = withNativeClient { (client, mat, _) =>
      given Materializer = mat
      for {
        _       <- Future.traverse(1 to 50)(i => client.sAdd("sscan", s"m$i"))
        members <- client.sScanAll[String]("sscan", count = Some(10L)).runWith(Sink.seq)
      } yield members
    }
    assertEquals(members.toSet, (1 to 50).map(i => s"m$i").toSet)
  }

  test("zScanAll streams every member/score pair as a native Pekko Source") {
    val pairs = withNativeClient { (client, mat, _) =>
      given Materializer = mat
      for {
        _     <- Future.traverse(1 to 50)(i => client.zAdd("zscan")((s"m$i", i.toDouble)))
        pairs <- client.zScanAll[String]("zscan", count = Some(10L)).runWith(Sink.seq)
      } yield pairs
    }
    assertEquals(pairs.toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)
  }

  test("tailing helpers surface an infinite block timeout through the effect because Future cannot interrupt it") {
    withNativeClient { (client, mat, system) =>
      given ActorSystem[Nothing]              = system
      given Materializer                      = mat
      given scala.concurrent.ExecutionContext = system.executionContext
      val tailed                              =
        client.xTail[String, String]("stream:forever", block = BlockTimeout.Forever).runWith(Sink.ignore).failed
      val consumed                            =
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

  test("client.rateLimiter admits up to capacity then denies") {
    val (first, second, denied) = withNativeClient { (client, _, system) =>
      given scala.concurrent.ExecutionContext = system.executionContext
      val rl                                  = client.rateLimiter[String](RateLimit(capacity = 2, refillTokens = 1, refillPeriod = 1.hour))
      for {
        a <- rl.tryAcquire("smoke")
        b <- rl.tryAcquire("smoke")
        c <- rl.tryAcquire("smoke")
      } yield (a, b, c)
    }
    assert(first.isAllowed && second.isAllowed, "the first two are admitted")
    assert(!denied.isAllowed, "the third is denied once the bucket empties")
  }
}

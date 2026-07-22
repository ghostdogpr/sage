package sage.integration

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.*

import com.dimafeng.testcontainers.GenericContainer
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import sage.*
import sage.backend.*

class PekkoSmokeSuite extends ServerSuite(Images.redis) {

  private def withClientMat[A](server: GenericContainer)(body: (SageClient, Materializer, ActorSystem[Nothing]) => Future[A]): A = {
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
      val _ = Await.ready(system.whenTerminated, 10.seconds)
    }
  }

  test("an end user connects and round-trips with scala.concurrent.Future") {
    withContainers { server =>
      val values = withClientMat(server) { (client, _, _) =>
        for {
          pong <- client.ping()
          _    <- Future.traverse(1 to 50)(i => client.set(s"key-$i", s"value-$i"))
          got  <- Future.traverse(1 to 50)(i => client.get[String](s"key-$i"))
        } yield (pong, got)
      }
      assertEquals(values._1, "PONG")
      assertEquals(values._2.toList, (1 to 50).toList.map(i => Option(s"value-$i")))
    }
  }

  test("a pipeline returns a typed tuple natively, surfacing failures per position") {
    withContainers { server =>
      val (out, attempt) = withClientMat(server) { (client, _, _) =>
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
  }

  test("a transaction commits atomically with Future, guarded by WATCH") {
    withContainers { server =>
      val out = withClientMat(server) { (client, _, _) =>
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
  }

  test("scanAll streams every key as a native Pekko Source") {
    withContainers { server =>
      val keys = withClientMat(server) { (client, mat, _) =>
        given Materializer = mat
        for {
          _    <- Future.traverse(1 to 50)(i => client.set(s"scan-$i", "v"))
          keys <- client.scanAll(pattern = Some("scan-*"), count = Some(10L)).runWith(Sink.seq)
        } yield keys
      }
      assertEquals(keys.toSet, (1 to 50).map(i => s"scan-$i").toSet)
    }
  }

  test("subscribe delivers published messages as a native Pekko Source") {
    withContainers { server =>
      val messages = withClientMat(server) { (client, mat, _) =>
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
  }

  test("hScanAll streams every field/value pair as a native Pekko Source") {
    withContainers { server =>
      val pairs = withClientMat(server) { (client, mat, _) =>
        given Materializer = mat
        for {
          _     <- Future.traverse(1 to 50)(i => client.hSet("hscan", (s"f$i", s"v$i")))
          pairs <- client.hScanAll[String, String]("hscan", count = Some(10L)).runWith(Sink.seq)
        } yield pairs
      }
      assertEquals(pairs.toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)
    }
  }

  test("zScanAll streams every member/score pair as a native Pekko Source") {
    withContainers { server =>
      val pairs = withClientMat(server) { (client, mat, _) =>
        given Materializer = mat
        for {
          _     <- Future.traverse(1 to 50)(i => client.zAdd("zscan")((s"m$i", i.toDouble)))
          pairs <- client.zScanAll[String]("zscan", count = Some(10L)).runWith(Sink.seq)
        } yield pairs
      }
      assertEquals(pairs.toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)
    }
  }

  test("tailing helpers surface an infinite block timeout through the effect because Future cannot interrupt it") {
    withContainers { server =>
      withClientMat(server) { (client, mat, system) =>
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
  }

  test("client.rateLimiter admits up to capacity then denies") {
    withContainers { server =>
      val (first, second, denied) = withClientMat(server) { (client, _, system) =>
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
}

package sage.integration

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.*
import scala.util.Using

import sage.*
import sage.backend.*

class DirectSmokeSuite extends ServerSuite(Images.redis) {

  private def withNativeClient(body: SageClient => Unit): Unit =
    withContainers(server => SageClient.use(configOf(server))(body))

  test("use keeps the caller's interrupt status when the body returns with it set") {
    withContainers { server =>
      SageClient.use(configOf(server)) { client =>
        assertEquals(client.ping(), "PONG")
        Thread.currentThread().interrupt()
      }
      assert(Thread.interrupted(), "closing the client should not clear the interrupt status")
    }
  }

  test("close releases the connections even when the caller's interrupt status is already set") {
    withContainers { server =>
      SageClient.use(configOf(server)) { observer =>
        val client   = SageClient.connect(configOf(server).copy(clientName = Some("interrupted-close")))
        assertEquals(client.ping(), "PONG")
        Thread.currentThread().interrupt()
        client.close
        assert(Thread.interrupted(), "close should keep the interrupt status")
        val deadline = System.nanoTime() + 5.seconds.toNanos
        while (observer.clientList.contains("name=interrupted-close") && System.nanoTime() < deadline) Thread.sleep(50)
        assert(!observer.clientList.contains("name=interrupted-close"), "the server should no longer see the closed client's connection")
      }
    }
  }

  test("a distributed lock scopes blocking code and skips contended bodies") {
    withNativeClient { client =>
      val locks     = client.lock[String]()
      var evaluated = false
      val busy      = locks.withLock("native-lock", 2.seconds) {
        locks.tryWithLock("native-lock") {
          evaluated = true
          client.ping()
        }
      }
      val acquired  = locks.tryWithLock("native-lock")(client.ping())
      assertEquals(busy, None)
      assertEquals(acquired, Some("PONG"))
      assert(!evaluated)
    }
  }

  test("a lock body runs on the calling thread and sees its thread-local values") {
    withNativeClient { client =>
      val context = new ThreadLocal[String]
      context.set("request-42")
      try {
        val caller          = Thread.currentThread()
        val (thread, value) = client.lock[String]().withLock("thread-lock", 2.seconds)((Thread.currentThread(), context.get()))
        assert(thread eq caller)
        assertEquals(value, "request-42")
      } finally context.remove()
    }
  }

  test("an end user connects and round-trips from many virtual threads") {
    withNativeClient { client =>
      assertEquals(client.ping(), "PONG")
      Using.resource(Executors.newVirtualThreadPerTaskExecutor()) { executor =>
        val futures = (1 to 50).toList.map { i =>
          executor.submit { () =>
            val _ = client.set(s"key-$i", s"value-$i")
            client.get[String](s"key-$i")
          }
        }
        assertEquals(futures.map(_.get(10, TimeUnit.SECONDS)), (1 to 50).toList.map(i => Some(s"value-$i")))
      }
    }
  }

  test("interrupting a thread blocked on a command cancels the wait") {
    withNativeClient { client =>
      var failure: Throwable = null
      val blocked            = Thread.ofVirtual().start { () =>
        try client.blPop[String]("never-filled")(BlockTimeout.After(5.seconds)): Unit
        catch { case t: Throwable => failure = t }
      }
      Thread.sleep(200)
      blocked.interrupt()
      blocked.join(2000)
      assert(!blocked.isAlive, "the interrupted thread should stop waiting")
      assert(failure.isInstanceOf[InterruptedException], String.valueOf(failure))
      assertEquals(client.ping(), "PONG")
    }
  }

  test("a pipeline returns a typed tuple, surfacing failures per position") {
    withNativeClient { client =>
      val _       = client.set("pipe:a", "x")
      val _       = client.set("pipe:n", 10)
      val out     = client.pipeline((Commands.get[String, String]("pipe:a"), Commands.incrBy[String]("pipe:n", 5)))
      assertEquals(out, (Some("x"), 15L))
      val _       = client.set("pipe:str", "hello")
      val attempt = client.pipelineAttempt((Commands.get[String, String]("pipe:str"), Commands.incr[String]("pipe:str")))
      assert(attempt._1 == Right(Some("hello")), attempt._1)
      assert(attempt._2.isLeft, attempt._2)
    }
  }

  test("a transaction commits atomically, guarded by WATCH") {
    withNativeClient { client =>
      val _   = client.set("tx:n", 1)
      val out = client.transaction { tx =>
        tx.watch("tx:n")
        val _ = tx.get[Int]("tx:n")
        tx.exec((Commands.incr[String]("tx:n"), Commands.incrBy[String]("tx:n", 4)))
      }
      assertEquals(out, Some((2L, 6L)))
    }
  }

  test("scanAll iterates over every key") {
    withNativeClient { client =>
      (1 to 50).foreach(i => client.set(s"scan-$i", "v"))
      val keys = client.scanAll(pattern = Some("scan-*"), count = Some(10L)).toSet
      assertEquals(keys, (1 to 50).map(i => s"scan-$i").toSet)
    }
  }

  test("subscribe delivers published messages through a blocking iterator") {
    withNativeClient { client =>
      Using.resource(client.subscribe[String]("smoke")) { messages =>
        (1 to 3).foreach(i => client.publish("smoke", s"m$i"))
        val received = messages.take(3).toList
        assertEquals(received.map(_.channel).toSet, Set("smoke"))
        assertEquals(received.map(_.payload), List("m1", "m2", "m3"))
      }
    }
  }

  test("closing a subscription ends its iterator") {
    withNativeClient { client =>
      val messages = client.subscribe[String]("closing")
      val _        = client.publish("closing", "a")
      assertEquals(messages.next().payload, "a")
      messages.close()
      assert(!messages.hasNext)
    }
  }

  test("onMessage delivers each message to the callback until it is closed") {
    withNativeClient { client =>
      val received     = new java.util.concurrent.LinkedBlockingQueue[String]()
      val subscription = client.onMessage[String]("callback")(m => received.put(m.payload))
      (1 to 3).foreach(i => client.publish("callback", s"m$i"))
      assertEquals((1 to 3).map(_ => received.poll(5, TimeUnit.SECONDS)).toList, List("m1", "m2", "m3"))
      // the delivery thread is waiting for the next message, so close must wake it rather than wait for traffic
      val started      = System.nanoTime()
      subscription.close()
      assert((System.nanoTime() - started).nanos < 2.seconds, "close should not wait for another message")
      assertEquals(client.publish("callback", "after"), 0L)
      subscription.close()
    }
  }

  test("a failing onMessage callback does not stop later deliveries") {
    withNativeClient { client =>
      val received     = new java.util.concurrent.LinkedBlockingQueue[String]()
      val subscription = client.onMessage[String]("failing") { m =>
        if (m.payload == "bad") throw new RuntimeException("expected test failure")
        received.put(m.payload)
      }
      try {
        val _ = client.publish("failing", "bad")
        val _ = client.publish("failing", "good")
        assertEquals(received.poll(5, TimeUnit.SECONDS), "good")
      } finally subscription.close()
    }
  }

  test("an onMessage callback does not inherit the subscriber's inheritable thread-local values") {
    withNativeClient { client =>
      val context      = new InheritableThreadLocal[String]
      val seen         = new java.util.concurrent.LinkedBlockingQueue[String]()
      context.set("request-42")
      val subscription =
        try client.onMessage[String]("context")(_ => seen.put(String.valueOf(context.get())))
        finally context.remove()
      try {
        val _ = client.publish("context", "x")
        assertEquals(seen.poll(5, TimeUnit.SECONDS), "null")
      } finally subscription.close()
    }
  }

  test("onPatternMessage and onShardMessage deliver pattern and shard messages") {
    withNativeClient { client =>
      val patterns = new java.util.concurrent.LinkedBlockingQueue[String]()
      val shards   = new java.util.concurrent.LinkedBlockingQueue[String]()
      Using.resources(
        client.onPatternMessage[String]("news.*")(m => patterns.put(s"${m.pattern} ${m.channel} ${m.payload}")),
        client.onShardMessage[String]("shard")(m => shards.put(m.payload))
      ) { (_, _) =>
        val _ = client.publish("news.sport", "goal")
        val _ = client.sPublish("shard", "s1")
        assertEquals(patterns.poll(5, TimeUnit.SECONDS), "news.* news.sport goal")
        assertEquals(shards.poll(5, TimeUnit.SECONDS), "s1")
      }
    }
  }

  test("hScanAll, sScanAll, and zScanAll iterate over every element") {
    withNativeClient { client =>
      (1 to 50).foreach { i =>
        val _ = client.hSet("hscan", (s"f$i", s"v$i"))
        val _ = client.sAdd("sscan", s"m$i")
        client.zAdd("zscan")((s"m$i", i.toDouble))
      }
      assertEquals(client.hScanAll[String, String]("hscan", count = Some(10L)).toMap, (1 to 50).map(i => s"f$i" -> s"v$i").toMap)
      assertEquals(client.sScanAll[String]("sscan", count = Some(10L)).toSet, (1 to 50).map(i => s"m$i").toSet)
      assertEquals(client.zScanAll[String]("zscan", count = Some(10L)).toMap, (1 to 50).map(i => s"m$i" -> i.toDouble).toMap)
    }
  }

  test("xConsume acknowledges entries and stops when its thread is interrupted") {
    withNativeClient { client =>
      client.xGroupCreate("events", "workers", mkStream = true)
      (1 to 3).foreach(i => client.xAdd("events")(("n", i.toString)))
      val _        = client.xAdd("events")(("n", "4"))
      val seen     = new java.util.concurrent.LinkedBlockingQueue[String]()
      val stopped  = new AtomicBoolean(false)
      val consumer = Thread.ofVirtual().start { () =>
        try
          client.xConsume[String, String]("workers", "w1", "events", block = BlockTimeout.After(100.millis))(e => seen.put(e.fields.toMap.apply("n")))
        catch { case _: InterruptedException => stopped.set(true) }
      }
      val values   = (1 to 4).map(_ => seen.poll(5, TimeUnit.SECONDS))
      consumer.interrupt()
      consumer.join(2000)
      assertEquals(values.toList, List("1", "2", "3", "4"))
      assert(stopped.get())
    }
  }

  test("client.rateLimiter admits up to capacity then denies") {
    withNativeClient { client =>
      val rl     = client.rateLimiter[String](RateLimit(capacity = 2, refillTokens = 1, refillPeriod = 1.hour))
      val first  = rl.tryAcquire("smoke")
      val second = rl.tryAcquire("smoke")
      val denied = rl.tryAcquire("smoke")
      assert(first.isAllowed && second.isAllowed, "the first two are admitted")
      assert(!denied.isAllowed, "the third is denied once the bucket empties")
    }
  }
}

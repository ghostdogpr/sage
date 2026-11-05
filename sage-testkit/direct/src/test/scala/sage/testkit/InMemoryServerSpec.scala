package sage.testkit

import scala.concurrent.duration.*

import sage.*
import sage.backend.*
import sage.codec.ValueCodec

class InMemoryServerSpec extends InMemorySuite {

  shared.test("clients of one server share the keyspace and pub/sub messages, and a throwing codec fails the subscription") {
    (_, publisher, subscriber) =>
      val messages = subscriber.subscribe[String]("news")
      val patterns = subscriber.pSubscribe[String]("ne*", "ne*")
      subscriber.subscribe[String]("news", "news"): Unit
      publisher.set("shared", "yes"): Unit
      assertEquals(subscriber.get[String]("shared"), Some("yes"))
      assertEquals(publisher.publish("news", "hello"), 2L)
      assertEquals(publisher.publish("elsewhere", "ignored"), 0L)
      assertEquals(inBackground(messages.next()).get(5, java.util.concurrent.TimeUnit.SECONDS), Message("news", "hello"))
      messages.close()
      assertEquals(publisher.publish("news", "gone"), 2L)
      val throwing = ValueCodec.from[Int](n => Bytes.utf8(n.toString))(bytes => Right(bytes.asUtf8String.toInt))
      val numbers  = subscriber.subscribe[Int]("numbers")(using throwing)
      publisher.publish("numbers", "x"): Unit
      inBackground(intercept[SageException.DecodeError](numbers.next())).get(5, java.util.concurrent.TimeUnit.SECONDS): Unit
      subscriber.close
      assert(!inBackground(patterns.hasNext).get(5, java.util.concurrent.TimeUnit.SECONDS))
  }

  shared.test("a blocking pop waits for another client's push") { (server, consumer, producer) =>
    val popped = inBackground(consumer.blPop[String]("jobs")(BlockTimeout.After(5.seconds)))
    awaitWaiting(server, 1)
    producer.rPush("jobs", "job-1"): Unit
    assertEquals(popped.get(5, java.util.concurrent.TimeUnit.SECONDS), Some(("jobs", "job-1")))
  }

  shared.test("blocking pops whose timeouts race pushes lose no element") { (server, consumer, producer) =>
    val poppers  = Vector.fill(4)(inBackground((1 to 200).count(_ => consumer.blPop[String]("jobs")(BlockTimeout.After(1.millis)).nonEmpty)))
    val deadline = System.nanoTime() + 10.seconds.toNanos
    var pushed   = 0L
    // pushing only while a pop waits makes each push race that pop's timeout
    while (!poppers.forall(_.isDone) && System.nanoTime() < deadline)
      if (server.executor.waiting > 0) {
        producer.rPush("jobs", "job"): Unit
        pushed += 1
      } else Thread.`yield`()
    assertEquals(poppers.map(_.get(5, java.util.concurrent.TimeUnit.SECONDS).toLong).sum + producer.lLen("jobs"), pushed)
  }

  shared.test("a lock excludes another client of the server, and a waiting withLock takes it once the holder releases it") {
    (server, holder, contender) =>
      val waiting = holder.lock[String]().withLock("job", 5.seconds) {
        assertEquals(contender.lock[String]().tryWithLock("job")("ran"), None)
        val waiting = inBackground(contender.lock[String]().withLock("job", 5.seconds)("ran"))
        awaitWaiting(server, 1)
        waiting
      }
      assertEquals(waiting.get(5, java.util.concurrent.TimeUnit.SECONDS), "ran")
  }
}

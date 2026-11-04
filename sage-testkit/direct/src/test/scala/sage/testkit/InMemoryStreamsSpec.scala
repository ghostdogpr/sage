package sage.testkit

import java.util.concurrent.{CompletableFuture, TimeUnit}

import scala.concurrent.duration.*

import sage.*

class InMemoryStreamsSpec extends munit.FunSuite {

  private def inBackground[A](body: => A): CompletableFuture[A] = {
    val result = new CompletableFuture[A]()
    Thread.startVirtualThread(() => result.complete(body): Unit): Unit
    result
  }

  test("a blocked XREAD with $ returns only entries added after the call started") {
    val server   = InMemoryServer()
    val reader   = InMemorySageClient(server)
    val producer = InMemorySageClient(server)
    producer.xAdd("events")(("n", "old")): Unit
    val read     = inBackground(reader.xRead[String, String](("events", ReadId.New))(block = Some(BlockTimeout.After(5.seconds))))
    Thread.sleep(50)
    // an unrelated write wakes the reader, which must keep waiting
    producer.set("other", "x"): Unit
    Thread.sleep(50)
    val id       = producer.xAdd("events")(("n", "new"))
    assertEquals(read.get(5, TimeUnit.SECONDS), Vector("events" -> Vector(StreamEntry(id, Vector("n" -> "new")))))
  }

  test("two consumers blocked on one group each receive one new entry") {
    val server                = InMemoryServer()
    val producer              = InMemorySageClient(server)
    producer.xGroupCreate("jobs", "workers", mkStream = true)
    def consume(name: String) =
      inBackground {
        InMemorySageClient(server)
          .xReadGroup[String, String]("workers", name)(("jobs", GroupReadId.New))(count = Some(1L), block = Some(BlockTimeout.After(5.seconds)))
          .flatMap(_._2.map(_.fields))
      }
    val first                 = consume("a")
    val second                = consume("b")
    Thread.sleep(50)
    producer.xAdd("jobs")(("job", "1")): Unit
    producer.xAdd("jobs")(("job", "2")): Unit
    val received              = Vector(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS)).flatten
    assertEquals(received.toSet, Set(Vector("job" -> "1"), Vector("job" -> "2")))
    assertEquals(producer.xPending("jobs", "workers").total, 2L)
  }

  test("a blocked XREADGROUP times out with no entries") {
    val client = InMemorySageClient()
    client.xGroupCreate("quiet", "g", mkStream = true)
    val result = client.xReadGroup[String, String]("g", "c")(("quiet", GroupReadId.New))(block = Some(BlockTimeout.After(100.millis)))
    assertEquals(result, Vector.empty)
  }

  test("approximate trimming leaves a short stream alone, as a server does") {
    val client = InMemorySageClient()
    (1 to 10).foreach(i => client.xAdd("short")(("i", i.toString)): Unit)
    assertEquals(client.xTrim("short", Trimming.Approximate(TrimThreshold.MaxLen(2L))), 0L)
    assertEquals(client.xTrim("short", Trimming.Exact(TrimThreshold.MaxLen(2L))), 8L)
  }
}

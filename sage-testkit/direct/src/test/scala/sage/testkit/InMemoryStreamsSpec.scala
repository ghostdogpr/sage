package sage.testkit

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.*

import sage.*
import sage.client.inmemory.Executor
import sage.protocol.Frame

class InMemoryStreamsSpec extends InMemorySuite {

  shared.test("a blocked XREAD with $ returns only entries added after the call started") { (server, reader, producer) =>
    producer.xAdd("events")(("n", "old")): Unit
    val read = inBackground(reader.xRead[String, String](("events", ReadId.New))(block = Some(BlockTimeout.After(5.seconds))))
    awaitWaiting(server, 1)
    // an unrelated write wakes the reader, which must keep waiting
    producer.set("other", "x"): Unit
    awaitWaiting(server, 1)
    val id   = producer.xAdd("events")(("n", "new"))
    assertEquals(read.get(5, TimeUnit.SECONDS), Vector("events" -> Vector(StreamEntry(id, Vector("n" -> "new")))))
  }

  shared.test("two consumers blocked on one group each receive one new entry") { (server, producer, _) =>
    producer.xGroupCreate("jobs", "workers", mkStream = true)
    producer.xGroupCreate("jobs", "auditors")
    def consume(name: String) =
      inBackground {
        InMemorySageClient(server)
          .xReadGroup[String, String]("workers", name)(("jobs", GroupReadId.New))(count = Some(1L), block = Some(BlockTimeout.After(5.seconds)))
          .flatMap(_._2.map(_.fields))
      }
    val first                 = consume("a")
    val second                = consume("b")
    awaitWaiting(server, 2)
    producer.xAdd("jobs")(("job", "1")): Unit
    producer.xAdd("jobs")(("job", "2")): Unit
    val received              = Vector(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS)).flatten
    assertEquals(received.toSet, Set(Vector("job" -> "1"), Vector("job" -> "2")))
    assertEquals(producer.xPending("jobs", "workers").consumers, Vector("a" -> 1L, "b" -> 1L))
    assertEquals(producer.xInfoConsumers("jobs", "workers").map(c => c.name -> c.pending), Vector("a" -> 1L, "b" -> 1L))
    assertEquals(producer.xInfoGroups("jobs").map(_.name), Vector("auditors", "workers"))
  }

  clients.test("a blocked XREADGROUP times out with no entries") { client =>
    client.xGroupCreate("quiet", "g", mkStream = true)
    val result = client.xReadGroup[String, String]("g", "c")(("quiet", GroupReadId.New))(block = Some(BlockTimeout.After(100.millis)))
    assertEquals(result, Vector.empty)
  }

  clients.test("approximate trimming trims exactly, removing at most LIMIT entries") { client =>
    (1 to 10).foreach(i => client.xAdd("short")(("i", i.toString)): Unit)
    assertEquals(client.xTrim("short", Trimming.Approximate(TrimThreshold.MaxLen(2L), limit = Some(3L))), 3L)
    assertEquals(client.xTrim("short", Trimming.Approximate(TrimThreshold.MaxLen(2L))), 5L)
    assertEquals(client.xTrim("short", Trimming.Approximate(TrimThreshold.MaxLen(0L), limit = Some(0L))), 2L)
  }

  test("XINFO GROUPS reports the lag, and XAUTOCLAIM examines at most ten entries per claim") {
    val executor                    = new Executor
    def run(command: String): Frame = raw(executor, command)
    val (none, at)                  = (Frame.Array(Vector.empty), (id: String) => Frame.BulkString(Bytes.utf8(id)))
    (1 to 12).foreach(i => run(s"XADD v $i-1 a 1"): Unit)
    Vector("XGROUP CREATE v g 0", "XREADGROUP GROUP g c COUNT 2 STREAMS v >", "XDEL v 1-1").foreach(run(_): Unit)
    val group                       = Frame.Map(
      Vector("name" -> at("g"), "consumers" -> Frame.Integer(1), "pending" -> Frame.Integer(2), "last-delivered-id" -> at("2-1"))
        .map((name, value) => at(name) -> value) ++ Vector(at("entries-read") -> Frame.Null, at("lag") -> Frame.Integer(10))
    )
    assertEquals(run("XINFO GROUPS v"), Frame.Array(Vector(group)))
    run("XREADGROUP GROUP g c STREAMS v >"): Unit
    // an entry gone from the stream counts as a claim whatever its idle time
    assertEquals(run("XAUTOCLAIM v g d 1000 0 COUNT 1"), Frame.Array(Vector(at("2-1"), none, Frame.Array(Vector(at("1-1"))))))
    assertEquals(run("XAUTOCLAIM v g d 1000 2-1 COUNT 1"), Frame.Array(Vector(at("12-1"), none, none)))
  }
}

package sage.commands

import java.time.Instant

import scala.concurrent.duration.*

import sage.Bytes
import sage.protocol.Frame

class ServerSpec extends munit.FunSuite {

  private def bulk(value: String): Frame = Frame.BulkString(Bytes.utf8(value))

  test("CONFIG GET decodes a RESP3 map and the RESP2 flat-array shape alike") {
    val resp3 = Frame.Map(Vector(bulk("maxmemory") -> bulk("100mb"), bulk("save") -> bulk("3600 1")))
    assertEquals(Reply.run(Server.configGet("*"), resp3), Right(Map("maxmemory" -> "100mb", "save" -> "3600 1")))
    val resp2 = Frame.Array(Vector(bulk("maxmemory"), bulk("100mb")))
    assertEquals(Reply.run(Server.configGet("*"), resp2), Right(Map("maxmemory" -> "100mb")))
  }

  test("TIME decodes seconds + microseconds into an Instant") {
    val reply = Frame.Array(Vector(bulk("1700000000"), bulk("123456")))
    assertEquals(Reply.run(Server.time, reply), Right(Instant.ofEpochSecond(1700000000L, 123456000L)))
  }

  test("ROLE decodes master, replica, and sentinel forms") {
    val master = Frame.Array(
      Vector(bulk("master"), Frame.Integer(100L), Frame.Array(Vector(Frame.Array(Vector(bulk("127.0.0.1"), bulk("6380"), bulk("90"))))))
    )
    assertEquals(Reply.run(Server.role, master), Right(Role.Master(100L, Vector(ReplicaNode("127.0.0.1", 6380, 90L)))))

    val replica = Frame.Array(Vector(bulk("slave"), bulk("127.0.0.1"), Frame.Integer(6379L), bulk("connected"), Frame.Integer(50L)))
    assertEquals(Reply.run(Server.role, replica), Right(Role.Replica("127.0.0.1", 6379, "connected", 50L)))

    val sentinel = Frame.Array(Vector(bulk("sentinel"), Frame.Array(Vector(bulk("master1"), bulk("master2")))))
    assertEquals(Reply.run(Server.role, sentinel), Right(Role.Sentinel(Vector("master1", "master2"))))
  }

  test("ROLE rejects an out-of-range replica port instead of wrapping it") {
    val wrapping =
      Frame.Array(Vector(bulk("slave"), bulk("127.0.0.1"), Frame.Integer(Int.MaxValue.toLong + 1L), bulk("connected"), Frame.Integer(0L)))
    assert(Reply.run(Server.role, wrapping).isLeft, "a port above Int.MaxValue must not wrap to a valid-looking port")
    val tooLarge =
      Frame.Array(Vector(bulk("master"), Frame.Integer(0L), Frame.Array(Vector(Frame.Array(Vector(bulk("127.0.0.1"), bulk("70000"), bulk("0")))))))
    assert(Reply.run(Server.role, tooLarge).isLeft, "a replica port outside 1..65535 must be a DecodeError")
  }

  test("SLOWLOG GET decodes entries, defaulting client fields absent on old servers") {
    val withClient = Frame.Array(
      Vector(
        Frame.Array(
          Vector(
            Frame.Integer(7L),
            Frame.Integer(1700000000L),
            Frame.Integer(150L),
            Frame.Array(Vector(bulk("GET"), bulk("k"))),
            bulk("1.2.3.4:5"),
            bulk("app")
          )
        )
      )
    )
    assertEquals(
      Reply.run(Server.slowLogGet(), withClient),
      Right(Vector(SlowLogEntry(7L, Instant.ofEpochSecond(1700000000L), 150.micros, Vector("GET", "k"), "1.2.3.4:5", "app")))
    )
    val old        = Frame.Array(Vector(Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(10L), Frame.Integer(20L), Frame.Array(Vector(bulk("PING")))))))
    assertEquals(Reply.run(Server.slowLogGet(), old), Right(Vector(SlowLogEntry(1L, Instant.ofEpochSecond(10L), 20.micros, Vector("PING"), "", ""))))
  }

  test("LATENCY LATEST decodes event rows") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(bulk("command"), Frame.Integer(1700000000L), Frame.Integer(5L), Frame.Integer(20L)))))
    assertEquals(
      Reply.run(Server.latencyLatest, reply),
      Right(Vector(LatencyEntry("command", Instant.ofEpochSecond(1700000000L), 5.millis, 20.millis)))
    )
  }

  test("WAITAOF decodes the [numlocal, numreplicas] pair; WAIT/WAITAOF ride the multiplexed connection and broadcast per master") {
    assertEquals(Reply.run(Server.waitAof(1L, 0L, 1.second), Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(2L)))), Right((1L, 2L)))
    assert(!Server.waitAof(1L, 0L, 1.second).isBlocking)
    assert(!Server.waitReplicas(1L, 1.second).isBlocking)
    assert(Server.waitAof(1L, 0L, 1.second).allMasters)
    assert(Server.waitReplicas(1L, 1.second).allMasters)
  }

  private def fold(command: Command[?]): (Frame, Frame) => Frame =
    command.broadcast match {
      case BroadcastReduce.Fold(f) => f
      case other                   => fail(s"expected a Fold broadcast, got $other")
    }

  test("WAIT/WAITAOF fold differing multi-master replies to the weakest shard") {
    val waitFold = fold(Server.waitReplicas(1L, 1.second))
    assertEquals(waitFold(Frame.Integer(2L), Frame.Integer(1L)), Frame.Integer(1L))
    assertEquals(waitFold(Frame.Integer(0L), Frame.Integer(3L)), Frame.Integer(0L))
    assertEquals(Reply.run(Server.waitReplicas(1L, 1.second), Frame.Integer(1L)), Right(1L))

    val aofFold = fold(Server.waitAof(1L, 0L, 1.second))
    assertEquals(
      aofFold(Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(2L))), Frame.Array(Vector(Frame.Integer(0L), Frame.Integer(3L)))),
      Frame.Array(Vector(Frame.Integer(0L), Frame.Integer(2L)))
    )
  }

  test("WAIT/WAITAOF folds pass a malformed reply through in either operand position, so it never hides behind a valid one") {
    val waitFold = fold(Server.waitReplicas(1L, 1.second))
    val badInt   = Frame.SimpleString("nonsense")
    assertEquals(waitFold(Frame.Integer(2L), badInt), badInt)
    assertEquals(waitFold(badInt, Frame.Integer(2L)), badInt)
    assert(Reply.run(Server.waitReplicas(1L, 1.second), waitFold(Frame.Integer(2L), badInt)).isLeft)

    val aofFold   = fold(Server.waitAof(1L, 0L, 1.second))
    val validPair = Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(2L)))
    val badPair   = Frame.Array(Vector(Frame.Integer(1L)))
    assertEquals(aofFold(validPair, badPair), badPair)
    assertEquals(aofFold(badPair, validPair), badPair)
    assert(Reply.run(Server.waitAof(1L, 0L, 1.second), aofFold(validPair, badPair)).isLeft)
  }

  test("MEMORY USAGE decodes a present count and a missing key as None, and is keyed") {
    assertEquals(Reply.run(Server.memoryUsage("k"), Frame.Integer(64L)), Right(Some(64L)))
    assertEquals(Reply.run(Server.memoryUsage("k"), Frame.Null), Right(None))
    assertEquals(Server.memoryUsage("k").keyIndices, Vector(1))
  }

  test("COMMAND INFO decodes the classic fields and drops null (unknown) entries") {
    val reply = Frame.Array(
      Vector(
        Frame.Array(
          Vector(
            bulk("get"),
            Frame.Integer(2L),
            Frame.Array(Vector(bulk("readonly"), bulk("fast"))),
            Frame.Integer(1L),
            Frame.Integer(1L),
            Frame.Integer(1L),
            Frame.Array(Vector(bulk("@read")))
          )
        ),
        Frame.Null
      )
    )
    assertEquals(
      Reply.run(Server.commandInfo("get", "nope"), reply),
      Right(Vector(CommandInfo("get", 2L, Set("readonly", "fast"), 1, 1, 1, Set("@read"))))
    )
  }

  test("COMMAND GETKEYSANDFLAGS decodes key/flag pairs") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(bulk("k"), Frame.Array(Vector(bulk("RW"), bulk("access")))))))
    assertEquals(Reply.run(Server.commandGetKeysAndFlags("SET", "k", "v"), reply), Right(Vector("k" -> Set("RW", "access"))))
  }
}

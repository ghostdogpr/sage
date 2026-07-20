package sage.client

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import kyo.compat.*

import sage.Bytes
import sage.SageException.{ConnectionLost, CrossSlot, DecodeError, NotConnected, ServerError}
import sage.client.internal.{ClusterLive, CountingScheduler, FakeTransport, MultiplexedConnection, Scheduler}
import sage.cluster.{Node, Slot}
import sage.commands.{BroadcastReduce, Command, Connection, Keys, Scripting, Server, Strings}
import sage.protocol.Frame

class ClusterClientSpec extends munit.FunSuite {

  private given ExecutionContext = munitExecutionContext

  private val nodeA    = Node("a", 6379)
  private val nodeB    = Node("b", 6379)
  private val nodeDead = Node("dead", 6379)

  private val helloReply: Frame =
    Frame.Map(
      Vector(
        Frame.BulkString(Bytes.utf8("server"))  -> Frame.BulkString(Bytes.utf8("redis")),
        Frame.BulkString(Bytes.utf8("version")) -> Frame.BulkString(Bytes.utf8("8.0.0")),
        Frame.BulkString(Bytes.utf8("proto"))   -> Frame.Integer(3),
        Frame.BulkString(Bytes.utf8("role"))    -> Frame.BulkString(Bytes.utf8("master"))
      )
    )

  private def nodeFrame(node: Node): Frame =
    Frame.Array(Vector(Frame.BulkString(Bytes.utf8(node.host)), Frame.Integer(node.port.toLong), Frame.BulkString(Bytes.utf8(s"${node.host}-id"))))

  private def slotsFrame(ranges: (Node, Int, Int)*): Frame =
    Frame.Array(ranges.toVector.map { case (node, start, end) =>
      Frame.Array(Vector(Frame.Integer(start.toLong), Frame.Integer(end.toLong), nodeFrame(node)))
    })

  private def subscribed(kind: String, channel: String): Frame =
    Frame.Push(Vector(Frame.BulkString(Bytes.utf8(kind)), Frame.BulkString(Bytes.utf8(channel)), Frame.Integer(1)))

  /**
    * A cluster of fake per-node transports. `behaviour` scripts each node's reply to a written command (HELLO and CLUSTER SLOTS are
    * answered here); `written(node)` exposes what reached that node so routing can be asserted.
    */
  final private class Fixture(
    behaviour: (Node, String) => Seq[Frame],
    seeds: Vector[Node],
    unreachable: Set[Node] = Set.empty,
    readFrom: ReadFrom = ReadFrom.Master,
    connectGate: (Node, Int) => Unit = (_, _) => (), // blocks a node's nth transport creation, to park an establish mid-flight
    scheduler: Scheduler = Scheduler.real
  ) {

    // accumulate every transport per node (a node has both a Multiplexed and, once a transaction pins, a Dedicated connection) so a refresh
    // issued on one is still observable even after the other is created
    private val transports = mutable.Map.empty[Node, mutable.ArrayBuffer[FakeTransport]]

    // nodes whose *next* HELLO fails and then clears: simulates an establish that fails once mid-recovery, so a re-home must retry to converge
    val flakyHello = mutable.Set.empty[Node]

    private def transportsOf(node: Node): Vector[FakeTransport] =
      transports.synchronized(transports.get(node).map(_.toVector).getOrElse(Vector.empty))

    private def allTransports: Vector[FakeTransport] = transports.synchronized(transports.values.flatten.toVector)

    private val factory: Node => MultiplexedConnection.TransportFactory = node =>
      (onFrame, onClosed) => {
        connectGate(node, transportsOf(node).size)
        val respond: Bytes => Seq[Frame] = payload => {
          val text = payload.asUtf8String
          if (text.contains("HELLO"))
            if (unreachable(node) || flakyHello.remove(node)) Seq(Frame.SimpleError("ERR node is down")) else Seq(helloReply)
          else behaviour(node, text)
        }
        val transport                    = new FakeTransport(onFrame, onClosed, respond)
        transports.synchronized { val _ = transports.getOrElseUpdate(node, mutable.ArrayBuffer.empty) += transport }
        transport
      }

    val live: ClusterLive =
      new ClusterLive(
        factory,
        scheduler,
        Vector(Connection.hello(None)),
        BackoffConfig(),
        WatchdogConfig(enabled = false),
        1.second,
        Duration.Zero,
        DedicatedPoolConfig(),
        ClusterConfig(),
        1024,
        seeds,
        readFrom
      )

    live.bootstrapTopology()

    def written(node: Node): Vector[String] = transportsOf(node).flatMap(_.written.map(_.asUtf8String))

    // simulate the server dropping a node's Sharded Subscription Connection (the post-migration disconnect): close the transport that
    // carried the SSUBSCRIBE so its onClosed fires and the manager re-homes
    def dropShardConn(node: Node): Unit =
      transportsOf(node).find(_.written.exists(_.asUtf8String.contains("SSUBSCRIBE"))).foreach(_.close())

    // close the master's classic Subscription Connection so its onClosed fires and the manager re-homes
    def dropClassicConn(): Unit =
      allTransports.find(_.written.exists(_.asUtf8String.contains("\r\nSUBSCRIBE\r\n"))).foreach(_.close())

    def drop(node: Node): Unit = transportsOf(node).foreach(_.close())

    def deliver(node: Node, frame: Frame): Unit = transportsOf(node).lastOption.foreach(_.emit(frame))
  }

  private def awaitWritten(fixture: Fixture, node: Node, token: String): Unit = {
    val deadline = System.nanoTime() + 2000000000L
    while (!fixture.written(node).exists(_.contains(token)) && System.nanoTime() < deadline) Thread.sleep(5)
    assert(fixture.written(node).exists(_.contains(token)), s"$node never received $token")
  }

  private def wholeClusterOn(node: Node): Frame = slotsFrame((node, 0, Slot.Count - 1))

  test("routes a single-key command to the slot's owner") {
    // nodeA owns the lower half, nodeB the upper half
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else Seq(Frame.BulkString(Bytes.utf8(node.host)))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    val key   = "foo"
    val owner = if (Slot.of(Bytes.utf8(key)).value < mid) nodeA else nodeB
    val other = if (owner == nodeA) nodeB else nodeA

    fixture.live.run(Strings.get[String, String](key)).unsafeRun.map { result =>
      assertEquals(result, Some(owner.host))
      assert(fixture.written(owner).exists(_.contains("GET")), "owner did not receive GET")
      assert(!fixture.written(other).exists(_.contains("GET")), "non-owner received GET")
    }
  }

  test("a command to an established node dispatches inline, with no zero-delay scheduler hop") {
    val counting  = new CountingScheduler
    val behaviour =
      (node: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.BulkString(Bytes.utf8(node.host)))
    val fixture   = new Fixture(behaviour, Vector(nodeA), scheduler = counting)

    val before = counting.zeroDelays.get()
    fixture.live.run(Strings.get[String, String]("foo")).unsafeRun.map { result =>
      assertEquals(result, Some(nodeA.host))
      assertEquals(counting.zeroDelays.get(), before, "an established-node dispatch must not offload")
    }
  }

  test("a pipeline to an established node dispatches inline, with no zero-delay scheduler hop") {
    val counting  = new CountingScheduler
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else Seq(Frame.BulkString(Bytes.utf8("v1")), Frame.BulkString(Bytes.utf8("v2")))
    val fixture   = new Fixture(behaviour, Vector(nodeA), scheduler = counting)

    val before = counting.zeroDelays.get()
    fixture.live.pipeline((Strings.get[String, String]("{x}1"), Strings.get[String, String]("{x}2"))).unsafeRun.map { result =>
      assertEquals(result, (Some("v1"), Some("v2")))
      assertEquals(counting.zeroDelays.get(), before, "an established-node batch must not offload")
    }
  }

  test("a broadcast to established masters dispatches inline, with no zero-delay scheduler hop") {
    val counting  = new CountingScheduler
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("DBSIZE")) Seq(Frame.Integer(if (node == nodeA) 2L else 3L))
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA), scheduler = counting)

    fixture.live.run(Server.dbSize).unsafeRun.flatMap { _ =>
      val before = counting.zeroDelays.get()
      fixture.live.run(Server.dbSize).unsafeRun.map { total =>
        assertEquals(total, 5L)
        assertEquals(counting.zeroDelays.get(), before, "a broadcast to established masters must not offload")
      }
    }
  }

  test("a warmed read-only pipeline under ReadFrom.Replica dispatches inline, with no zero-delay scheduler hop") {
    val counting  = new CountingScheduler
    val nodeR     = Node("r", 6379)
    def slots     =
      Frame.Array(Vector(Frame.Array(Vector(Frame.Integer(0L), Frame.Integer((Slot.Count - 1).toLong), nodeFrame(nodeA), nodeFrame(nodeR)))))
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slots)
      else if (text.contains("READONLY")) Seq(Frame.SimpleString("OK"))
      else Seq(Frame.BulkString(Bytes.utf8("v1")), Frame.BulkString(Bytes.utf8("v2")))
    val fixture   = new Fixture(behaviour, Vector(nodeA), readFrom = ReadFrom.Replica, scheduler = counting)

    val pipe = fixture.live.pipeline((Strings.get[String, String]("{x}1"), Strings.get[String, String]("{x}2")))
    pipe.unsafeRun.flatMap { _ =>
      val before = counting.zeroDelays.get()
      pipe.unsafeRun.map { result =>
        assertEquals(result, (Some("v1"), Some("v2")))
        assertEquals(counting.zeroDelays.get(), before, "a warmed replica pipeline must not offload")
      }
    }
  }

  test("KEYS broadcasts to every slot-owning master and concatenates the per-node slices") {
    // nodeA owns the lower half, nodeB the upper half; KEYS is node-local, so each returns only its own keys
    val mid             = Slot.Count / 2
    val aKeys           = Vector("a1", "a2")
    val bKeys           = Vector("b1")
    def bulk(s: String) = Frame.BulkString(Bytes.utf8(s))
    val behaviour       = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("KEYS")) Seq(Frame.Array((if (node == nodeA) aKeys else bKeys).map(bulk)))
      else Seq(Frame.Null)
    val fixture         = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Keys.keys[String]("*")).unsafeRun.map { result =>
      assertEquals(result.toSet, (aKeys ++ bKeys).toSet)
      assert(fixture.written(nodeA).exists(_.contains("KEYS")), "nodeA did not receive KEYS")
      assert(fixture.written(nodeB).exists(_.contains("KEYS")), "nodeB did not receive KEYS")
    }
  }

  test("WAIT broadcasts to every slot-owning master and returns the weakest shard's count") {
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("WAIT")) Seq(Frame.Integer(if (node == nodeA) 2L else 1L))
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Server.waitReplicas(1L, 100.millis)).unsafeRun.map { result =>
      assertEquals(result, 1L)
      assert(fixture.written(nodeA).exists(_.contains("WAIT")), "nodeA did not receive WAIT")
      assert(fixture.written(nodeB).exists(_.contains("WAIT")), "nodeB did not receive WAIT")
    }
  }

  test("WAIT fails when any master fails, rather than reporting a partial durability signal") {
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("WAIT")) Seq(if (node == nodeA) Frame.Integer(2L) else Frame.SimpleError("ERR replication offset unavailable"))
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Server.waitReplicas(1L, 100.millis)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[ServerError], s"expected ServerError, got $error")
    }
  }

  test("WAIT surfaces a malformed master reply rather than hiding it behind a valid one") {
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("WAIT")) Seq(if (node == nodeA) Frame.Integer(2L) else Frame.BulkString(Bytes.utf8("nonsense")))
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Server.waitReplicas(1L, 100.millis)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[DecodeError], s"expected DecodeError, got $error")
    }
  }

  test("WAIT fails when a master's connection cannot be established, not only on a server error") {
    val mid       = Slot.Count / 2
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("WAIT")) Seq(Frame.Integer(2L))
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA), unreachable = Set(nodeB))

    fixture.live.run(Server.waitReplicas(1L, 100.millis)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[NotConnected], s"expected NotConnected, got $error")
    }
  }

  test("a broadcast fold that throws on a reply arriving after dispatch fails the command rather than hanging the caller") {
    val mid       = Slot.Count / 2
    val behaviour =
      (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1))) else Nil
    val fixture   = new Fixture(behaviour, Vector(nodeA))
    val throwing  = Command[Long](
      "WAIT",
      Command.NoKeys,
      Vector(Bytes.utf8("0"), Bytes.utf8("0")),
      _ => Right(0L),
      allMasters = true,
      broadcast = BroadcastReduce.Fold((_, _) => throw new RuntimeException("boom"))
    )

    val running = fixture.live.run(throwing).unsafeRun.failed
    awaitWritten(fixture, nodeA, "WAIT")
    awaitWritten(fixture, nodeB, "WAIT")
    fixture.deliver(nodeA, Frame.Integer(1L))
    fixture.deliver(nodeB, Frame.Integer(1L))
    running.map(error => assertEquals(error.getMessage, "boom"))
  }

  test("WAIT fails when a master's connection drops while the barrier is pending, rather than returning partially or hanging") {
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("WAIT")) if (node == nodeA) Seq(Frame.Integer(2L)) else Nil
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    val running = fixture.live.run(Server.waitReplicas(1L, 5.seconds)).unsafeRun.failed
    awaitWritten(fixture, nodeB, "WAIT")
    fixture.drop(nodeB)
    running.map(error => assert(error.isInstanceOf[ConnectionLost], s"expected ConnectionLost, got $error"))
  }

  test("WAITAOF broadcasts to every slot-owning master and returns component-wise minimums") {
    val mid                               = Slot.Count / 2
    def pair(local: Long, replicas: Long) = Frame.Array(Vector(Frame.Integer(local), Frame.Integer(replicas)))
    val behaviour                         = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("WAITAOF")) Seq(if (node == nodeA) pair(2L, 2L) else pair(1L, 3L))
      else Seq(Frame.Null)
    val fixture                           = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Server.waitAof(1L, 1L, 100.millis)).unsafeRun.map { result =>
      assertEquals(result, (1L, 2L))
      assert(fixture.written(nodeA).exists(_.contains("WAITAOF")), "nodeA did not receive WAITAOF")
      assert(fixture.written(nodeB).exists(_.contains("WAITAOF")), "nodeB did not receive WAITAOF")
    }
  }

  test("DBSIZE broadcasts to every slot-owning master and sums shard counts into the cluster total") {
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("DBSIZE")) Seq(Frame.Integer(if (node == nodeA) 2L else 3L))
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Server.dbSize).unsafeRun.map { result =>
      assertEquals(result, 5L)
      assert(fixture.written(nodeA).exists(_.contains("DBSIZE")), "nodeA did not receive DBSIZE")
      assert(fixture.written(nodeB).exists(_.contains("DBSIZE")), "nodeB did not receive DBSIZE")
    }
  }

  test("DBSIZE fails when any master fails, rather than reporting a partial count") {
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("DBSIZE")) Seq(if (node == nodeA) Frame.Integer(2L) else Frame.SimpleError("ERR unavailable"))
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Server.dbSize).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[ServerError], s"expected ServerError, got $error")
    }
  }

  test("SCRIPT EXISTS broadcasts to every master and reports a sha present only when every master has it") {
    val mid                = Slot.Count / 2
    def flags(bits: Long*) = Frame.Array(bits.map(Frame.Integer(_)).toVector)
    val behaviour          = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("SCRIPT")) Seq(if (node == nodeA) flags(1L, 1L) else flags(1L, 0L))
      else Seq(Frame.Null)
    val fixture            = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Scripting.scriptExists("a", "b")).unsafeRun.map { result =>
      assertEquals(result, Vector(true, false))
      assert(fixture.written(nodeA).exists(_.contains("SCRIPT")), "nodeA did not receive SCRIPT EXISTS")
      assert(fixture.written(nodeB).exists(_.contains("SCRIPT")), "nodeB did not receive SCRIPT EXISTS")
    }
  }

  test("SCRIPT EXISTS fails when any master fails, rather than reporting a partial answer") {
    val mid       = Slot.Count / 2
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (nodeB, mid, Slot.Count - 1)))
      else if (text.contains("SCRIPT"))
        Seq(if (node == nodeA) Frame.Array(Vector(Frame.Integer(1L))) else Frame.SimpleError("ERR unavailable"))
      else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Scripting.scriptExists("a")).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[ServerError], s"expected ServerError, got $error")
    }
  }

  test("under a Replica policy an eligible read routes to the shard's replica, which gets READONLY at setup") {
    val nodeR                   = Node("r", 6379)
    // CLUSTER SLOTS lists nodeR as nodeA's replica for the whole keyspace
    def slotsWithReplica: Frame =
      Frame.Array(Vector(Frame.Array(Vector(Frame.Integer(0L), Frame.Integer((Slot.Count - 1).toLong), nodeFrame(nodeA), nodeFrame(nodeR)))))
    val behaviour               = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsWithReplica)
      else if (text.contains("READONLY")) Seq(Frame.SimpleString("OK"))
      else Seq(Frame.BulkString(Bytes.utf8(if (node == nodeR) "from-replica" else "from-master")))
    val fixture                 = new Fixture(behaviour, Vector(nodeA), readFrom = ReadFrom.Replica)

    fixture.live.run(Strings.get[String, String]("foo")).unsafeRun.map { result =>
      assertEquals(result, Some("from-replica"))
      assert(fixture.written(nodeR).exists(_.contains("READONLY")), "replica connection did not issue READONLY at setup")
      assert(fixture.written(nodeR).exists(_.contains("GET")), "replica did not receive the read")
      assert(!fixture.written(nodeA).exists(_.contains("GET")), "master received a read it should not have")
    }
  }

  test("under a Replica policy a write still goes to the master") {
    val nodeR     = Node("r", 6379)
    def slots     =
      Frame.Array(Vector(Frame.Array(Vector(Frame.Integer(0L), Frame.Integer((Slot.Count - 1).toLong), nodeFrame(nodeA), nodeFrame(nodeR)))))
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(slots) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA), readFrom = ReadFrom.Replica)

    fixture.live.run(Strings.set[String, String]("foo", "bar")).unsafeRun.map { _ =>
      assert(fixture.written(nodeA).exists(_.contains("SET")), "master did not receive the write")
      assert(!fixture.written(nodeR).exists(_.contains("SET")), "replica received a write")
    }
  }

  test("follows a MOVED redirect to the named node and refreshes") {
    // topology says nodeA owns everything, but nodeA reports the slot has MOVED to nodeB
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (node == nodeA) Seq(Frame.SimpleError(s"MOVED ${Slot.of(Bytes.utf8("foo")).value} b:6379"))
      else Seq(Frame.BulkString(Bytes.utf8("from-b")))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.get[String, String]("foo")).unsafeRun.map { result =>
      assertEquals(result, Some("from-b"))
      assert(fixture.written(nodeB).exists(_.contains("GET")), "redirect target did not receive GET")
    }
  }

  test("follows an ASK redirect with an ASKING prefix on the target") {
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (node == nodeA) Seq(Frame.SimpleError(s"ASK ${Slot.of(Bytes.utf8("foo")).value} b:6379"))
      else Seq(Frame.SimpleString("OK"), Frame.BulkString(Bytes.utf8("asked"))) // ASKING reply, then the command's
    val fixture = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.get[String, String]("foo")).unsafeRun.map { result =>
      assertEquals(result, Some("asked"))
      val toB = fixture.written(nodeB).filterNot(_.contains("HELLO"))
      assert(toB.exists(p => p.contains("ASKING") && p.contains("GET")), s"ASKING did not precede the command: $toB")
    }
  }

  test("an unsupported multi-key command whose keys span slots fails CrossSlot") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.Integer(1))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mSetNx("{a}" -> "a", "{b}" -> "b")).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[CrossSlot], s"unexpected error: $error")
    }
  }

  test("an MSET-named custom command without alternating key/value arguments is not rewritten") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))
    val malformed = Command[Unit](
      "MSET",
      Vector(0, 3),
      Vector("{a}", "a-value", "option", "{b}", "b-value").map(Bytes.utf8),
      _ => Right(())
    )

    fixture.live.run(malformed).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[CrossSlot], s"unexpected error: $error")
      assert(!fixture.written(nodeA).exists(_.contains("MSET")), "an unvalidated custom shape reached the wire")
    }
  }

  test("an MGET-named custom command with non-key arguments is not rewritten") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.Array(Vector.empty))
    val fixture   = new Fixture(behaviour, Vector(nodeA))
    val malformed = Command[Vector[String]](
      "MGET",
      Vector(0, 2),
      Vector("{a}", "not-a-key", "{b}").map(Bytes.utf8),
      _ => Right(Vector.empty)
    )

    fixture.live.run(malformed).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[CrossSlot], s"unexpected error: $error")
      assert(!fixture.written(nodeA).exists(_.contains("MGET")), "an unvalidated custom shape reached the wire")
    }
  }

  test("a cross-slot MGET splits by exact slot on one node and restores positional results") {
    val keyA      = "{mget-a}value"
    val keyB      = "{mget-b}value"
    val missingA  = "{mget-a}missing"
    assert(Slot.of(Bytes.utf8(keyA)) != Slot.of(Bytes.utf8(keyB)), "test keys must hash to different slots")
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains(keyA))
        Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("a-value")), Frame.Null)))
      else if (text.contains(keyB))
        Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("b-value")), Frame.BulkString(Bytes.utf8("b-value")))))
      else Seq(Frame.SimpleError("ERR unexpected command"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mGet[String, String](keyA, keyB, missingA, keyB)).unsafeRun.map { result =>
      assertEquals(result, Vector(Some("a-value"), Some("b-value"), None, Some("b-value")))
      val subMgets = fixture.written(nodeA).filter(_.contains("MGET"))
      assertEquals(subMgets.size, 2, "different slots must remain separate even when one node owns both")
      assert(subMgets.forall(text => !(text.contains(keyA) && text.contains(keyB))), s"a subgroup crossed slots: $subMgets")
    }
  }

  test("a lowercase supported command name is transparently split") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else Seq(Frame.Integer(1L))
    val fixture   = new Fixture(behaviour, Vector(nodeA))
    val lowercase = Keys.exists(keyA, keyB).copy(name = "exists")

    fixture.live.run(lowercase).unsafeRun.map { result =>
      assertEquals(result, 2L)
      assertEquals(fixture.written(nodeA).count(_.contains("\r\nexists\r\n")), 2)
    }
  }

  test("a single-slot MGET keeps the one-command fast path") {
    val first     = "{same}one"
    val second    = "{same}two"
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("one")), Frame.BulkString(Bytes.utf8("two")))))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mGet[String, String](first, second)).unsafeRun.map { result =>
      assertEquals(result, Vector(Some("one"), Some("two")))
      val mgets = fixture.written(nodeA).filter(_.contains("MGET"))
      assertEquals(mgets.size, 1)
      assert(mgets.head.contains(first) && mgets.head.contains(second), s"same-slot keys were split: $mgets")
    }
  }

  test("a cross-slot MSET keeps values paired while splitting by exact slot") {
    val firstA    = "{mset-a}one"
    val keyB      = "{mset-b}one"
    val secondA   = "{mset-a}two"
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mSet(firstA -> "first", keyB -> "other", secondA -> "second")).unsafeRun.map { _ =>
      val subMsets = fixture.written(nodeA).filter(_.contains("\r\nMSET\r\n"))
      assertEquals(subMsets.size, 2, "different slots must remain separate even when one node owns both")
      val groupA   = subMsets.find(_.contains(firstA)).getOrElse(fail(s"missing first slot subgroup: $subMsets"))
      val groupB   = subMsets.find(_.contains(keyB)).getOrElse(fail(s"missing second slot subgroup: $subMsets"))
      assert(groupA.contains("first") && groupA.contains(secondA) && groupA.contains("second"), s"values lost pairing: $groupA")
      assert(groupB.contains("other"), s"value lost pairing: $groupB")
      assert(!groupA.contains(keyB) && !groupB.contains(firstA), s"an MSET subgroup crossed slots: $subMsets")
    }
  }

  test("a single-slot MSET keeps the one-command fast path") {
    val first     = "{same}one"
    val second    = "{same}two"
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mSet(first -> "one", second -> "two")).unsafeRun.map { _ =>
      val msets = fixture.written(nodeA).filter(_.contains("\r\nMSET\r\n"))
      assertEquals(msets.size, 1)
      assert(msets.head.contains(first) && msets.head.contains(second), s"same-slot pairs were split: $msets")
    }
  }

  test("a cross-slot MGET routes slot groups independently to their owners") {
    val keyA      = "{a}"
    val keyB      = "{b}"
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(splitOn(Slot.of(Bytes.utf8(keyB)).value))
      else Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8(node.host)))))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mGet[String, String](keyA, keyB)).unsafeRun.map { result =>
      assertEquals(result, Vector(Some(nodeA.host), Some(nodeB.host)))
      assert(fixture.written(nodeA).exists(text => text.contains("MGET") && text.contains(keyA)), "nodeA missed its slot group")
      assert(fixture.written(nodeB).exists(text => text.contains("MGET") && text.contains(keyB)), "nodeB missed its slot group")
    }
  }

  test("a cross-slot MGET fails as a whole when one slot group fails") {
    val keyA      = "{a}"
    val keyB      = "{b}"
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(splitOn(Slot.of(Bytes.utf8(keyB)).value))
      else if (node == nodeB) Seq(Frame.SimpleError("ERR subgroup unavailable"))
      else Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("ok")))))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mGet[String, String](keyA, keyB)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[ServerError], s"expected ServerError, got $error")
      assert(fixture.written(nodeA).exists(_.contains("MGET")), "the successful group was not issued")
      assert(fixture.written(nodeB).exists(_.contains("MGET")), "the failing group was not issued")
    }
  }

  test("a cross-slot MGET rejects a subgroup reply with the wrong positional length") {
    val keyA      = "{a}"
    val keyB      = "{b}"
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains(keyA)) Seq(Frame.Array(Vector.empty))
      else Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("b")))))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mGet[String, String](keyA, keyB)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[DecodeError], s"expected DecodeError, got $error")
    }
  }

  test("a cross-slot MGET follows a redirect for only the affected slot group") {
    val keyA      = "{a}"
    val keyB      = "{b}"
    val slotB     = Slot.of(Bytes.utf8(keyB)).value
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (node == nodeA && text.contains(keyB)) Seq(Frame.SimpleError(s"MOVED $slotB b:6379"))
      else if (text.contains(keyA)) Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("a")))))
      else Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("b")))))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mGet[String, String](keyA, keyB)).unsafeRun.map { result =>
      assertEquals(result, Vector(Some("a"), Some("b")))
      assert(fixture.written(nodeB).exists(text => text.contains("MGET") && text.contains(keyB)), "redirected subgroup missed nodeB")
      assert(!fixture.written(nodeB).exists(_.contains(keyA)), "unaffected subgroup followed the other slot's redirect")
    }
  }

  Vector("DEL", "EXISTS", "TOUCH", "UNLINK").foreach { name =>
    test(s"a cross-slot $name splits by exact slot and sums subgroup counts") {
      val firstA    = "{sum-a}one"
      val keyB      = "{sum-b}one"
      val secondA   = "{sum-a}two"
      val behaviour = (_: Node, text: String) =>
        if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
        else Seq(Frame.Integer(if (text.contains(secondA)) 2L else 1L))
      val fixture   = new Fixture(behaviour, Vector(nodeA))
      val command   = name match {
        case "DEL"    => Keys.del(firstA, keyB, secondA)
        case "EXISTS" => Keys.exists(firstA, keyB, secondA)
        case "TOUCH"  => Keys.touch(firstA, keyB, secondA)
        case "UNLINK" => Keys.unlink(firstA, keyB, secondA)
        case other    => fail(s"unexpected command: $other")
      }

      fixture.live.run(command).unsafeRun.map { result =>
        assertEquals(result, 3L)
        val subcommands = fixture.written(nodeA).filter(_.contains(s"\r\n$name\r\n"))
        assertEquals(subcommands.size, 2, "different slots must remain separate even when one node owns both")
        assert(subcommands.forall(text => !(text.contains(firstA) && text.contains(keyB))), s"a subgroup crossed slots: $subcommands")
      }
    }
  }

  test("a summed cross-slot command fails as a whole when one subgroup fails") {
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(splitOn(slotB))
      else if (node == nodeB) Seq(Frame.SimpleError("ERR subgroup unavailable"))
      else Seq(Frame.Integer(1L))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Keys.del(keyA, keyB)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[ServerError], s"expected ServerError, got $error")
      assert(fixture.written(nodeA).exists(_.contains("DEL")), "the successful subgroup was not issued")
      assert(fixture.written(nodeB).exists(_.contains("DEL")), "the failing subgroup was not issued")
    }
  }

  test("a summed cross-slot command rejects a non-integer subgroup reply") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.Array(Vector.empty))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Keys.exists(keyA, keyB)).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[DecodeError], s"expected DecodeError, got $error")
    }
  }

  test("a cross-slot MSET fails as a whole after issuing every subgroup") {
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(splitOn(slotB))
      else if (node == nodeB) Seq(Frame.SimpleError("ERR subgroup unavailable"))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mSet(keyA -> "a", keyB -> "b")).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[ServerError], s"expected ServerError, got $error")
      assert(fixture.written(nodeA).exists(_.contains("MSET")), "the successful subgroup was not issued")
      assert(fixture.written(nodeB).exists(_.contains("MSET")), "the failing subgroup was not issued")
    }
  }

  test("a cross-slot MSET rejects a subgroup reply other than OK") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains(keyA)) Seq(Frame.SimpleString("OK"))
      else Seq(Frame.Integer(1L))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.run(Strings.mSet(keyA -> "a", keyB -> "b")).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[DecodeError], s"expected DecodeError, got $error")
    }
  }

  test("absorbs a failover: an unreachable owner triggers a refresh and the command retries on the new master") {
    val key                  = "foo"
    val mid                  = Slot.of(Bytes.utf8(key)).value // the upper range starts here, so `key` lands in it
    @volatile var failedOver = false
    val behaviour            = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(slotsFrame((nodeA, 0, mid - 1), (if (failedOver) nodeB else nodeDead, mid, Slot.Count - 1)))
      else Seq(Frame.BulkString(Bytes.utf8(node.host)))
    val fixture              = new Fixture(behaviour, Vector(nodeA), unreachable = Set(nodeDead))

    failedOver = true // the master for `key` has been replaced by nodeB; the stale topology still points at the dead node
    fixture.live.run(Strings.get[String, String](key)).unsafeRun.map { result =>
      assertEquals(result, Some(nodeB.host))
      assert(fixture.written(nodeB).exists(_.contains("GET")), "command did not retry on the promoted master")
    }
  }

  test("close is terminal: a command after close fails rather than reconnecting") {
    val behaviour =
      (node: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.BulkString(Bytes.utf8(node.host)))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.close.unsafeRun.flatMap { _ =>
      fixture.live.run(Strings.get[String, String]("foo")).unsafeRun.failed.map { error =>
        assert(error.isInstanceOf[NotConnected], s"expected NotConnected after close, got: $error")
      }
    }
  }

  // nodeB owns only `slot`; nodeA owns the rest. A key hashing to `slot` routes to nodeB, any other key to nodeA.
  private def splitOn(slot: Int): Frame = {
    val ranges = mutable.ArrayBuffer.empty[(Node, Int, Int)]
    if (slot > 0) ranges += ((nodeA, 0, slot - 1))
    ranges += ((nodeB, slot, slot))
    if (slot < Slot.Count - 1) ranges += ((nodeA, slot + 1, Slot.Count - 1))
    slotsFrame(ranges.toVector*)
  }

  private val keyA  = "{a}"
  private val keyB  = "{b}"
  private val slotB = Slot.of(Bytes.utf8(keyB)).value

  test("a cross-slot pipeline splits per node and merges in submission order") {
    assert(Slot.of(Bytes.utf8(keyA)).value != slotB, "test keys must hash to different slots")
    // each node answers a GET with its own host, so the merged result reveals which node served which position
    val behaviour =
      (node: Node, text: String) => if (text.contains("CLUSTER")) Seq(splitOn(slotB)) else Seq(Frame.BulkString(Bytes.utf8(node.host)))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.pipelineAttempt((Strings.get[String, String](keyA), Strings.get[String, String](keyB))).unsafeRun.map { result =>
      assertEquals(result, (Right(Some(nodeA.host)), Right(Some(nodeB.host))))
      assert(fixture.written(nodeA).exists(_.contains("GET")), "nodeA did not receive its GET")
      assert(fixture.written(nodeB).exists(_.contains("GET")), "nodeB did not receive its GET")
    }
  }

  test("a pipeline transparently executes a cross-slot MGET at its logical position") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("MGET") && text.contains(keyA)) Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("a")))))
      else if (text.contains("MGET") && text.contains(keyB)) Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("b")))))
      else Seq(Frame.BulkString(Bytes.utf8("tail")))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.pipeline((Strings.mGet[String, String](keyA, keyB), Strings.get[String, String]("{a}tail"))).unsafeRun.map { result =>
      assertEquals(result, (Vector(Some("a"), Some("b")), Some("tail")))
    }
  }

  test("a pipeline transparently executes a summed cross-slot command at its logical position") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("EXISTS")) Seq(Frame.Integer(1L))
      else Seq(Frame.BulkString(Bytes.utf8("tail")))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.pipeline((Keys.exists(keyA, keyB), Strings.get[String, String]("{a}tail"))).unsafeRun.map { result =>
      assertEquals(result, (2L, Some("tail")))
    }
  }

  test("a pipeline transparently executes a cross-slot MSET at its logical position") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("MSET")) Seq(Frame.SimpleString("OK"))
      else Seq(Frame.BulkString(Bytes.utf8("tail")))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.pipeline((Strings.mSet(keyA -> "a", keyB -> "b"), Strings.get[String, String]("{a}tail"))).unsafeRun.map { result =>
      assertEquals(result, ((), Some("tail")))
    }
  }

  test("a per-node failure surfaces per-position without poisoning the other node's result") {
    val behaviour = (node: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(splitOn(slotB))
      else if (node == nodeB) Seq(Frame.SimpleError("WRONGTYPE Operation against a key holding the wrong kind of value"))
      else Seq(Frame.BulkString(Bytes.utf8("ok")))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.pipelineAttempt((Strings.get[String, String](keyA), Strings.get[String, String](keyB))).unsafeRun.map { case (first, second) =>
      assertEquals(first, Right(Some("ok")))
      assert(second.fold(_.isInstanceOf[ServerError], _ => false), s"second position should be a ServerError: $second")
    }
  }

  test("a single-slot pipeline routes to one node and returns the typed tuple") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else Seq(Frame.BulkString(Bytes.utf8("v1")), Frame.BulkString(Bytes.utf8("v2")))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.pipeline((Strings.get[String, String]("{x}1"), Strings.get[String, String]("{x}2"))).unsafeRun.map { result =>
      assertEquals(result, (Some("v1"), Some("v2")))
    }
  }

  test("a cross-slot transaction is rejected with CrossSlot, with no MULTI sent") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(splitOn(slotB)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .transaction(tx => tx.watch(keyA).flatMap(_ => tx.run(Strings.get[String, String](keyB))))
      .unsafeRun
      .failed
      .map { error =>
        assert(error.isInstanceOf[CrossSlot], s"unexpected error: $error")
        assert(!fixture.written(nodeB).exists(_.contains("MULTI")), "no MULTI should reach the other slot's node")
      }
  }

  test("a keyless command keeps its submission-order position within a node's batch") {
    // get, ping, get all hash/route to one node; the keyless ping must stay between the two gets on the wire, not be reordered to the end
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else Seq(Frame.BulkString(Bytes.utf8("v1")), Frame.SimpleString("PONG"), Frame.BulkString(Bytes.utf8("v2")))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .pipeline((Strings.get[String, String]("{a}1"), Connection.ping(None), Strings.get[String, String]("{a}2")))
      .unsafeRun
      .map { result =>
        assertEquals(result, (Some("v1"), "PONG", Some("v2")))
        val batch = fixture.written(nodeA).find(_.contains("PING")).getOrElse(fail("no batch reached the node"))
        assert(batch.indexOf("{a}1") < batch.indexOf("PING"), s"ping reordered before the first get: $batch")
        assert(batch.indexOf("PING") < batch.indexOf("{a}2"), s"ping reordered after the second get: $batch")
      }
  }

  test("an ownership fault during a transaction refreshes the topology so a retry can re-pin") {
    val slot      = Slot.of(Bytes.utf8("{a}1")).value
    // the pinned node answers the MULTI/EXEC batch with a MOVED while queuing: the tx fails, but a background refresh must still fire so a
    // retry routes to the new owner instead of looping on the stale node
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("MULTI"))
        Seq(Frame.SimpleString("OK"), Frame.SimpleError(s"MOVED $slot b:6379"), Frame.SimpleError("EXECABORT Transaction discarded"))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .transaction(_.exec(Vector(Strings.get[String, String]("{a}1"))))
      .unsafeRun
      .failed
      .map { _ =>
        Thread.sleep(200) // the refresh is offloaded; give it a moment to issue its CLUSTER SLOTS
        val clusterCalls = fixture.written(nodeA).count(_.contains("CLUSTER"))
        assert(clusterCalls >= 2, s"a MOVED in EXEC should trigger a topology refresh; CLUSTER calls seen: $clusterCalls")
      }
  }

  test("a transaction fault forces a refresh even inside the throttle window") {
    val slot      = Slot.of(Bytes.utf8("{a}1")).value
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("MULTI"))
        Seq(Frame.SimpleString("OK"), Frame.SimpleError(s"MOVED $slot b:6379"), Frame.SimpleError("EXECABORT discarded"))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA)) // default minRefreshInterval (5s) far exceeds this test's duration

    // two faults within the throttle window: a throttled refresh would run only once (CLUSTER stays at 2), a forced one runs on each
    val tx = fixture.live.transaction(_.exec(Vector(Strings.get[String, String]("{a}1"))))
    for {
      _ <- tx.unsafeRun.failed
      _  = Thread.sleep(150) // let the first forced refresh complete and stamp the throttle window
      _ <- tx.unsafeRun.failed
    } yield {
      Thread.sleep(150)
      val clusterCalls = fixture.written(nodeA).count(_.contains("CLUSTER"))
      assert(clusterCalls >= 3, s"each tx fault must force a refresh despite the throttle; CLUSTER calls: $clusterCalls")
    }
  }

  test("a cross-slot MGET remains forbidden inside a transaction") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(splitOn(slotB)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.transaction(_.run(Strings.mGet[String, String](keyA, keyB))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[CrossSlot], s"unexpected error: $error")
      assert(!fixture.written(nodeA).exists(_.contains("MULTI")), "no MULTI should be sent for a rejected MGET")
      assert(!fixture.written(nodeA).exists(_.contains("MGET")), "no MGET subgroup should escape the transaction")
      assert(!fixture.written(nodeB).exists(_.contains("MGET")), "no MGET subgroup should escape the transaction")
    }
  }

  test("a summed cross-slot command remains forbidden inside a transaction") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(splitOn(slotB)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.transaction(_.run(Keys.exists(keyA, keyB))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[CrossSlot], s"unexpected error: $error")
      assert(!fixture.written(nodeA).exists(_.contains("MULTI")), "no MULTI should be sent for a rejected EXISTS")
      assert(!fixture.written(nodeA).exists(_.contains("EXISTS")), "no EXISTS subgroup should escape the transaction")
      assert(!fixture.written(nodeB).exists(_.contains("EXISTS")), "no EXISTS subgroup should escape the transaction")
    }
  }

  test("a cross-slot MSET remains forbidden inside a transaction") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(splitOn(slotB)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.transaction(_.run(Strings.mSet(keyA -> "a", keyB -> "b"))).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[CrossSlot], s"unexpected error: $error")
      assert(!fixture.written(nodeA).exists(_.contains("MULTI")), "no MULTI should be sent for a rejected MSET")
      assert(!fixture.written(nodeA).exists(_.contains("MSET")), "no MSET subgroup should escape the transaction")
      assert(!fixture.written(nodeB).exists(_.contains("MSET")), "no MSET subgroup should escape the transaction")
    }
  }

  test("an interrupted transaction releases promptly while its first acquire is mid-connect, and the acquired connection is recycled") {
    val gate      = new java.util.concurrent.CountDownLatch(1)
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("MULTI"))
        Seq(Frame.SimpleString("OK"), Frame.SimpleString("QUEUED"), Frame.Array(Vector(Frame.BulkString(Bytes.utf8("v")))))
      else Seq(Frame.SimpleString("OK"))
    // gate the dedicated (second) transport of nodeA so the transaction's first acquire parks mid-connect
    val fixture   = new Fixture(behaviour, Vector(nodeA), connectGate = (node, index) => if (node == nodeA && index == 1) gate.await())

    val started = System.nanoTime()
    CIO.timeout(500.millis)(fixture.live.transaction(_.exec(Vector(Strings.get[String, String]("{a}1"))))).unsafeRun.flatMap { result =>
      val elapsedMs = (System.nanoTime() - started) / 1000000L
      assert(result.isEmpty, s"expected the timeout to win, got $result")
      assert(elapsedMs < 3000L, s"the finalizer stalled behind the parked acquire: ${elapsedMs}ms")
      gate.countDown()
      Thread.sleep(300) // let the orphaned acquire finish and hand its never-used connection back
      fixture.live.transaction(_.exec(Vector(Strings.get[String, String]("{a}1")))).unsafeRun.map { committed =>
        assertEquals(committed, Some(Vector(Some("v"))))
        val hellos = fixture.written(nodeA).count(_.contains("HELLO"))
        assertEquals(hellos, 2, "the recycled dedicated connection must be reused, not re-established")
      }
    }
  }

  test("a single-slot transaction commits in cluster mode") {
    val key       = "{a}1"
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("MULTI"))
        Seq(Frame.SimpleString("OK"), Frame.SimpleString("QUEUED"), Frame.Array(Vector(Frame.BulkString(Bytes.utf8("v")))))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .transaction(tx => tx.watch(key).flatMap(_ => tx.exec(Vector(Strings.get[String, String](key)))))
      .unsafeRun
      .map(result => assertEquals(result, Some(Vector(Some("v")))))
  }

  test("a cluster transaction rejects DBSIZE on the immediate-run path, with no MULTI sent") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .transaction(_.dbSize)
      .unsafeRun
      .failed
      .map { error =>
        assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
        assert(!fixture.written(nodeA).exists(_.contains("DBSIZE")), "a rejected DBSIZE must not reach the wire")
      }
  }

  test("a cluster transaction rejects DBSIZE inside exec, with no MULTI sent") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .transaction(_.exec(Vector(Server.dbSize)))
      .unsafeRun
      .failed
      .map { error =>
        assert(error.isInstanceOf[IllegalArgumentException], s"unexpected error: $error")
        assert(!fixture.written(nodeA).exists(_.contains("MULTI")), "a rejected exec must not open MULTI")
      }
  }

  test("a cluster transaction still accepts WAIT inside exec, whose node-local reply is a valid transaction result") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("MULTI")) Seq(Frame.SimpleString("OK"), Frame.SimpleString("QUEUED"), Frame.Array(Vector(Frame.Integer(1L))))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .transaction(_.exec(Vector(Server.waitReplicas(1L, 100.millis))))
      .unsafeRun
      .map { result =>
        assertEquals(result, Some(Vector(1L)))
        assert(fixture.written(nodeA).exists(_.contains("WAIT")), "WAIT should ride the pinned transaction connection")
      }
  }

  test("a cluster transaction accepts immediate WAIT and WAITAOF on the run path, whose node-local replies are valid results") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("WAITAOF")) Seq(Frame.Array(Vector(Frame.Integer(1L), Frame.Integer(1L))))
      else if (text.contains("WAIT")) Seq(Frame.Integer(1L))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .transaction(tx => tx.run(Server.waitReplicas(1L, 100.millis)).flatMap(_ => tx.run(Server.waitAof(1L, 1L, 100.millis))))
      .unsafeRun
      .map { result =>
        assertEquals(result, (1L, 1L))
        assert(fixture.written(nodeA).exists(_.contains("WAIT\r\n")), "WAIT should ride the pinned transaction connection")
        assert(fixture.written(nodeA).exists(_.contains("WAITAOF")), "WAITAOF should ride the pinned transaction connection")
      }
  }

  test("a cluster transaction still accepts KEYS, an existing broadcast command, returning the pinned node's keys") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("KEYS")) Seq(Frame.Array(Vector(Frame.BulkString(Bytes.utf8("k1")))))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live
      .transaction(_.run(Keys.keys[String]("*")))
      .unsafeRun
      .map { result =>
        assertEquals(result, Vector("k1"))
        assert(fixture.written(nodeA).exists(_.contains("KEYS")), "KEYS should ride the pinned transaction connection")
      }
  }

  test("a sharded subscribe routes SSUBSCRIBE to the channel's slot owner, not other nodes") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(splitOn(slotB))
      else if (text.contains("SSUBSCRIBE")) Seq(subscribed("ssubscribe", keyB))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.subscribeShardChannels[String](keyB).unsafeRun.map { _ =>
      assert(fixture.written(nodeB).exists(_.contains("SSUBSCRIBE")), "owner did not receive SSUBSCRIBE")
      assert(!fixture.written(nodeA).exists(_.contains("SSUBSCRIBE")), "non-owner received SSUBSCRIBE")
    }
  }

  test("sPublish routes by slot to the channel's owner") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(splitOn(slotB))
      else if (text.contains("SPUBLISH")) Seq(Frame.Integer(1))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.sPublish(keyB, "hi").unsafeRun.map { count =>
      assertEquals(count, 1L)
      assert(fixture.written(nodeB).exists(_.contains("SPUBLISH")), "owner did not receive SPUBLISH")
      assert(!fixture.written(nodeA).exists(_.contains("SPUBLISH")), "non-owner received SPUBLISH")
    }
  }

  test("a classic subscribe rides one master connection, coexisting with sharded routing") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(splitOn(slotB))
      else if (text.contains("SUBSCRIBE")) Seq(subscribed("subscribe", "news"))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    fixture.live.subscribeChannels[String]("news").unsafeRun.map { _ =>
      // classic PUBLISH broadcasts cluster-wide, so the subscription pins to one master (the live seed) regardless of any slot
      assert(fixture.written(nodeA).exists(_.contains("\r\nSUBSCRIBE\r\n")), "classic subscribe did not reach a master")
    }
  }

  test("a sharded subscription re-homes to the new owner after its connection drops") {
    @volatile var migrated = false
    val behaviour          = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(if (migrated) wholeClusterOn(nodeA) else splitOn(slotB))
      else if (text.contains("SSUBSCRIBE")) Seq(subscribed("ssubscribe", keyB))
      else Seq(Frame.SimpleString("OK"))
    val fixture            = new Fixture(behaviour, Vector(nodeA))

    fixture.live.subscribeShardChannels[String](keyB).unsafeRun.map { _ =>
      assert(fixture.written(nodeB).exists(_.contains("SSUBSCRIBE")), "initial subscribe did not reach the owner nodeB")
      // slotB migrates to nodeA and nodeB drops the subscriber connection (the server's post-migration disconnect)
      migrated = true
      fixture.dropShardConn(nodeB)
      Thread.sleep(300) // re-homing is offloaded: force a refresh, reconcile, and re-SSUBSCRIBE on the new owner
      assert(fixture.written(nodeA).exists(_.contains("SSUBSCRIBE")), "subscription did not re-home to the new owner nodeA")
    }
  }

  test("a classic subscription recovers when a re-home's establish fails, rather than stranding") {
    val behaviour = (_: Node, text: String) =>
      if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA))
      else if (text.contains("SUBSCRIBE")) Seq(subscribed("subscribe", "news"))
      else Seq(Frame.SimpleString("OK"))
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    def classicSubscribes = fixture.written(nodeA).count(_.contains("\r\nSUBSCRIBE\r\n"))

    fixture.live.subscribeChannels[String]("news").unsafeRun.map { _ =>
      assertEquals(classicSubscribes, 1, "initial classic subscribe did not reach the master")
      // the master drops the classic connection; the first re-home attempt's HELLO fails, so establish throws without firing onTerminated.
      // The old code swallowed that and stranded the sub; the manager must instead retry until it re-attaches.
      fixture.flakyHello += nodeA
      fixture.dropClassicConn()
      Thread.sleep(300) // the failed establish retries after the 50ms backoff, once HELLO succeeds again
      assert(classicSubscribes >= 2, "classic subscription did not recover after the failed re-home")
    }
  }
}

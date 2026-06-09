package sage.client

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import kyo.compat.*

import sage.Bytes
import sage.SageException.{CrossSlot, NotConnected}
import sage.client.internal.{ClusterLive, FakeTransport, MultiplexedConnection, Scheduler}
import sage.cluster.{Node, Slot}
import sage.commands.{Command, Connection, Pipeline, Strings}
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

  /**
    * A cluster of fake per-node transports. `behaviour` scripts each node's reply to a written command (HELLO and CLUSTER SLOTS are
    * answered here); `written(node)` exposes what reached that node so routing can be asserted.
    */
  final private class Fixture(behaviour: (Node, String) => Seq[Frame], seeds: Vector[Node], unreachable: Set[Node] = Set.empty) {

    private val transports = mutable.Map.empty[Node, FakeTransport]

    private val factory: Node => MultiplexedConnection.TransportFactory = node =>
      (onFrame, onClosed) => {
        val respond: Bytes => Seq[Frame] = payload => {
          val text = payload.asUtf8String
          if (text.contains("HELLO")) if (unreachable(node)) Seq(Frame.SimpleError("ERR node is down")) else Seq(helloReply)
          else behaviour(node, text)
        }
        val transport                    = new FakeTransport(onFrame, onClosed, respond)
        transports(node) = transport
        transport
      }

    val live: ClusterLive =
      new ClusterLive(
        factory,
        Scheduler.real,
        Vector(Connection.hello(None)),
        BackoffConfig(),
        WatchdogConfig(enabled = false),
        1.second,
        Duration.Zero,
        DedicatedPoolConfig(),
        ClusterConfig(),
        seeds
      )

    live.bootstrapTopology()

    def written(node: Node): Vector[String] = transports.get(node).toVector.flatMap(_.written.map(_.asUtf8String))
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

  test("a single command whose keys span slots fails CrossSlot") {
    val behaviour                = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.Integer(1))
    val fixture                  = new Fixture(behaviour, Vector(nodeA))
    val crossSlot: Command[Long] =
      Command("MSET", Vector(0, 2), Vector("{a}", "v", "{b}", "v").map(Bytes.utf8), _ => Right(1L))

    fixture.live.run(crossSlot).unsafeRun.failed.map { error =>
      assert(error.isInstanceOf[CrossSlot], s"unexpected error: $error")
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

  test("pipelines and transactions are rejected in cluster mode") {
    val behaviour = (_: Node, text: String) => if (text.contains("CLUSTER")) Seq(wholeClusterOn(nodeA)) else Seq(Frame.Null)
    val fixture   = new Fixture(behaviour, Vector(nodeA))

    val pipelineFailed    = fixture.live.pipeline(Pipeline.sequence(Vector(Strings.get[String, String]("k")))).unsafeRun.failed
    val transactionFailed = fixture.live.transaction(_ => CIO.value(())).unsafeRun.failed

    for {
      p <- pipelineFailed
      t <- transactionFailed
    } yield {
      assert(p.isInstanceOf[UnsupportedOperationException], s"pipeline: $p")
      assert(t.isInstanceOf[UnsupportedOperationException], s"transaction: $t")
    }
  }
}

package sage.client

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import kyo.compat.*

import sage.{Bytes, CommandSpan, CommandTracer, Outcome, SageEvent, SageListener}
import sage.client.internal.{CountingScheduler, Events, FakeTransport, MasterReplicaLive, MultiplexedConnection, Replies, Scheduler}
import sage.client.internal.Replies.ok
import sage.cluster.Node
import sage.commands.{Command, Connection}
import sage.protocol.Frame

class MasterReplicaPipelineSpec extends munit.FunSuite {

  private given ExecutionContext = munitExecutionContext

  private val master  = Node("master-host", 7000)
  private val replica = Node("replica-host", 7001)

  private val roleReply: Frame = Replies.masterRole(replica)

  // one reply per command occurrence in a batch; only the master answers ROLE; other bootstrap commands each get a single OK
  private def respondFor(
    node: Node,
    readFailure: Option[(Node, Frame)],
    readsByNode: ConcurrentLinkedQueue[Node]
  ): Bytes => Seq[Frame] = payload => {
    val s = payload.asUtf8String
    if (s.contains("HELLO")) Seq(Replies.hello)
    else if (s.contains("ROLE")) if (node == master) Seq(roleReply) else Nil
    else {
      val reads  = occurrences(s, "PREAD")
      val writes = occurrences(s, "PWRITE")
      (0 until reads).foreach(_ => readsByNode.add(node))
      if (reads + writes == 0) Seq(ok)
      else
        Seq.fill(reads)(readFailure.collect { case (`node`, failure) => failure }.getOrElse(ok)) ++ Seq.fill(writes)(ok)
    }
  }

  private def occurrences(haystack: String, needle: String): Int = {
    var count = 0
    var from  = haystack.indexOf(needle)
    while (from >= 0) {
      count += 1
      from = haystack.indexOf(needle, from + needle.length)
    }
    count
  }

  final private class RoutingTracer extends CommandTracer {
    val routed                                      = new ConcurrentLinkedQueue[(String, Node)]()
    def onCommand(command: Command[?]): CommandSpan =
      new CommandSpan {
        def routedTo(node: Node): Unit      = routed.add(command.name -> node): Unit
        def settled(outcome: Outcome): Unit = ()
      }
  }

  private val readCmd: Command[Unit]  = Command("PREAD", Command.NoKeys, Vector.empty, (_: Frame) => Right(()), isReadOnly = true)
  private val writeCmd: Command[Unit] = Command("PWRITE", Command.NoKeys, Vector.empty, (_: Frame) => Right(()), isReadOnly = false)

  final private case class Fixture(
    live: MasterReplicaLive,
    completions: ConcurrentLinkedQueue[SageEvent.CommandCompleted],
    tracer: RoutingTracer,
    latch: CountDownLatch,
    readsByNode: ConcurrentLinkedQueue[Node]
  )

  private def build(
    readFrom: ReadFrom,
    scheduler: Scheduler = Scheduler.real,
    readFailure: Option[(Node, Frame)] = None
  ): Fixture = {
    val completions                                             = new ConcurrentLinkedQueue[SageEvent.CommandCompleted]()
    val readsByNode                                             = new ConcurrentLinkedQueue[Node]()
    val latch                                                   = new CountDownLatch(2)
    val listener                                                = new SageListener {
      def onEvent(event: SageEvent): Unit = event match {
        case c: SageEvent.CommandCompleted if c.name == "PREAD" || c.name == "PWRITE" =>
          completions.add(c)
          latch.countDown()
        case _                                                                        => ()
      }
    }
    val tracer                                                  = new RoutingTracer
    val factory: Node => MultiplexedConnection.TransportFactory =
      node => (onFrame, onClosed) => new FakeTransport(onFrame, onClosed, respondFor(node, readFailure, readsByNode))
    val live                                                    =
      new MasterReplicaLive(
        factory,
        scheduler,
        Vector(Connection.hello(None)),
        SageConfig(readFrom = readFrom),
        Vector(master),
        MasterReplicaConfig(1.second),
        Events(Vector(listener), Some(tracer))
      )
    live.bootstrapRoles()
    Fixture(live, completions, tracer, latch, readsByNode)
  }

  test("a command to the established master dispatches inline, with no zero-delay scheduler hop") {
    val counting = new CountingScheduler
    val f        = build(ReadFrom.Master, counting)
    f.live.run(writeCmd).unsafeRun.flatMap { _ =>
      val before = counting.zeroDelays.get()
      f.live.run(writeCmd).unsafeRun.flatMap { _ =>
        f.live.pipeline(Seq(writeCmd, readCmd)).unsafeRun.map { _ =>
          assertEquals(counting.zeroDelays.get(), before, "an established-master dispatch must not offload")
          f.live.close.unsafeRun
        }
      }
    }
  }

  test("a warmed read-only pipeline under ReadFrom.Replica dispatches inline, with no zero-delay scheduler hop") {
    val counting = new CountingScheduler
    val f        = build(ReadFrom.Replica, counting)
    f.live.pipeline(Seq(readCmd, readCmd)).unsafeRun.flatMap { _ =>
      val before = counting.zeroDelays.get()
      f.live.pipeline(Seq(readCmd, readCmd)).unsafeRun.map { _ =>
        assertEquals(counting.zeroDelays.get(), before, "a warmed replica pipeline must not offload")
        f.live.close.unsafeRun
      }
    }
  }

  test("a fully read-only pipeline under ReadFrom.Replica attributes every command to the replica") {
    val f = build(ReadFrom.Replica)
    f.live
      .pipeline(Seq(readCmd, readCmd))
      .unsafeRun
      .map { _ =>
        assert(f.latch.await(2, TimeUnit.SECONDS), "expected a completion per pipeline position")
        val nodes  = f.completions.asScala.toVector.map(_.node)
        assertEquals(nodes, Vector(Some(replica), Some(replica)), s"pipeline commands should attribute to the replica, got $nodes")
        val routed = f.tracer.routed.asScala.toVector.filter(_._1 == "PREAD")
        assertEquals(routed, Vector("PREAD" -> replica, "PREAD" -> replica), s"tracer should route both reads to the replica, got $routed")
        f.live.close.unsafeRun
      }
  }

  test("a pipeline containing a write attributes the whole batch to the master") {
    val f = build(ReadFrom.Replica)
    f.live
      .pipeline(Seq(writeCmd, readCmd))
      .unsafeRun
      .map { _ =>
        assert(f.latch.await(2, TimeUnit.SECONDS), "expected a completion per pipeline position")
        val nodes  = f.completions.asScala.toVector.map(_.node)
        assertEquals(nodes, Vector(Some(master), Some(master)), s"a write forces the whole batch to the master, got $nodes")
        val routed = f.tracer.routed.asScala.toVector.filter(p => p._1 == "PWRITE" || p._1 == "PREAD")
        assertEquals(routed, Vector("PWRITE" -> master, "PREAD" -> master), s"tracer should route both commands to the master, got $routed")
        f.live.close.unsafeRun
      }
  }

  test("a ReplicaPreferred read-only pipeline falls back when the replica cannot serve reads") {
    val f = build(
      ReadFrom.ReplicaPreferred,
      readFailure = Some(replica -> Frame.SimpleError("MASTERDOWN Link with MASTER is down"))
    )
    f.live
      .pipeline(Seq(readCmd, readCmd))
      .unsafeRun
      .map { _ =>
        assert(f.latch.await(2, TimeUnit.SECONDS), "expected a completion per pipeline position")
        assertEquals(f.readsByNode.asScala.toVector, Vector(replica, replica, master, master))
        assertEquals(f.completions.asScala.toVector.map(_.node), Vector(Some(master), Some(master)))
        f.live.close.unsafeRun
      }
  }

  test("a ReplicaPreferred read-only pipeline does not fall back after a command error") {
    val f = build(
      ReadFrom.ReplicaPreferred,
      readFailure = Some(replica -> Frame.SimpleError("WRONGTYPE Operation against a key holding the wrong kind of value"))
    )
    f.live
      .pipelineAttempt(Seq(readCmd, readCmd))
      .unsafeRun
      .map { results =>
        assert(results.forall(_.isLeft), s"expected the replica's command errors, got $results")
        assert(f.latch.await(2, TimeUnit.SECONDS), "expected a completion per pipeline position")
        assertEquals(f.readsByNode.asScala.toVector, Vector(replica, replica))
        assertEquals(f.completions.asScala.toVector.map(_.node), Vector(Some(replica), Some(replica)))
        f.live.close.unsafeRun
      }
  }

  test("a MasterPreferred read-only pipeline falls back when the master cannot serve reads") {
    val f = build(
      ReadFrom.MasterPreferred,
      readFailure = Some(master -> Frame.SimpleError("LOADING Redis is loading the dataset in memory"))
    )
    f.live
      .pipeline(Seq(readCmd, readCmd))
      .unsafeRun
      .map { _ =>
        assert(f.latch.await(2, TimeUnit.SECONDS), "expected a completion per pipeline position")
        assertEquals(f.readsByNode.asScala.toVector, Vector(master, master, replica, replica))
        assertEquals(f.completions.asScala.toVector.map(_.node), Vector(Some(replica), Some(replica)))
        f.live.close.unsafeRun
      }
  }
}

package sage.client

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

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

  private val master   = Node("master-host", 7000)
  private val replica  = Node("replica-host", 7001)
  private val replica2 = Node("replica2-host", 7002)

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

  // one reply per command occurrence in a batch; only the master answers ROLE; other bootstrap commands each get a single OK
  final private class PipelineScript(role: Frame, failure: Option[(Node, Frame)], disconnectOnRead: Option[Node]) {
    val readsByNode = new ConcurrentLinkedQueue[Node]()
    val readBatches = new ConcurrentLinkedQueue[(Node, Int)]()
    val readFailure = new AtomicReference(failure)
    val readReplies = new AtomicReference[Option[(Node, Vector[Frame])]](None)

    val factory: Node => MultiplexedConnection.TransportFactory =
      node =>
        (onFrame, onClosed) => {
          var transport: FakeTransport = null
          transport = new FakeTransport(onFrame, onClosed, respondFor(node, () => transport.close()))
          transport
        }

    private def respondFor(node: Node, disconnect: () => Unit): Bytes => Seq[Frame] = payload => {
      val s = payload.asUtf8String
      if (s.contains("HELLO")) Seq(Replies.hello)
      else if (s.contains("ROLE")) if (node == master) Seq(role) else Nil
      else {
        val reads  = occurrences(s, "PREAD")
        val writes = occurrences(s, "PWRITE")
        (0 until reads).foreach(_ => readsByNode.add(node))
        if (reads > 0) { readBatches.add(node -> reads): Unit }
        if (reads > 0 && disconnectOnRead.contains(node)) {
          disconnect()
          Seq.empty
        } else if (reads + writes == 0) Seq(ok)
        else {
          val scripted =
            readReplies.get().collect { case (`node`, frames) => frames }.getOrElse {
              Vector.fill(reads)(readFailure.get().collect { case (`node`, frame) => frame }.getOrElse(ok))
            }
          scripted ++ Seq.fill(writes)(ok)
        }
      }
    }
  }

  final private case class Fixture(
    live: MasterReplicaLive,
    completions: ConcurrentLinkedQueue[SageEvent.CommandCompleted],
    tracer: RoutingTracer,
    latch: CountDownLatch,
    script: PipelineScript
  ) {
    def readsByNode: ConcurrentLinkedQueue[Node]                    = script.readsByNode
    def readBatches: ConcurrentLinkedQueue[(Node, Int)]             = script.readBatches
    def readFailure: AtomicReference[Option[(Node, Frame)]]         = script.readFailure
    def readReplies: AtomicReference[Option[(Node, Vector[Frame])]] = script.readReplies
  }

  private def build(
    readFrom: ReadFrom,
    scheduler: Scheduler = Scheduler.real,
    readFailure: Option[(Node, Frame)] = None,
    disconnectOnRead: Option[Node] = None,
    includeSecondReplica: Boolean = false
  ): Fixture = {
    val completions = new ConcurrentLinkedQueue[SageEvent.CommandCompleted]()
    val replicas    = if (includeSecondReplica) Vector(replica, replica2) else Vector(replica)
    val role        = Replies.masterRole(replicas*)
    val script      = new PipelineScript(role, readFailure, disconnectOnRead)
    val latch       = new CountDownLatch(2)
    val listener    = new SageListener {
      def onEvent(event: SageEvent): Unit = event match {
        case c: SageEvent.CommandCompleted if c.name == "PREAD" || c.name == "PWRITE" =>
          completions.add(c)
          latch.countDown()
        case _                                                                        => ()
      }
    }
    val tracer      = new RoutingTracer
    val live        =
      new MasterReplicaLive(
        script.factory,
        scheduler,
        Vector(Connection.hello(None)),
        SageConfig(readFrom = readFrom),
        Vector(master),
        MasterReplicaConfig(1.second),
        Events(Vector(listener), Some(tracer))
      )
    live.bootstrapRoles()
    Fixture(live, completions, tracer, latch, script)
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
        assertEquals(f.readBatches.asScala.toVector, Vector(replica -> 2, master -> 2))
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

  test("a terminal pipeline error settles without a scheduler hop") {
    val counting = new CountingScheduler
    val f        = build(ReadFrom.ReplicaPreferred, scheduler = counting)
    f.live.pipeline(Seq(readCmd, readCmd)).unsafeRun.flatMap { _ =>
      f.readFailure.set(Some(replica -> Frame.SimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")))
      val before = counting.zeroDelays.get()
      f.live.pipelineAttempt(Seq(readCmd, readCmd)).unsafeRun.map { results =>
        assert(results.forall(_.isLeft), s"expected terminal command errors, got $results")
        assertEquals(counting.zeroDelays.get(), before, "a terminal server reply must settle without a scheduler hop")
        f.live.close.unsafeRun
      }
    }
  }

  test("a retryable position retries a mixed-result pipeline as one batch") {
    val f = build(ReadFrom.ReplicaPreferred)
    f.readReplies.set(
      Some(
        replica -> Vector(
          Frame.SimpleError("WRONGTYPE Operation against a key holding the wrong kind of value"),
          Frame.SimpleError("MASTERDOWN Link with MASTER is down")
        )
      )
    )
    f.live
      .pipelineAttempt(Seq(readCmd, readCmd))
      .unsafeRun
      .map { results =>
        assert(results.forall(_.isRight), s"the fallback attempt should replace every result from the refused node, got $results")
        assert(f.latch.await(2, TimeUnit.SECONDS), "expected a completion per pipeline position")
        assertEquals(f.readsByNode.asScala.toVector, Vector(replica, replica, master, master))
        assertEquals(f.readBatches.asScala.toVector, Vector(replica -> 2, master -> 2))
        assertEquals(f.completions.asScala.toVector.map(_.node), Vector(Some(master), Some(master)))
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
        assertEquals(f.readBatches.asScala.toVector, Vector(master -> 2, replica -> 2))
        assertEquals(f.completions.asScala.toVector.map(_.node), Vector(Some(replica), Some(replica)))
        f.live.close.unsafeRun
      }
  }

  test("a ReplicaPreferred read-only pipeline falls back when the replica disconnects after submission") {
    val f = build(ReadFrom.ReplicaPreferred, disconnectOnRead = Some(replica))
    f.live
      .pipeline(Seq(readCmd, readCmd))
      .unsafeRun
      .map { _ =>
        assert(f.latch.await(2, TimeUnit.SECONDS), "expected a completion per pipeline position")
        assertEquals(f.readsByNode.asScala.toVector, Vector(replica, replica, master, master))
        assertEquals(f.readBatches.asScala.toVector, Vector(replica -> 2, master -> 2))
        assertEquals(f.completions.asScala.toVector.map(_.node), Vector(Some(master), Some(master)))
        f.live.close.unsafeRun
      }
  }

  test("a strict Replica read-only pipeline falls through to the next replica as one batch") {
    val f = build(
      ReadFrom.Replica,
      readFailure = Some(replica -> Frame.SimpleError("MASTERDOWN Link with MASTER is down")),
      includeSecondReplica = true
    )
    f.live
      .pipeline(Seq(readCmd, readCmd))
      .unsafeRun
      .map { _ =>
        assert(f.latch.await(2, TimeUnit.SECONDS), "expected a completion per pipeline position")
        assertEquals(f.readsByNode.asScala.toVector, Vector(replica, replica, replica2, replica2))
        assertEquals(f.readBatches.asScala.toVector, Vector(replica -> 2, replica2 -> 2))
        assertEquals(f.completions.asScala.toVector.map(_.node), Vector(Some(replica2), Some(replica2)))
        f.live.close.unsafeRun
      }
  }
}

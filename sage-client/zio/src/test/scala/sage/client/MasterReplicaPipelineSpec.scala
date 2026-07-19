package sage.client

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import kyo.compat.*

import sage.{Bytes, CommandSpan, CommandTracer, Outcome, SageEvent, SageListener}
import sage.client.internal.{CountingScheduler, Events, FakeTransport, MasterReplicaLive, MultiplexedConnection, Scheduler}
import sage.cluster.Node
import sage.commands.{Command, Connection}
import sage.protocol.Frame

class MasterReplicaPipelineSpec extends munit.FunSuite {

  private given ExecutionContext = munitExecutionContext

  private val master  = Node("master-host", 7000)
  private val replica = Node("replica-host", 7001)

  private val helloReply: Frame =
    Frame.Map(
      Vector(
        Frame.BulkString(Bytes.utf8("server"))  -> Frame.BulkString(Bytes.utf8("redis")),
        Frame.BulkString(Bytes.utf8("version")) -> Frame.BulkString(Bytes.utf8("8.0.0")),
        Frame.BulkString(Bytes.utf8("proto"))   -> Frame.Integer(3),
        Frame.BulkString(Bytes.utf8("role"))    -> Frame.BulkString(Bytes.utf8("master"))
      )
    )

  private val roleReply: Frame =
    Frame.Array(
      Vector(
        Frame.BulkString(Bytes.utf8("master")),
        Frame.Integer(0L),
        Frame.Array(
          Vector(
            Frame.Array(
              Vector(
                Frame.BulkString(Bytes.utf8(replica.host)),
                Frame.BulkString(Bytes.utf8(replica.port.toString)),
                Frame.BulkString(Bytes.utf8("0"))
              )
            )
          )
        )
      )
    )

  // one reply per command occurrence in a batch; only the master answers ROLE; other bootstrap commands each get a single OK
  private def respondFor(node: Node): Bytes => Seq[Frame] = payload => {
    val s = payload.asUtf8String
    if (s.contains("HELLO")) Seq(helloReply)
    else if (s.contains("ROLE")) if (node == master) Seq(roleReply) else Nil
    else {
      val batch = occurrences(s, "PREAD") + occurrences(s, "PWRITE")
      if (batch > 0) Seq.fill(batch)(Frame.SimpleString("OK")) else Seq(Frame.SimpleString("OK"))
    }
  }

  private def occurrences(haystack: String, needle: String): Int = {
    var count = 0
    var from  = haystack.indexOf(needle)
    while (from >= 0) { count += 1; from = haystack.indexOf(needle, from + needle.length) }
    count
  }

  final private class RoutingTracer extends CommandTracer {
    val routed                                      = new ConcurrentLinkedQueue[(String, Node)]()
    def onCommand(command: Command[?]): CommandSpan =
      new CommandSpan {
        def routedTo(node: Node): Unit      = { val _ = routed.add(command.name -> node) }
        def settled(outcome: Outcome): Unit = ()
      }
  }

  private val readCmd: Command[Unit]  = Command("PREAD", Command.NoKeys, Vector.empty, (_: Frame) => Right(()), isReadOnly = true)
  private val writeCmd: Command[Unit] = Command("PWRITE", Command.NoKeys, Vector.empty, (_: Frame) => Right(()), isReadOnly = false)

  final private case class Fixture(
    live: MasterReplicaLive,
    completions: ConcurrentLinkedQueue[SageEvent.CommandCompleted],
    tracer: RoutingTracer,
    latch: CountDownLatch
  )

  private def build(readFrom: ReadFrom, scheduler: Scheduler = Scheduler.real): Fixture = {
    val completions                                             = new ConcurrentLinkedQueue[SageEvent.CommandCompleted]()
    val latch                                                   = new CountDownLatch(2)
    val listener                                                = new SageListener {
      def onEvent(event: SageEvent): Unit = event match {
        case c: SageEvent.CommandCompleted if c.name == "PREAD" || c.name == "PWRITE" => completions.add(c); latch.countDown()
        case _                                                                        => ()
      }
    }
    val tracer                                                  = new RoutingTracer
    val factory: Node => MultiplexedConnection.TransportFactory =
      node => (onFrame, onClosed) => new FakeTransport(onFrame, onClosed, respondFor(node))
    val live                                                    =
      new MasterReplicaLive(
        factory,
        scheduler,
        Vector(Connection.hello(None)),
        SageConfig(readFrom = readFrom),
        Vector(master),
        1.second,
        Events(Vector(listener), Some(tracer))
      )
    live.bootstrapRoles()
    Fixture(live, completions, tracer, latch)
  }

  test("a command to the established master dispatches inline, with no zero-delay scheduler hop") {
    val counting = new CountingScheduler
    val f        = build(ReadFrom.Master, counting)
    f.live.run(writeCmd).unsafeRun.flatMap { _ =>
      val before = counting.zeroDelays.get()
      f.live.run(writeCmd).unsafeRun.flatMap { _ =>
        f.live.pipeline(Seq(writeCmd, readCmd)).unsafeRun.map { _ =>
          assertEquals(counting.zeroDelays.get(), before, "an established-master dispatch must not offload")
          val _ = f.live.close.unsafeRun
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
        val _ = f.live.close.unsafeRun
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
        val _      = f.live.close.unsafeRun
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
        val _      = f.live.close.unsafeRun
      }
  }
}

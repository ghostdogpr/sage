package sage.client.internal

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import sage.{CommandSpan, CommandTracer, Outcome}
import sage.SageException.{ConnectionLost, NotConnected, ServerError}
import sage.client.ReadFrom
import sage.cluster.{Node, Redirect}
import sage.commands.{Command, Execution}

class ReadPolicySpec extends munit.FunSuite {

  private val master = Node("master", 6379)
  private val r1     = Node("replica-1", 6379)
  private val r2     = Node("replica-2", 6379)

  private val read: Command[String] =
    Command("GET", Command.NoKeys, Vector.empty, _ => Right("unused"), isReadOnly = true)

  private def command(
    name: String,
    execution: Execution = Execution.Ordinary,
    isReadOnly: Boolean = false,
    cursorBound: Boolean = false
  ): Command[Unit] =
    Command(name, Command.NoKeys, Vector.empty, _ => Right(()), execution, isReadOnly = isReadOnly, cursorBound = cursorBound)

  test("eligibility belongs to the Read Policy for single Commands and whole Pipelines") {
    val eligible  = command("GET", isReadOnly = true)
    val cacheable = Command.read("GET", Command.FirstKey, Vector(sage.Bytes.utf8("key")), _ => Right(()))
    val write     = command("SET")
    val blocking  = command("XREAD", Execution.Blocking, isReadOnly = true)
    val cursor    = command("HSCAN", isReadOnly = true, cursorBound = true)

    assert(ReadPolicy.replicaEligible(eligible))
    assert(ReadPolicy.replicaEligible(cacheable), "cacheability does not pin an ordinary run; the cached API bypasses Read Policy")
    assert(!ReadPolicy.replicaEligible(write))
    assert(!ReadPolicy.replicaEligible(blocking))
    assert(!ReadPolicy.replicaEligible(cursor))
    assert(ReadPolicy.replicaEligible(Vector(eligible, eligible)))
    assert(!ReadPolicy.replicaEligible(Vector(eligible, write)))
  }

  test("safe loss falls through to the next candidate and preserves round-robin per source") {
    val scheduler = new ManualScheduler
    val access    = new FakeAccess(
      Map(
        r1 -> new FakeConnection(Vector(Failure(ConnectionLost(mayHaveExecuted = false)))),
        r2 -> new FakeConnection(Vector(Success("from-r2")))
      )
    )
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val results   = mutable.ArrayBuffer.empty[Try[String]]

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), read, redirectsLeft = 0, results += _)
    scheduler.advance(Duration.Zero)

    assertEquals(access.attempted.toVector, Vector(r1, r2))
    assertEquals(results.toVector, Vector(Success("from-r2")))
    assertEquals(topology.fallthroughs.toVector, Vector(r1 -> ConnectionLost(mayHaveExecuted = false)))

    access.attempted.clear()
    access.connections(r1) = new FakeConnection(Vector(Success("from-r1")))
    access.connections(r2) = new FakeConnection(Vector(Success("from-r2")))
    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), read, redirectsLeft = 0, results += _)

    assertEquals(access.attempted.toVector, Vector(r2), "the source-local cursor rotates the first replica")
  }

  test("ReadFrom ordering and strict fallback are executed behind the policy interface") {
    def attempts(readFrom: ReadFrom, replicas: Vector[Node], connections: Map[Node, FakeConnection]): Vector[Node] = {
      val access   = new FakeAccess(connections)
      val topology = new RecordingTopology
      val policy   = new ReadPolicy(readFrom, access, topology, new ManualScheduler)
      policy.submit(ReadPolicy.Source.forMaster(master, replicas), read, redirectsLeft = 0, _ => ())
      access.attempted.toVector
    }

    assertEquals(attempts(ReadFrom.Master, Vector(r1, r2), Map(master -> new FakeConnection(Vector(Success("master"))))), Vector(master))
    assertEquals(
      attempts(
        ReadFrom.MasterPreferred,
        Vector(r1, r2),
        Map(master -> new FakeConnection(Vector.empty, live = false), r1 -> new FakeConnection(Vector(Success("replica"))))
      ),
      Vector(master, r1)
    )
    assertEquals(
      attempts(
        ReadFrom.ReplicaPreferred,
        Vector(r1, r2),
        Map(
          r1     -> new FakeConnection(Vector.empty, live = false),
          r2     -> new FakeConnection(Vector.empty, live = false),
          master -> new FakeConnection(Vector(Success("master")))
        )
      ),
      Vector(r1, r2, master)
    )
    assertEquals(attempts(ReadFrom.Replica, Vector.empty, Map.empty), Vector.empty)
  }

  test("keyless reads have a distinct cursor lane and stale master lanes can be pruned") {
    val access   = new FakeAccess(
      Map(r1 -> new FakeConnection(Vector(Success("r1"))), r2 -> new FakeConnection(Vector(Success("r2"))))
    )
    val topology = new RecordingTopology
    val policy   = new ReadPolicy(ReadFrom.Replica, access, topology, new ManualScheduler)

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), read, redirectsLeft = 0, _ => ())
    access.attempted.clear()
    policy.submit(ReadPolicy.Source.keyless(master, Vector(r1, r2)), read, redirectsLeft = 0, _ => ())
    assertEquals(access.attempted.toVector, Vector(r1), "keyless rotation must not inherit the master's cursor")

    policy.retainSources(Set(master))
    access.attempted.clear()
    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), read, redirectsLeft = 0, _ => ())
    policy.submit(ReadPolicy.Source.keyless(master, Vector(r1, r2)), read, redirectsLeft = 0, _ => ())
    assertEquals(access.attempted.toVector, Vector(r2, r2), "retained master and keyless lanes must preserve their independent cursors")

    policy.retainSources(Set.empty)
    access.attempted.clear()
    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), read, redirectsLeft = 0, _ => ())
    assertEquals(access.attempted.toVector, Vector(r1), "a pruned master lane restarts when that master returns")
  }

  test("ambiguous loss is terminal and never falls through to another Node") {
    val scheduler = new ManualScheduler
    val lost      = ConnectionLost(mayHaveExecuted = true)
    val access    = new FakeAccess(
      Map(
        r1 -> new FakeConnection(Vector(Failure(lost))),
        r2 -> new FakeConnection(Vector(Success("must-not-run")))
      )
    )
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val results   = mutable.ArrayBuffer.empty[Try[String]]

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), read, redirectsLeft = 0, results += _)
    scheduler.advance(Duration.Zero)

    assertEquals(access.attempted.toVector, Vector(r1))
    assertEquals(results.toVector, Vector(Failure(lost)))
    assertEquals(topology.terminals.toVector, Vector(r1 -> lost))
  }

  test("candidate exhaustion is delegated without attributing a Node") {
    val scheduler = new ManualScheduler
    val access    = new FakeAccess(Map(r1 -> new FakeConnection(Vector.empty, live = false)))
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val results   = mutable.ArrayBuffer.empty[Try[String]]

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1)), read, redirectsLeft = 3, results += _)

    assertEquals(topology.exhausted.toVector.map(_._2), Vector(3))
    assert(results.head.failed.get.isInstanceOf[NotConnected])
  }

  test("safe loss remains attributed to its serving Node when a later candidate fails to establish") {
    val scheduler = new ManualScheduler
    val lost      = ConnectionLost(mayHaveExecuted = false)
    val access    = new FakeAccess(Map(r1 -> new FakeConnection(Vector(Failure(lost)))))
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val routed    = mutable.ArrayBuffer.empty[Node]
    val tracer    = new CommandTracer {
      def onCommand(command: Command[?]): CommandSpan = new CommandSpan {
        def routedTo(node: Node): Unit      = routed += node
        def settled(outcome: Outcome): Unit = ()
      }
    }
    val events    = Events(Vector.empty, Some(tracer))
    val results   = mutable.ArrayBuffer.empty[Try[String]]
    val tracked   = Events.trackCommand[String](events, read, result => results += result)

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), read, redirectsLeft = 0, tracked)
    scheduler.advance(Duration.Zero)

    assertEquals(access.attempted.toVector, Vector(r1, r2))
    assertEquals(routed.toVector, Vector(r1))
    assertEquals(results.toVector, Vector(Failure(lost)))
    events.close()
  }

  test("establish failures and dead candidates exhaust once without Node attribution") {
    val scheduler = new ManualScheduler
    val access    = new FakeAccess(Map(r2 -> new FakeConnection(Vector.empty, live = false)))
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val routed    = mutable.ArrayBuffer.empty[Node]
    val tracer    = new CommandTracer {
      def onCommand(command: Command[?]): CommandSpan = new CommandSpan {
        def routedTo(node: Node): Unit      = routed += node
        def settled(outcome: Outcome): Unit = ()
      }
    }
    val events    = Events(Vector.empty, Some(tracer))
    val results   = mutable.ArrayBuffer.empty[Try[String]]
    val tracked   = Events.trackCommand[String](events, read, result => results += result)

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), read, redirectsLeft = 0, tracked)
    scheduler.advance(Duration.Zero)

    assertEquals(access.attempted.toVector, Vector(r1, r2))
    assertEquals(topology.exhausted.size, 1)
    assertEquals(routed.toVector, Vector.empty)
    assert(results.head.failed.get.isInstanceOf[NotConnected])
    events.close()
  }

  test("demotion is terminal and invokes the topology adapter exactly once") {
    val scheduler = new ManualScheduler
    val demoted   = ServerError("READONLY", "the selected Node is no longer master")
    val access    = new FakeAccess(Map(master -> new FakeConnection(Vector(Failure(demoted)))))
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.MasterPreferred, access, topology, scheduler)
    val results   = mutable.ArrayBuffer.empty[Try[String]]

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1)), read, redirectsLeft = 0, results += _)
    scheduler.advance(Duration.Zero)

    assertEquals(access.attempted.toVector, Vector(master))
    assertEquals(topology.terminals.toVector, Vector(master -> demoted))
    assertEquals(results.toVector, Vector(Failure(demoted)))
  }

  test("Redirect handling stays behind the topology-policy adapter") {
    val scheduler = new ManualScheduler
    val moved     = ServerError("MOVED", "42 other:6380")
    val access    = new FakeAccess(Map(r1 -> new FakeConnection(Vector(Failure(moved)))))
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val results   = mutable.ArrayBuffer.empty[Try[String]]

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1)), read, redirectsLeft = 2, results += _)
    scheduler.advance(Duration.Zero)

    assertEquals(topology.redirects.toVector.map { case (from, _, left) => from -> left }, Vector(r1 -> 2))
    assertEquals(results.toVector, Vector(Failure(moved)))
  }

  test("the serving Node is attributed before terminal completion") {
    val scheduler = new ManualScheduler
    val access    = new FakeAccess(Map(r1 -> new FakeConnection(Vector(Success("ok")))))
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val routed    = mutable.ArrayBuffer.empty[Node]
    val tracer    = new CommandTracer {
      def onCommand(command: Command[?]): CommandSpan = new CommandSpan {
        def routedTo(node: Node): Unit      = routed += node
        def settled(outcome: Outcome): Unit = ()
      }
    }
    val events    = Events(Vector.empty, Some(tracer))
    val results   = mutable.ArrayBuffer.empty[Try[String]]
    val tracked   = Events.trackCommand[String](events, read, (result: Try[String]) => results += result)

    policy.submit(ReadPolicy.Source.forMaster(master, Vector(r1)), read, redirectsLeft = 0, tracked)

    assertEquals(routed.toVector, Vector(r1))
    assertEquals(results.toVector, Vector(Success("ok")))
    events.close()
  }

  test("a Pipeline that cannot reach its first candidate is submitted whole to the next candidate") {
    val scheduler = new ManualScheduler
    val access    = new FakeAccess(
      Map(
        r1 -> new FakeConnection(Vector.empty, acceptBatch = false),
        r2 -> new FakeConnection(Vector(Success("one"), Success("two")))
      )
    )
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val results   = Array.fill[Option[Try[Any]]](2)(None)
    val callbacks = Vector.tabulate(2)(index => (result: Try[Any]) => results(index) = Some(result))

    val submitted = policy.submitBatch(
      ReadPolicy.Source.forMaster(master, Vector(r1, r2)),
      Vector(read, read),
      callbacks,
      redirectsLeft = 0
    )

    assert(submitted)
    assertEquals(access.attempted.toVector, Vector(r1, r2))
    assertEquals(results.toVector, Vector(Some(Success("one")), Some(Success("two"))))
  }

  test("safe per-position Pipeline losses fall through without replaying successful positions") {
    val scheduler = new ManualScheduler
    val lost      = Failure(ConnectionLost(mayHaveExecuted = false))
    val access    = new FakeAccess(
      Map(
        r1 -> new FakeConnection(Vector(lost, Success("already-ok"))),
        r2 -> new FakeConnection(Vector(Success("retried")))
      )
    )
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val results   = Array.fill[Option[Try[Any]]](2)(None)
    val callbacks = Vector.tabulate(2)(index => (result: Try[Any]) => results(index) = Some(result))

    val submitted = policy.submitBatch(
      ReadPolicy.Source.forMaster(master, Vector(r1, r2)),
      Vector(read, read),
      callbacks,
      redirectsLeft = 0
    )
    scheduler.advance(Duration.Zero)

    assert(submitted)
    assertEquals(access.attempted.toVector, Vector(r1, r2))
    assertEquals(results.toVector, Vector(Some(Success("retried")), Some(Success("already-ok"))))
  }

  test("ambiguous per-position Pipeline loss is terminal without replaying that position") {
    val scheduler = new ManualScheduler
    val lost      = ConnectionLost(mayHaveExecuted = true)
    val access    = new FakeAccess(
      Map(
        r1 -> new FakeConnection(Vector(Failure(lost), Success("ok"))),
        r2 -> new FakeConnection(Vector(Success("must-not-run")))
      )
    )
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val results   = Array.fill[Option[Try[Any]]](2)(None)
    val callbacks = Vector.tabulate(2)(index => (result: Try[Any]) => results(index) = Some(result))

    assert(policy.submitBatch(ReadPolicy.Source.forMaster(master, Vector(r1, r2)), Vector(read, read), callbacks, redirectsLeft = 0))
    scheduler.advance(Duration.Zero)

    assertEquals(access.attempted.toVector, Vector(r1))
    assertEquals(results.toVector, Vector(Some(Failure(lost)), Some(Success("ok"))))
    assertEquals(topology.terminals.toVector, Vector(r1 -> lost))
  }

  test("a Pipeline rejected by every candidate invokes no callback and remains unattributed") {
    val scheduler = new ManualScheduler
    val access    = new FakeAccess(
      Map(
        r1 -> new FakeConnection(Vector.empty, acceptBatch = false),
        r2 -> new FakeConnection(Vector.empty, acceptBatch = false)
      )
    )
    val topology  = new RecordingTopology
    val policy    = new ReadPolicy(ReadFrom.Replica, access, topology, scheduler)
    val results   = mutable.ArrayBuffer.empty[Try[Any]]
    val routed    = mutable.ArrayBuffer.empty[Node]
    val tracer    = new CommandTracer {
      def onCommand(command: Command[?]): CommandSpan = new CommandSpan {
        def routedTo(node: Node): Unit      = routed += node
        def settled(outcome: Outcome): Unit = ()
      }
    }
    val events    = Events(Vector.empty, Some(tracer))
    val tracked   = Events.trackCommand[Any](events, read, result => results += result)

    val submitted = policy.submitBatch(
      ReadPolicy.Source.forMaster(master, Vector(r1, r2)),
      Vector(read),
      Vector(tracked),
      redirectsLeft = 0
    )

    assert(!submitted)
    assertEquals(results.toVector, Vector.empty)
    assertEquals(routed.toVector, Vector.empty)
    assertEquals(topology.batchExhaustions.size, 1)
    events.close()
  }

  final private class FakeConnection(var replies: Vector[Try[Any]], val live: Boolean = true, acceptBatch: Boolean = true)
    extends ReadPolicy.Connection {
    def isLive: Boolean = live

    def submit[A](command: Command[A], callback: Try[A] => Unit): Unit =
      callback(replies.head.asInstanceOf[Try[A]])

    def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Boolean = {
      if (acceptBatch) callbacks.zip(replies).foreach { case (callback, reply) => callback(reply) }
      acceptBatch
    }
  }

  final private class FakeAccess(initial: Map[Node, FakeConnection]) extends ReadPolicy.NodeAccess {
    val connections                                                        = mutable.HashMap.from(initial)
    val attempted                                                          = mutable.ArrayBuffer.empty[Node]
    def existing(node: Node, role: ReadPolicy.Role): ReadPolicy.Connection =
      connections.get(node) match {
        case Some(connection) => attempted += node; connection
        case None             => null
      }

    def establish(node: Node, role: ReadPolicy.Role): ReadPolicy.Connection = {
      attempted += node
      connections.getOrElse(node, throw NotConnected())
    }
  }

  final private class RecordingTopology extends ReadPolicy.TopologyPolicy {
    val redirects        = mutable.ArrayBuffer.empty[(Node, Redirect, Int)]
    val exhausted        = mutable.ArrayBuffer.empty[(ReadPolicy.Exhaustion, Int)]
    val terminals        = mutable.ArrayBuffer.empty[(Node, Throwable)]
    val fallthroughs     = mutable.ArrayBuffer.empty[(Node, Throwable)]
    val batchExhaustions = mutable.ArrayBuffer.empty[ReadPolicy.Exhaustion]

    override def redirected[A](attempt: ReadPolicy.RedirectAttempt[A]): Unit = {
      redirects += ((attempt.from, attempt.redirect, attempt.redirectsLeft))
      attempt.failAtSource(attempt.error)
    }

    def exhausted[A](attempt: ReadPolicy.ExhaustedAttempt[A]): Unit = {
      exhausted += ((attempt.exhaustion, attempt.redirectsLeft))
      val error = attempt.exhaustion match {
        case ReadPolicy.Exhaustion.Unsubmitted(_)          => NotConnected()
        case ReadPolicy.Exhaustion.AfterSafeLoss(_, error) => error
      }
      attempt.failAtSource(error)
    }

    def terminal(node: Node, error: Throwable): Unit = terminals += ((node, error))

    override def onSafeLoss(node: Node, error: Throwable): Unit = fallthroughs += ((node, error))

    def batchExhausted(exhaustion: ReadPolicy.Exhaustion): Unit = batchExhaustions += exhaustion
  }
}

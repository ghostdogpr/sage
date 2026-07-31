package sage.client.internal

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.{Success, Try}

import sage.Bytes
import sage.SageException.NotConnected
import sage.client.{BackoffConfig, DedicatedPoolConfig, ReadFrom, WatchdogConfig}
import sage.cluster.Node
import sage.commands.{Command, Connection, Execution}
import sage.protocol.Frame

class ReadRoutingSpec extends munit.FunSuite {

  private val m  = Node("master", 6379)
  private val r1 = Node("replica1", 6379)
  private val r2 = Node("replica2", 6379)

  test("Master routes to the master only") {
    assertEquals(ReadRouting.candidates(ReadFrom.Master, m, Vector(r1, r2), 0), Vector(m))
  }

  test("MasterPreferred tries the master first, then replicas") {
    assertEquals(ReadRouting.candidates(ReadFrom.MasterPreferred, m, Vector(r1, r2), 0), Vector(m, r1, r2))
  }

  test("ReplicaPreferred tries replicas first, then the master") {
    assertEquals(ReadRouting.candidates(ReadFrom.ReplicaPreferred, m, Vector(r1, r2), 0), Vector(r1, r2, m))
  }

  test("Replica lists replicas only, round-robin-rotated by the cursor") {
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector(r1, r2), 0), Vector(r1, r2))
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector(r1, r2), 1), Vector(r2, r1))
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector(r1, r2), 2), Vector(r1, r2))
  }

  test("Replica with no replicas is empty, so the strict policy fails") {
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector.empty, 0), Vector.empty)
  }

  test("ReplicaPreferred with no replicas falls back to the master") {
    assertEquals(ReadRouting.candidates(ReadFrom.ReplicaPreferred, m, Vector.empty, 0), Vector(m))
  }

  test("a negative cursor still rotates within bounds") {
    assertEquals(ReadRouting.candidates(ReadFrom.Replica, m, Vector(r1, r2), -1), Vector(r2, r1))
  }

  private def cmd(
    name: String,
    execution: Execution = Execution.Ordinary,
    isReadOnly: Boolean = false,
    cursorBound: Boolean = false
  ): Command[Unit] =
    Command(name, Command.NoKeys, Vector.empty, _ => Right(()), execution, isReadOnly = isReadOnly, cursorBound = cursorBound)

  test("eligibility: ordinary read-only is eligible; writes, blocking reads, and cursor-bound scans are not") {
    assert(ReadRouting.replicaEligible(cmd("GET", isReadOnly = true)))
    assert(!ReadRouting.replicaEligible(cmd("SET")))
    assert(!ReadRouting.replicaEligible(cmd("XREAD", Execution.Blocking, isReadOnly = true)))
    // a SCAN cursor is only valid on its issuing node, so it must never round-robin across replicas
    assert(!ReadRouting.replicaEligible(cmd("HSCAN", isReadOnly = true, cursorBound = true)))
  }

  private val ping = Connection.ping(None)

  final private class Fixture(readFrom: ReadFrom = ReadFrom.Replica, unreachable: Set[Node] = Set.empty, refusing: Set[Node] = Set.empty) {
    val scheduler                                                       = new ManualScheduler
    val refreshes                                                       = new AtomicInteger()
    private val transports                                              = new ConcurrentHashMap[Node, FakeTransport]()
    private def respond(node: Node)(payload: Bytes): Seq[Frame]         =
      if (payload.asUtf8String.contains("HELLO")) Seq(Replies.hello)
      else if (refusing(node)) Seq(Frame.SimpleError("LOADING the dataset is loading"))
      else {
        val text  = payload.asUtf8String
        val pings = text.sliding("PING".length).count(_ == "PING")
        Seq.fill(math.max(1, pings))(Frame.SimpleString("PONG"))
      }
    private val factory: Node => MultiplexedConnection.TransportFactory = node =>
      (onFrame, onClosed) =>
        if (unreachable(node)) throw new IOException(s"unreachable $node")
        else {
          val transport = new FakeTransport(onFrame, onClosed, respond(node))
          transports.put(node, transport)
          transport
        }
    private def newPool(): NodePool                                     =
      new NodePool(
        factory,
        scheduler,
        Vector(Connection.hello(None)),
        BackoffConfig(),
        WatchdogConfig(enabled = false),
        1.second,
        Duration.Zero,
        DedicatedPoolConfig()
      )
    val masterPool                                                      = newPool()
    val replicaPool                                                     = newPool()
    val reads                                                           =
      new ReadRouting(masterPool, replicaPool, scheduler, readFrom, () => refreshes.incrementAndGet(): Unit)
    def establish(node: Node): Unit                                     = replicaPool.getOrEstablish(node): Unit
    def kill(node: Node): Unit                                          = transports.get(node).close()
    def wrote(node: Node): Boolean                                      = transports.get(node).written.exists(_.asUtf8String.contains("PING"))
    def written(node: Node): Vector[String]                             = transports.get(node).written.map(_.asUtf8String)
    def close(): Unit                                                   = {
      masterPool.close()
      replicaPool.close()
    }
  }

  private def collector(): (AtomicReference[Try[String]], Try[String] => Unit) = {
    val result = new AtomicReference[Try[String]]()
    (result, result.set)
  }

  test("walk with no candidate left refreshes, then fails with NotConnected") {
    val fixture            = new Fixture()
    val (result, complete) = collector()
    fixture.reads.walk(ping, Vector.empty, m, complete)((_, _, _) => fail("no candidate should be tried"))
    assertEquals(fixture.refreshes.get(), 1)
    assert(result.get().failed.get.isInstanceOf[NotConnected])
    fixture.close()
  }

  test("walk skips an established-but-dead candidate and submits to the next") {
    val fixture            = new Fixture()
    fixture.establish(r1)
    fixture.establish(r2)
    fixture.kill(r1)
    val (result, complete) = collector()
    fixture.reads.walk(ping, Vector(r1, r2), m, complete)((_, _, _) => fail("a live candidate should have answered"))
    assertEquals(result.get(), Success("PONG"))
    assert(!fixture.wrote(r1), "the dead candidate must not be written to")
    assert(fixture.wrote(r2))
    fixture.close()
  }

  test("walk offloads to establish an unseen candidate") {
    val fixture            = new Fixture()
    val (result, complete) = collector()
    fixture.reads.walk(ping, Vector(r1), m, complete)((_, _, _) => fail("the candidate should have answered"))
    assertEquals(result.get(), null, "establishing must not run on the caller's thread")
    fixture.scheduler.advance(Duration.Zero)
    assertEquals(result.get(), Success("PONG"))
    fixture.close()
  }

  test("walk falls through a candidate it cannot establish") {
    val fixture            = new Fixture(unreachable = Set(r1))
    val (result, complete) = collector()
    fixture.reads.walk(ping, Vector(r1, r2), m, complete)((_, _, _) => fail("r2 should have answered"))
    fixture.scheduler.advance(Duration.Zero)
    assertEquals(result.get(), Success("PONG"))
    assert(fixture.wrote(r2))
    fixture.close()
  }

  test("walk reports a fault with the node that failed and the candidates left") {
    val fixture = new Fixture(refusing = Set(r1))
    fixture.establish(r1)
    val seen    = new AtomicReference[(Node, Vector[Node])]()
    fixture.reads.walk(ping, Vector(r1, r2), m, _ => fail("the fault must not complete the command"))((node, _, rest) => seen.set((node, rest)))
    fixture.scheduler.advance(Duration.Zero)
    assertEquals(seen.get(), (r1, Vector(r2)))
    fixture.close()
  }

  test("pickOne answers the first live candidate, without offloading") {
    val fixture = new Fixture()
    fixture.establish(r1)
    val picked  = new AtomicReference[Option[ReadRouting.Picked]]()
    fixture.reads.pickOne(Vector(r1), m)(picked.set)
    assertEquals(picked.get(), Some(ReadRouting.Picked(r1, fixture.replicaPool.existing(r1), Vector.empty)))
    fixture.close()
  }

  test("pickOne returns the candidates after the selected node") {
    val fixture = new Fixture()
    fixture.establish(r1)
    fixture.establish(r2)
    val picked  = new AtomicReference[Option[ReadRouting.Picked]]()
    fixture.reads.pickOne(Vector(r1, r2), m)(picked.set)
    assertEquals(picked.get(), Some(ReadRouting.Picked(r1, fixture.replicaPool.existing(r1), Vector(r2))))
    fixture.close()
  }

  test("pickOne answers None when every candidate is established but dead") {
    val fixture = new Fixture()
    fixture.establish(r1)
    fixture.kill(r1)
    val picked  = new AtomicReference[Option[ReadRouting.Picked]]()
    fixture.reads.pickOne(Vector(r1), m)(picked.set)
    assertEquals(picked.get(), None)
    fixture.close()
  }

  test("pickOne retries an unsent batch on the remaining candidate as one batch") {
    val fixture   = new Fixture()
    fixture.establish(r1)
    fixture.establish(r2)
    val results   = new java.util.concurrent.ConcurrentLinkedQueue[Try[Any]]()
    val commands  = Vector(ping, ping)
    val callbacks = Vector.fill(commands.length)((result: Try[Any]) => results.add(result): Unit)

    def submitOn(picked: Option[ReadRouting.Picked]): Unit =
      picked match {
        case Some(ReadRouting.Picked(node, client, remaining)) =>
          if (node == r1) fixture.kill(r1) // selected while live, disconnected before submitAll reserves the generation
          if (!client.submitAll(commands, callbacks))
            fixture.reads.pickOne(remaining, m)(submitOn)
        case None                                              => fail("the remaining live candidate should accept the batch")
      }

    fixture.reads.pickOne(Vector(r1, r2), m)(submitOn)

    assertEquals(results.size(), commands.length)
    assert(results.asScala.forall(_.isSuccess))
    val fallbackWrites = fixture.written(r2).filter(_.contains("PING"))
    assertEquals(fallbackWrites.length, 1, "the retry must remain one transport batch")
    assertEquals(fallbackWrites.head.sliding("PING".length).count(_ == "PING"), commands.length)
    fixture.close()
  }

  test("pickOne offloads to establish, then answers None when no candidate can be reached") {
    val fixture = new Fixture(unreachable = Set(r1, r2))
    val picked  = new AtomicReference[Option[ReadRouting.Picked]]()
    fixture.reads.pickOne(Vector(r1, r2), m)(picked.set)
    assertEquals(picked.get(), null, "establishing must not run on the caller's thread")
    fixture.scheduler.advance(Duration.Zero)
    assertEquals(picked.get(), None)
    fixture.close()
  }

  test("candidatesFor advances its master's round-robin cursor once per call") {
    val fixture = new Fixture()
    assertEquals(fixture.reads.candidatesFor(m, Vector(r1, r2)), Vector(r1, r2))
    assertEquals(fixture.reads.candidatesFor(m, Vector(r1, r2)), Vector(r2, r1))
    fixture.close()
  }

  test("candidatesFor refreshes when a replica-preferred read finds no replica") {
    val preferred = new Fixture(ReadFrom.ReplicaPreferred)
    assertEquals(preferred.reads.candidatesFor(m, Vector.empty), Vector(m))
    assertEquals(preferred.refreshes.get(), 1)
    val strict    = new Fixture()
    assertEquals(strict.reads.candidatesFor(m, Vector.empty), Vector.empty)
    assertEquals(strict.refreshes.get(), 0)
    preferred.close()
    strict.close()
  }

  test("retain drops the cursors of departed masters") {
    val fixture = new Fixture()
    assertEquals(fixture.reads.candidatesFor(m, Vector(r1, r2)), Vector(r1, r2))
    fixture.reads.retain(_ => false)
    assertEquals(fixture.reads.candidatesFor(m, Vector(r1, r2)), Vector(r1, r2))
    fixture.close()
  }
}

package sage.client

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import kyo.compat.*

import sage.{Bytes, SageEvent, SageListener}
import sage.SageException.ConnectionFailed
import sage.client.internal.{Events, FakeTransport, MasterReplicaLive, MultiplexedConnection, Scheduler}
import sage.cluster.Node
import sage.commands.{Command, Connection}
import sage.protocol.Frame

class MasterReplicaTopologySpec extends munit.FunSuite {

  private val primary           = Node("primary-endpoint", 6379)
  private val reader            = Node("reader-endpoint", 6379)
  private val advertisedReplica = Node("10.0.0.7", 6379)

  private val helloReply: Frame =
    Frame.Map(
      Vector(
        Frame.BulkString(Bytes.utf8("server"))  -> Frame.BulkString(Bytes.utf8("redis")),
        Frame.BulkString(Bytes.utf8("version")) -> Frame.BulkString(Bytes.utf8("8.0.0")),
        Frame.BulkString(Bytes.utf8("proto"))   -> Frame.Integer(3),
        Frame.BulkString(Bytes.utf8("role"))    -> Frame.BulkString(Bytes.utf8("master"))
      )
    )

  private def masterRole(replicas: Node*): Frame =
    Frame.Array(
      Vector(
        Frame.BulkString(Bytes.utf8("master")),
        Frame.Integer(0L),
        Frame.Array(
          replicas.toVector.map(r =>
            Frame.Array(
              Vector(
                Frame.BulkString(Bytes.utf8(r.host)),
                Frame.BulkString(Bytes.utf8(r.port.toString)),
                Frame.BulkString(Bytes.utf8("0"))
              )
            )
          )
        )
      )
    )

  private def replicaRole(master: Node): Frame =
    Frame.Array(
      Vector(
        Frame.BulkString(Bytes.utf8("slave")),
        Frame.BulkString(Bytes.utf8(master.host)),
        Frame.Integer(master.port.toLong),
        Frame.BulkString(Bytes.utf8("connected")),
        Frame.Integer(0L)
      )
    )

  private val zscore: Command[Long] =
    Command("ZSCORE", Command.NoKeys, Vector.empty, (_: Frame) => Right(1L), isReadOnly = true)

  final private class Fixture(
    seeds: Vector[Node],
    roles: collection.Map[Node, Frame],
    unreachable: collection.Set[Node] = Set.empty,
    readFrom: ReadFrom = ReadFrom.ReplicaPreferred,
    events: Events = Events.disabled,
    minRefreshInterval: FiniteDuration = 1.second
  ) {
    private val dials      = new ConcurrentLinkedQueue[Node]()
    private val transports = new ConcurrentLinkedQueue[(Node, FakeTransport)]()

    private val factory: Node => MultiplexedConnection.TransportFactory = node =>
      (onFrame, onClosed) => {
        val _                            = dials.add(node)
        if (unreachable(node)) throw new IOException(s"cannot reach $node")
        val respond: Bytes => Seq[Frame] = payload => {
          val text = payload.asUtf8String
          if (text.contains("HELLO")) Seq(helloReply)
          else if (text.contains("ROLE")) roles.get(node).toSeq
          else if (text.contains("ZSCORE")) Seq(Frame.Integer(1L))
          else Seq(Frame.SimpleString("OK"))
        }
        val transport                    = new FakeTransport(onFrame, onClosed, respond)
        val _                            = transports.add(node -> transport)
        transport
      }

    val live: MasterReplicaLive = new MasterReplicaLive(
      factory,
      Scheduler.real,
      Vector(Connection.hello(None)),
      SageConfig(readFrom = readFrom, connectTimeout = 500.millis),
      seeds,
      minRefreshInterval,
      events
    )

    def dialled: Vector[Node] = dials.asScala.toVector

    def readsServedBy(node: Node): Int =
      transports.asScala.toVector.collect { case (n, t) if n == node => t.written.count(_.asUtf8String.contains("ZSCORE")) }.sum

    def read(): Long = scala.concurrent.Await.result(live.run(zscore).unsafeRun, 30.seconds)

    def timedRead(): Long = {
      val start = System.nanoTime()
      val _     = read()
      (System.nanoTime() - start) / 1000000L
    }

    def close(): Unit = { val _ = scala.concurrent.Await.result(live.close.unsafeRun, 30.seconds) }
  }

  test("several seeds pin the topology to the supplied endpoints, and a ROLE-reported address is never dialled") {
    val f = new Fixture(
      seeds = Vector(primary, reader),
      roles = Map(primary -> masterRole(advertisedReplica), reader -> replicaRole(primary), advertisedReplica -> replicaRole(primary)),
      unreachable = Set(advertisedReplica)
    )
    f.live.bootstrapRoles()

    assertEquals(f.read(), 1L)
    assertEquals(f.read(), 1L)
    val dialled = f.dialled
    val served  = f.readsServedBy(reader)
    f.close()

    assertEquals(served, 2, "both reads should be served by the supplied reader endpoint")
    assert(!dialled.contains(advertisedReplica), s"the ROLE-reported address must never be dialled, dialled $dialled")
  }

  test("a single seed keeps discovery, taking replica addresses from the ROLE reply") {
    val f = new Fixture(
      seeds = Vector(primary),
      roles = Map(primary -> masterRole(advertisedReplica), advertisedReplica -> replicaRole(primary))
    )
    f.live.bootstrapRoles()

    assertEquals(f.read(), 1L)
    val served = f.readsServedBy(advertisedReplica)
    f.close()

    assertEquals(served, 1, "a lone seed should discover its replica and read from it")
  }

  test("a discovered replica that cannot be reached is dropped, so reads stop dialling it") {
    val f = new Fixture(
      seeds = Vector(primary),
      roles = Map(primary -> masterRole(advertisedReplica), advertisedReplica -> replicaRole(primary)),
      unreachable = Set(advertisedReplica),
      minRefreshInterval = 1.hour
    )
    f.live.bootstrapRoles()

    Vector.fill(5)(f.read()).foreach(r => assertEquals(r, 1L))
    val dials = f.dialled.count(_ == advertisedReplica)
    f.close()

    assertEquals(f.readsServedBy(advertisedReplica), 0, "a dead replica must not serve reads")
    assert(dials <= 2, s"a dead replica should cost a discovery probe and at most one refresh probe, not one dial per read, got $dials")
  }

  test("an unreachable supplied endpoint is dropped, so a partially available deployment still connects") {
    val f = new Fixture(
      seeds = Vector(primary, reader),
      roles = Map(primary -> masterRole(), reader -> replicaRole(primary)),
      unreachable = Set(reader)
    )
    f.live.bootstrapRoles()

    assertEquals(f.read(), 1L, "with no reachable replica the read falls back to the master")
    f.close()
  }

  test("a pruned replica is re-admitted once it becomes reachable again") {
    val down = collection.mutable.Set[Node](advertisedReplica)
    val f    = new Fixture(
      seeds = Vector(primary),
      roles = Map(primary -> masterRole(advertisedReplica), advertisedReplica -> replicaRole(primary)),
      unreachable = down,
      minRefreshInterval = 100.millis
    )
    f.live.bootstrapRoles()
    assertEquals(f.read(), 1L)
    assertEquals(f.readsServedBy(advertisedReplica), 0, "a dead replica must not serve reads")

    down.clear()
    val deadline = System.nanoTime() + 5.seconds.toNanos
    while (f.readsServedBy(advertisedReplica) == 0 && System.nanoTime() < deadline) {
      val _ = f.read()
      Thread.sleep(50)
    }
    val served   = f.readsServedBy(advertisedReplica)
    f.close()

    assert(served > 0, "reads should return to the replica once it is reachable and a refresh re-admits it")
  }

  test("seeds that report no master fail the connect with an actionable error") {
    val f     = new Fixture(
      seeds = Vector(reader, advertisedReplica),
      roles = Map(reader -> replicaRole(primary), advertisedReplica -> replicaRole(primary))
    )
    val error = intercept[ConnectionFailed](f.live.bootstrapRoles())
    assert(error.getMessage.contains("missing from the seeds"), s"unhelpful message: ${error.getMessage}")
  }

  test("re-resolving after a failover moves the roles between the same pinned endpoints") {
    val roles = collection.mutable.Map[Node, Frame](primary -> masterRole(advertisedReplica), reader -> replicaRole(primary))
    val f     = new Fixture(seeds = Vector(primary, reader), roles = roles, unreachable = Set(advertisedReplica))
    f.live.bootstrapRoles()
    assertEquals(f.read(), 1L)
    assert(f.dialled.contains(reader), "the reader endpoint should serve reads before the failover")

    roles(primary) = replicaRole(reader)
    roles(reader) = masterRole(advertisedReplica)
    f.live.bootstrapRoles()

    assertEquals(f.read(), 1L)
    val dialled = f.dialled
    f.close()
    assert(!dialled.contains(advertisedReplica), s"a failover must not introduce a discovered address, dialled $dialled")
  }

  test("a connect that fails names the address in a ConnectFailed event") {
    val seen     = new ConcurrentLinkedQueue[SageEvent.Connection.ConnectFailed]()
    val listener = new SageListener {
      def onEvent(event: SageEvent): Unit = event match {
        case e: SageEvent.Connection.ConnectFailed => val _ = seen.add(e)
        case _                                     => ()
      }
    }
    val f        = new Fixture(
      seeds = Vector(primary, reader),
      roles = Map(primary -> masterRole(), reader -> replicaRole(primary)),
      unreachable = Set(reader),
      events = Events(Vector(listener))
    )
    f.live.bootstrapRoles()

    val deadline = System.nanoTime() + 2.seconds.toNanos
    while (seen.isEmpty && System.nanoTime() < deadline) Thread.sleep(10)
    f.close()

    val failures = seen.asScala.toVector
    assertEquals(failures.map(_.node), Vector(Some(reader)), s"the failing address should be reported, got $failures")
    assert(failures.head.error.isInstanceOf[IOException], s"the cause should be carried: ${failures.head.error}")
  }
}

package sage.client

import java.io.IOException
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

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
          replicas.toVector.map(replica =>
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

  private def replicaRole(master: Node, state: String = "connected"): Frame =
    Frame.Array(
      Vector(
        Frame.BulkString(Bytes.utf8("slave")),
        Frame.BulkString(Bytes.utf8(master.host)),
        Frame.Integer(master.port.toLong),
        Frame.BulkString(Bytes.utf8(state)),
        Frame.Integer(0L)
      )
    )

  private val readCommand: Command[Long] =
    Command("ZSCORE", Command.NoKeys, Vector.empty, (_: Frame) => Right(1L), isReadOnly = true)

  private val writeCommand: Command[Long] =
    Command("ZADD", Command.NoKeys, Vector.empty, (_: Frame) => Right(1L), isReadOnly = false)

  final private class Fixture(
    seeds: Vector[Node],
    initialRoles: Map[Node, Frame],
    unreachable: collection.Set[Node] = Set.empty,
    events: Events = Events.disabled,
    minRefreshInterval: FiniteDuration = 50.millis
  ) {

    val roles                                                           = TrieMap.from(initialRoles)
    @volatile var writesFailReadonly                                    = false
    private val dials                                                   = new ConcurrentLinkedQueue[Node]()
    private val transports                                              = new ConcurrentLinkedQueue[(Node, FakeTransport)]()
    private val factory: Node => MultiplexedConnection.TransportFactory = node =>
      (onFrame, onClosed) => {
        val _                            = dials.add(node)
        if (unreachable(node)) throw new IOException(s"cannot reach $node")
        val respond: Bytes => Seq[Frame] = payload => {
          val text = payload.asUtf8String
          if (text.contains("HELLO")) Seq(helloReply)
          else if (text.contains("ROLE")) roles.get(node).toSeq
          else if (text.contains("ZSCORE")) Seq(Frame.Integer(1L))
          else if (text.contains("ZADD") && writesFailReadonly)
            Seq(Frame.SimpleError("READONLY You can't write against a read only replica."))
          else if (text.contains("ZADD")) Seq(Frame.Integer(1L))
          else Seq(Frame.SimpleString("OK"))
        }
        val transport                    = new FakeTransport(onFrame, onClosed, respond)
        val _                            = transports.add(node -> transport)
        transport
      }

    val live = new MasterReplicaLive(
      factory,
      Scheduler.real,
      Vector(Connection.hello(None)),
      SageConfig(readFrom = ReadFrom.ReplicaPreferred, connectTimeout = 500.millis),
      seeds,
      minRefreshInterval,
      events
    )

    def dialled: Vector[Node] = dials.asScala.toVector

    def readsServedBy(node: Node): Int =
      transports.asScala.toVector.collect { case (`node`, transport) =>
        transport.written.count(_.asUtf8String.contains("ZSCORE"))
      }.sum

    def read(): Long =
      scala.concurrent.Await.result(live.run(readCommand).unsafeRun, 10.seconds)

    def write(): Try[Long] =
      Try(scala.concurrent.Await.result(live.run(writeCommand).unsafeRun, 10.seconds))

    def awaitTrue(condition: => Boolean, clue: String): Unit = {
      val deadline = System.nanoTime() + 5.seconds.toNanos
      while (!condition && System.nanoTime() < deadline) Thread.sleep(25)
      assert(condition, clue)
    }

    def close(): Unit =
      scala.concurrent.Await.result(live.close.unsafeRun, 10.seconds)
  }

  test("several seeds keep the supplied addresses and never dial a ROLE-advertised address") {
    val fixture = new Fixture(
      seeds = Vector(primary, reader),
      initialRoles = Map(
        primary           -> masterRole(advertisedReplica),
        reader            -> replicaRole(primary),
        advertisedReplica -> replicaRole(primary)
      ),
      unreachable = Set(advertisedReplica)
    )
    fixture.live.bootstrapRoles()

    assertEquals(fixture.read(), 1L)
    assertEquals(fixture.readsServedBy(reader), 1)
    val dialled = fixture.dialled
    fixture.close()

    assert(!dialled.contains(advertisedReplica), s"advertised address must not be dialled: $dialled")
  }

  test("a single seed retains advertised-replica discovery") {
    val fixture = new Fixture(
      seeds = Vector(primary),
      initialRoles = Map(primary -> masterRole(advertisedReplica), advertisedReplica -> replicaRole(primary))
    )
    fixture.live.bootstrapRoles()

    assertEquals(fixture.read(), 1L)
    assertEquals(fixture.readsServedBy(advertisedReplica), 1)
    fixture.close()
  }

  test("an unreachable supplied endpoint is omitted and reported while the available topology connects") {
    val failures  = new ConcurrentLinkedQueue[SageEvent.Connection.ConnectFailed]()
    val delivered = new CountDownLatch(1)
    val events    = Events(
      Vector(
        new SageListener {
          def onEvent(event: SageEvent): Unit = event match {
            case failure: SageEvent.Connection.ConnectFailed =>
              val _ = failures.add(failure)
              delivered.countDown()
            case _                                           => ()
          }
        }
      )
    )
    val fixture   = new Fixture(
      seeds = Vector(primary, reader),
      initialRoles = Map(primary -> masterRole(), reader -> replicaRole(primary)),
      unreachable = Set(reader),
      events = events
    )
    fixture.live.bootstrapRoles()

    assertEquals(fixture.read(), 1L)
    assert(delivered.await(2, TimeUnit.SECONDS), "ConnectFailed was not delivered")
    val failure = failures.peek()
    fixture.close()

    assertEquals(failure.node, Some(reader))
    assert(failure.error.isInstanceOf[IOException], s"unexpected cause: ${failure.error}")
  }

  test("a singleton refresh reports the address whose connection failed, including a replica's advertised master") {
    val failures  = new ConcurrentLinkedQueue[SageEvent.Connection.ConnectFailed]()
    val delivered = new CountDownLatch(2)
    val events    = Events(
      Vector(
        new SageListener {
          def onEvent(event: SageEvent): Unit = event match {
            case failure: SageEvent.Connection.ConnectFailed =>
              val _ = failures.add(failure)
              delivered.countDown()
            case _                                           => ()
          }
        }
      )
    )
    val down      = mutable.Set.empty[Node]
    val fixture   = new Fixture(
      seeds = Vector(primary),
      initialRoles = Map(primary -> masterRole(reader), reader -> replicaRole(primary)),
      unreachable = down,
      events = events
    )
    fixture.live.bootstrapRoles()
    assert(fixture.write().isSuccess, "the master pool should be established before discovery connections fail")

    down += primary
    fixture.writesFailReadonly = true
    assert(fixture.write().isFailure, "READONLY should trigger a singleton role refresh")

    assert(delivered.await(2, TimeUnit.SECONDS), s"expected direct and replica-follow failures, got ${failures.asScala.toVector}")
    val reported = failures.asScala.toVector
    fixture.close()

    assertEquals(reported.map(_.node), Vector(Some(primary), Some(primary)))
    assert(reported.forall(_.error.isInstanceOf[IOException]), s"unexpected causes: ${reported.map(_.error)}")
  }

  test("a pinned replica is not used until its own ROLE state is connected") {
    val fixture = new Fixture(
      seeds = Vector(primary, reader),
      initialRoles = Map(primary -> masterRole(reader), reader -> replicaRole(primary, state = "sync"))
    )
    fixture.live.bootstrapRoles()

    assertEquals(fixture.read(), 1L)
    assertEquals(fixture.readsServedBy(primary), 1)
    assertEquals(fixture.readsServedBy(reader), 0)

    fixture.roles.update(reader, replicaRole(primary, state = "connected"))
    fixture.writesFailReadonly = true
    assert(fixture.write().isFailure, "READONLY should trigger a later role refresh")

    fixture.awaitTrue(
      {
        val _ = fixture.read()
        fixture.readsServedBy(reader) > 0
      },
      "the connected replica was not admitted after the refresh"
    )
    fixture.close()
  }

  test("several reachable seeds with no master fail as a connection problem") {
    val fixture = new Fixture(
      seeds = Vector(reader, advertisedReplica),
      initialRoles = Map(reader -> replicaRole(primary), advertisedReplica -> replicaRole(primary))
    )

    val error = intercept[ConnectionFailed](fixture.live.bootstrapRoles())
    assert(error.getMessage.contains("no supplied endpoint reports the master role"), error.getMessage)
  }

  test("a role refresh moves master and replica roles only between the pinned endpoints") {
    val fixture = new Fixture(
      seeds = Vector(primary, reader),
      initialRoles = Map(primary -> masterRole(advertisedReplica), reader -> replicaRole(primary)),
      unreachable = Set(advertisedReplica)
    )
    fixture.live.bootstrapRoles()
    assertEquals(fixture.read(), 1L)
    assertEquals(fixture.readsServedBy(reader), 1)

    fixture.roles.update(primary, replicaRole(reader))
    fixture.roles.update(reader, masterRole(advertisedReplica))
    fixture.writesFailReadonly = true
    assert(fixture.write().isFailure, "a write to the demoted master should fail")

    val before  = fixture.readsServedBy(primary)
    fixture.awaitTrue(
      {
        val _ = fixture.read()
        fixture.readsServedBy(primary) > before
      },
      "the refresh did not move reads to the demoted endpoint"
    )
    val dialled = fixture.dialled
    fixture.close()

    assert(!dialled.contains(advertisedReplica), s"refresh introduced an advertised address: $dialled")
  }
}

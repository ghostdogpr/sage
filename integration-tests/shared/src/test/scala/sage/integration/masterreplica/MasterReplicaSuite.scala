package sage.integration.masterreplica

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*

import sage.{Bytes, Message}
import sage.SageException.DecodeError
import sage.client.{Endpoint, MasterReplicaConfig, ReadFrom, SageConfig, Topology}
import sage.client.internal.Client
import sage.commands.{Command, Commands}
import sage.integration.{ContainerClient, Images}
import sage.protocol.Frame

/**
  * Shared scaffolding for the master-replica suites. One container runs two server processes — a master on 6379 and a replica on 6380 — kept
  * single-container like [[sage.integration.cluster.ClusterSuite]] while still exercising the runtime end to end: `ROLE` discovery, a distinct
  * replica-pool endpoint, [[ReadFrom]] routing, and genuine replication between two processes. The replica announces its testcontainers-mapped
  * host port (via `replica-announce-ip`/`-port`) so the address the master reports in `ROLE` is reachable from the test.
  */
abstract class MasterReplicaSuiteBase(image: String, serverBinary: String) extends munit.FunSuite with TestContainerForAll with ContainerClient {

  // the fault that kills the master keeps the replica in the foreground (and vice versa) so the container survives — the exec'd process owns it
  protected def masterRunsInForeground: Boolean = false

  // --protected-mode no admits the testcontainers-mapped (non-loopback) connection; --save '' / --appendonly no keep the nodes in-memory
  override val containerDef: GenericContainer.Def[GenericContainer] = {
    val master                   = s"$serverBinary --port 6379 --save '' --appendonly no --protected-mode no"
    val replica                  = s"$serverBinary --port 6380 --save '' --appendonly no --protected-mode no"
    val (background, foreground) = if (masterRunsInForeground) (replica, master) else (master, replica)
    GenericContainer.Def(image, exposedPorts = Seq(6379, 6380), command = Seq("sh", "-c", s"$background & exec $foreground"))
  }

  given ExecutionContext = munitExecutionContext

  protected val masterPort  = 6379
  protected val replicaPort = 6380
  protected val marker      = "mr:replica-only-marker"

  protected def admin(name: String, args: String*): Command[Unit] =
    Command(name, Command.NoKeys, args.toVector.map(Bytes.utf8), _ => Right(()))

  protected val infoReplication: Command[String] =
    Command(
      "INFO",
      Command.NoKeys,
      Vector(Bytes.utf8("replication")),
      {
        case Frame.BulkString(bytes)        => Right(bytes.asUtf8String)
        case Frame.VerbatimString(_, bytes) => Right(bytes.asUtf8String)
        case other                          => Left(DecodeError("bulk or verbatim string", Frame.describe(other)))
      }
    )

  // follow the master and announce the host-mapped port, then write a marker key the master never has, so a read's origin is observable. The
  // marker goes in after the link is up, since REPLICAOF triggers a full resync that would wipe an earlier write. Idempotent across a suite's tests.
  protected def ensureReplicating(replica: Client[CIO, String], announceHost: String, announcePort: Int): CIO[Unit] =
    replica.run(infoReplication).flatMap { info =>
      if (info.contains("master_link_status:up")) CIO.value(())
      else
        for {
          _ <- replica.run(admin("CONFIG", "SET", "replica-announce-ip", announceHost))
          _ <- replica.run(admin("CONFIG", "SET", "replica-announce-port", announcePort.toString))
          _ <- replica.run(admin("REPLICAOF", "127.0.0.1", masterPort.toString))
          // Valkey's async full-sync takes a few seconds even for an empty dataset, so budget generously under CI load
          _ <- awaitLinkUp(replica, 150)
          _ <- replica.run(admin("CONFIG", "SET", "replica-read-only", "no"))
          _ <- replica.run(admin("SET", marker, "from-replica"))
          // restore read-only so a misrouted write to the replica fails loudly; replication still applies (it bypasses read-only) and the marker persists
          _ <- replica.run(admin("CONFIG", "SET", "replica-read-only", "yes"))
        } yield ()
    }

  protected def awaitLinkUp(replica: Client[CIO, String], attempts: Int): CIO[Unit] =
    replica.run(infoReplication).flatMap { info =>
      if (info.contains("master_link_status:up")) CIO.value(())
      else if (attempts <= 0) CIO.fail(new RuntimeException(s"replica did not sync: $info"))
      else CIO.sleep(100.millis).flatMap(_ => awaitLinkUp(replica, attempts - 1))
    }

  // replication is async; poll until the replica catches up
  protected def awaitValue(client: Client[CIO, String], key: String, expected: String, attempts: Int): CIO[Option[String]] =
    client.get[String](key).flatMap {
      case Some(v) if v == expected => CIO.value(Some(v))
      case _ if attempts <= 0       => CIO.value(None)
      case _                        => CIO.sleep(100.millis).flatMap(_ => awaitValue(client, key, expected, attempts - 1))
    }

  // run for effect, discarding failure — for commands the server answers by closing the socket (SHUTDOWN), observed by what follows
  protected def ignoreFailure(action: CIO[Unit]): CIO[Unit] =
    action.fold(_ => CIO.value(()), _ => CIO.value(()))

  protected def standalone(host: String, port: Int): SageConfig =
    SageConfig(topology = Topology.Standalone(Endpoint(host, port)))

  protected def masterReplica(host: String, masterMappedPort: Int, policy: ReadFrom, minRefreshInterval: FiniteDuration = 5.seconds): SageConfig =
    SageConfig(
      topology = Topology.MasterReplica(Vector(Endpoint(host, masterMappedPort)), MasterReplicaConfig(minRefreshInterval = minRefreshInterval)),
      readFrom = policy
    )
}

/**
  * Read routing and master-pinned operations against a real master-replica deployment. Routing is proven with a marker key that lives only on
  * the replica: a read that sees it was served by the replica, and one that does not was served by the master, which never has it.
  */
abstract class MasterReplicaSuite(image: String, serverBinary: String) extends MasterReplicaSuiteBase(image, serverBinary) {

  test("reads honor the ReadFrom policy and writes always reach the master") {
    withContainers { server =>
      val host       = server.host
      val pm         = server.mappedPort(masterPort)
      val pr         = server.mappedPort(replicaPort)
      val replicaCfg = standalone(host, pr)

      val program =
        connectAndUse(replicaCfg)(ensureReplicating(_, host, pr))
          .flatMap { _ =>
            connectAndUse(masterReplica(host, pm, ReadFrom.Replica)) { client =>
              for {
                fromReplica <- client.get[String](marker)
                // the write always goes to the master; reading it back off the replica proves both replication and replica routing
                _           <- client.set("mr:k", "v")
                replicated  <- awaitValue(client, "mr:k", "v", 50)
              } yield {
                assertEquals(fromReplica, Some("from-replica"))
                assertEquals(replicated, Some("v"))
              }
            }
          }
          .flatMap(_ => connectAndUse(masterReplica(host, pm, ReadFrom.Master))(_.get[String](marker).map(assertEquals(_, None))))
          .flatMap(_ =>
            connectAndUse(masterReplica(host, pm, ReadFrom.ReplicaPreferred))(_.get[String](marker).map(assertEquals(_, Some("from-replica"))))
          )
          .flatMap(_ => connectAndUse(masterReplica(host, pm, ReadFrom.MasterPreferred))(_.get[String](marker).map(assertEquals(_, None))))
      program.unsafeRun
    }
  }

  test("transactions and pub/sub run on the master under the master-replica runtime") {
    withContainers { server =>
      val host       = server.host
      val pm         = server.mappedPort(masterPort)
      val pr         = server.mappedPort(replicaPort)
      val replicaCfg = standalone(host, pr)

      val program =
        connectAndUse(replicaCfg)(ensureReplicating(_, host, pr)).flatMap { _ =>
          connectAndUse(masterReplica(host, pm, ReadFrom.ReplicaPreferred)) { client =>
            for {
              commit  <- client.transaction(tx => tx.exec(Vector(Commands.incr[String]("mr:c"), Commands.incr[String]("mr:c"))))
              sub     <- client.subscribeChannels[String]("mr:news")
              count   <- client.publish("mr:news", "hello")
              message <- sub.next
              _       <- sub.close
            } yield {
              assertEquals(commit, Some(Vector(1L, 2L)))
              assertEquals(count, 1L)
              assertEquals(message, Some(Message("mr:news", "hello")))
            }
          }
        }
      program.unsafeRun
    }
  }
}

/**
  * Failover recovery: the replica is promoted and the old master taken out of write service, and the client re-discovers and re-homes on its
  * own. Roles refresh only on events, so the first write meets the old master, fails fast, and trips a re-discovery; the caller's retry then
  * lands on the promoted node. [[induceFailover]] supplies the fault, so the contract is checked against both the `READONLY` (demotion) and
  * connection-loss (crash) branches. Its own container, since the promotion permanently rewrites the topology.
  */
abstract class MasterReplicaFailoverSuite(image: String, serverBinary: String, fault: String) extends MasterReplicaSuiteBase(image, serverBinary) {

  // promote the replica, then take the old master out of write service in a fault-specific way; the client is told nothing
  protected def induceFailover(replicaCfg: SageConfig, masterCfg: SageConfig): CIO[Unit]

  // retry the write until it succeeds: the first attempt meets the old master and trips re-discovery, a later retry lands on the promoted master
  private def writeUntilAccepted(client: Client[CIO, String], key: String, value: String, attempts: Int): CIO[Boolean] =
    client
      .set(key, value)
      .fold(
        _ => CIO.value(true),
        _ => if (attempts <= 0) CIO.value(false) else CIO.sleep(100.millis).flatMap(_ => writeUntilAccepted(client, key, value, attempts - 1))
      )

  test(s"the client recovers writes after the replica is promoted to master ($fault)") {
    withContainers { server =>
      val host       = server.host
      val pm         = server.mappedPort(masterPort)
      val pr         = server.mappedPort(replicaPort)
      val replicaCfg = standalone(host, pr)
      val masterCfg  = standalone(host, pm)
      // short refresh interval so the event-driven re-discovery is not throttled away during the retry window
      val mrCfg      = masterReplica(host, pm, ReadFrom.Master, minRefreshInterval = 100.millis)

      val program =
        connectAndUse(replicaCfg)(ensureReplicating(_, host, pr)).flatMap { _ =>
          connectAndUse(mrCfg) { client =>
            for {
              _         <- client.set("fo:before", "v1")
              before    <- client.get[String]("fo:before")
              _         <- induceFailover(replicaCfg, masterCfg)
              recovered <- writeUntilAccepted(client, "fo:after", "v2", 50)
              after     <- client.get[String]("fo:after")
            } yield {
              assertEquals(before, Some("v1"))
              assert(recovered, "write never recovered after promotion")
              assertEquals(after, Some("v2"))
            }
          }
        }
      program.unsafeRun
    }
  }
}

/**
  * Failover where the old master is demoted to follow the new one — it stays reachable but answers writes with `READONLY`, exercising the
  * ownership-fault branch of the runtime's re-discovery.
  */
abstract class MasterReplicaDemotionFailoverSuite(image: String, serverBinary: String)
  extends MasterReplicaFailoverSuite(image, serverBinary, "demoted master") {

  protected def induceFailover(replicaCfg: SageConfig, masterCfg: SageConfig): CIO[Unit] =
    for {
      _ <- connectAndUse(replicaCfg)(_.run(admin("REPLICAOF", "NO", "ONE")))
      _ <- connectAndUse(masterCfg)(_.run(admin("REPLICAOF", "127.0.0.1", replicaPort.toString)))
    } yield ()
}

/**
  * Failover where the old master crashes outright (`SHUTDOWN`) — the next write meets a refused connection, exercising the connection-loss
  * branch of the runtime's re-discovery. The master is backgrounded (the suite default), so the foreground replica keeps the container alive.
  */
abstract class MasterReplicaConnectionLossFailoverSuite(image: String, serverBinary: String)
  extends MasterReplicaFailoverSuite(image, serverBinary, "master down") {

  protected def induceFailover(replicaCfg: SageConfig, masterCfg: SageConfig): CIO[Unit] =
    for {
      _ <- connectAndUse(replicaCfg)(_.run(admin("REPLICAOF", "NO", "ONE")))
      _ <- ignoreFailure(connectAndUse(masterCfg)(_.run(admin("SHUTDOWN", "NOSAVE"))))
    } yield ()
}

/**
  * A replica going down. The replica runs in the foreground so `SHUTDOWN`-ing it leaves the foreground master (and the container) alive. A
  * strict `Replica` read then has no node to reach and fails, while `ReplicaPreferred` falls back to the master. Both clients connect before
  * the replica dies, so they actually attempt the now-dead replica rather than simply never discovering it.
  */
abstract class MasterReplicaReplicaDownSuite(image: String, serverBinary: String) extends MasterReplicaSuiteBase(image, serverBinary) {

  override protected def masterRunsInForeground: Boolean = true

  test("a strict Replica read fails when the replica is down, while ReplicaPreferred falls back to the master") {
    withContainers { server =>
      val host       = server.host
      val pm         = server.mappedPort(masterPort)
      val pr         = server.mappedPort(replicaPort)
      val replicaCfg = standalone(host, pr)

      val program =
        connectAndUse(replicaCfg)(ensureReplicating(_, host, pr)).flatMap { _ =>
          connectAndUse(masterReplica(host, pm, ReadFrom.Replica)) { strict =>
            connectAndUse(masterReplica(host, pm, ReadFrom.ReplicaPreferred)) { preferred =>
              for {
                // a master-backed key for the fallback read; the replica-only marker for the warm-up reads
                _             <- preferred.set("rd:k", "v")
                warmStrict    <- strict.get[String](marker)
                warmPreferred <- preferred.get[String](marker)
                _             <- ignoreFailure(connectAndUse(replicaCfg)(_.run(admin("SHUTDOWN", "NOSAVE"))))
                strictFailed  <- strict.get[String]("rd:k").fold(_ => CIO.value(false), _ => CIO.value(true))
                fallback      <- awaitValue(preferred, "rd:k", "v", 50)
              } yield {
                assertEquals(warmStrict, Some("from-replica"))
                assertEquals(warmPreferred, Some("from-replica"))
                assert(strictFailed, "strict Replica read should fail when no replica is reachable")
                assertEquals(fallback, Some("v"))
              }
            }
          }
        }
      program.unsafeRun
    }
  }
}

class RedisMasterReplicaSuite extends MasterReplicaSuite(Images.redis, "redis-server")

class ValkeyMasterReplicaSuite extends MasterReplicaSuite(Images.valkey, "valkey-server")

class RedisMasterReplicaDemotionFailoverSuite extends MasterReplicaDemotionFailoverSuite(Images.redis, "redis-server")

class ValkeyMasterReplicaDemotionFailoverSuite extends MasterReplicaDemotionFailoverSuite(Images.valkey, "valkey-server")

class RedisMasterReplicaConnectionLossFailoverSuite extends MasterReplicaConnectionLossFailoverSuite(Images.redis, "redis-server")

class ValkeyMasterReplicaConnectionLossFailoverSuite extends MasterReplicaConnectionLossFailoverSuite(Images.valkey, "valkey-server")

class RedisMasterReplicaReplicaDownSuite extends MasterReplicaReplicaDownSuite(Images.redis, "redis-server")

class ValkeyMasterReplicaReplicaDownSuite extends MasterReplicaReplicaDownSuite(Images.valkey, "valkey-server")

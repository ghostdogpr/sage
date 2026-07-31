package sage.integration.cluster

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.Try

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForEach
import kyo.compat.*

import sage.client.{ClusterConfig, Endpoint, SageConfig, Topology}
import sage.client.internal.Client
import sage.commands.Commands
import sage.integration.{ContainerClient, Eventually, Images}

/**
  * Drives cluster failover recovery against a real multi-node cluster: three masters and their replicas in one container. When a master
  * crashes, the cluster promotes its replica automatically, and the client re-homes on its own — on the connection loss it force-refreshes
  * `CLUSTER SLOTS` and re-dispatches, bounded by `maxRedirects`. That bound is shorter than an election, so, as with a real application, the
  * caller retries across the failover window while the client refreshes the topology underneath.
  *
  * The fixed host ports are 7100-7105; see [[MultiNodeCluster]] for why a multi-node cluster cannot use mapped random ports.
  */
abstract class ClusterFailoverSuite(val image: String, val serverBinary: String)
  extends munit.FunSuite
  with TestContainerForEach
  with ContainerClient
  with MultiNodeCluster {

  protected val ports: Seq[Int] = 7100 to 7105
  private val victim            = 7100 // redis-cli --cluster-create makes the first nodes masters, so 7100 is a master with a replica to promote

  override protected val replicasPerMaster: Option[Int] = Some(1)

  // start the replica's initial sync immediately, not after the default 5s window, so it is a live copy before the failover
  override protected val extraServerFlags: Vector[String] = Vector("--repl-diskless-sync-delay 0")

  override val containerDef: FixedHostPortGenericContainer.Def = clusterContainerDef

  given ExecutionContext = munitExecutionContext

  // forming a six-node cluster and waiting out an election runs past munit's 30s default on a loaded CI box
  override def munitTimeout: Duration = 120.seconds

  private def parseKeys(out: String): Vector[String] =
    out.split("\n").iterator.map(_.trim).filter(_.nonEmpty).toVector

  private def victimReplicaPort(container: FixedHostPortGenericContainer): Int = {
    val nodes = clusterNodes(container, victim)
    val myId  =
      nodes.collectFirst { case node if node.isMyself => node.id }.getOrElse(throw new RuntimeException(s"victim $victim has no myself line"))
    nodes
      .collectFirst { case node if node.isReplica && node.masterId == myId => node.port }
      .getOrElse(throw new RuntimeException(s"no replica found for victim $victim among ${nodes.map(_.port)}"))
  }

  // the replication barrier: poll the victim's own replica until it holds every victim-owned key. WAIT keys off the calling connection's last
  // write, so it would not cover writes the cluster client sent on its own routed connections; reading the replica directly proves recovery.
  private def awaitReplicated(container: FixedHostPortGenericContainer, replicaPort: Int, expected: Set[String], attempts: Int): CIO[Unit] =
    Eventually.converges(attempts)(() => CIO.blocking(parseKeys(cli(container, replicaPort, "keys", "*")).toSet))(expected.subsetOf)(have =>
      s"victim's replica did not catch up; missing ${(expected -- have).take(5)}"
    )

  // `cluster_state:ok` flips before every node will actually serve writes, so a freshly formed cluster can briefly answer CLUSTERDOWN; retry
  // each write across that warm-up window, as a real application would, so the failover the test means to exercise is not masked by a startup race
  private def writeKey(client: Client[CIO, String], key: String, attempts: Int): CIO[Unit] =
    client
      .set(key, key)
      .fold(
        _ => CIO.value(()),
        error => if (attempts <= 0) CIO.fail(error) else CIO.sleep(200.millis).flatMap(_ => writeKey(client, key, attempts - 1))
      )

  private def writeAll(client: Client[CIO, String], keys: Vector[String]): CIO[Unit] =
    keys.foldLeft(CIO.value(()))((acc, key) => acc.flatMap(_ => writeKey(client, key, 150)))

  // retries a transport error, but fails at once on a `reject` value: retrying a stale success would let it slip through behind a later refresh
  private def awaitReadEquals(read: () => CIO[Option[String]], expected: String, reject: Option[String], attempts: Int): CIO[Boolean] =
    read().fold(
      {
        case Some(v) if v == expected      => CIO.value(true)
        case Some(v) if reject.contains(v) => CIO.value(false)
        case _ if attempts <= 0            => CIO.value(false)
        case _                             => CIO.sleep(200.millis).flatMap(_ => awaitReadEquals(read, expected, reject, attempts - 1))
      },
      _ => if (attempts <= 0) CIO.value(false) else CIO.sleep(200.millis).flatMap(_ => awaitReadEquals(read, expected, reject, attempts - 1))
    )

  private def recoverKey(client: Client[CIO, String], key: String, attempts: Int): CIO[Boolean] =
    awaitReadEquals(() => client.get[String](key), key, None, attempts)

  private def recoverCached(client: Client[CIO, String], key: String, expected: String, stale: String, attempts: Int): CIO[Boolean] =
    awaitReadEquals(() => client.cached(Commands.get[String, String](key), 1.minute), expected, Some(stale), attempts)

  private def recoverAll(client: Client[CIO, String], keys: Vector[String]): CIO[Boolean] =
    keys.foldLeft(CIO.value(true))((acc, key) => acc.flatMap(ok => if (!ok) CIO.value(false) else recoverKey(client, key, 150)))

  // retries until the node accepts the write (e.g. a replica once it is promoted to master)
  private def writeDirect(container: FixedHostPortGenericContainer, port: Int, key: String, value: String, attempts: Int): CIO[Unit] =
    Eventually.converges(attempts, 200.millis)(() => CIO.blocking(cli(container, port, "set", key, value)))(_.contains("OK"))(out =>
      s"could not write $key on $port: $out"
    )

  private def masterId(container: FixedHostPortGenericContainer, port: Int): String = cli(container, port, "cluster", "myid").trim

  private def masterPortsExcludingVictim(container: FixedHostPortGenericContainer): Vector[Int] =
    clusterNodes(container, victim).filter(_.isMaster).map(_.port).filter(_ != victim)

  // queried on the node itself, so its own line carries the `myself` flag
  private def ownSlotCount(container: FixedHostPortGenericContainer, port: Int): Int =
    clusterNodes(container, port).collectFirst { case node if node.isMyself => node.ownedSlotCount }.getOrElse(0)

  private def reshard(container: FixedHostPortGenericContainer, fromId: String, toId: String, slots: Int): String =
    exec(
      container,
      "redis-cli",
      "--cluster",
      "reshard",
      s"127.0.0.1:$victim",
      "--cluster-from",
      fromId,
      "--cluster-to",
      toId,
      "--cluster-slots",
      slots.toString,
      "--cluster-yes"
    )

  test("the client recovers reads after a master crashes and its replica is promoted") {
    withContainers { container =>
      val seeds  = ports.map(p => Endpoint("127.0.0.1", p)).toVector
      // short refresh interval so the topology refresh keeps pace with the caller's retries during the election
      val config = SageConfig(topology = Topology.Cluster(seeds, ClusterConfig(minRefreshInterval = 500.millis)))
      val keys   = (1 to 30).map(i => s"failover:$i").toVector

      val program =
        formCluster(container).flatMap { _ =>
          connectAndUse(config) { client =>
            for {
              _           <- writeAll(client, keys)
              // recovering exactly the victim's keys proves the client re-homed onto the promoted replica
              onVictim    <- CIO.blocking(parseKeys(cli(container, victim, "keys", "*")))
              replicaPort <- CIO.blocking(victimReplicaPort(container))
              _           <- awaitReplicated(container, replicaPort, onVictim.toSet, 100)
              _           <- CIO.blocking(Try(cli(container, victim, "shutdown", "nosave")))
              recovered   <- recoverAll(client, onVictim)
            } yield {
              assert(onVictim.nonEmpty, "no keys landed on the victim master; cannot prove failover recovery")
              assert(recovered, "client did not recover the victim master's keys after its replica was promoted")
            }
          }
        }
      program.unsafeRun
    }
  }

  test("a cached read follows MOVED to the new owner after its slot is resharded off its master") {
    withContainers { container =>
      val seeds  = ports.map(p => Endpoint("127.0.0.1", p)).toVector
      val config = SageConfig(topology = Topology.Cluster(seeds, ClusterConfig(minRefreshInterval = 500.millis)))
      val keys   = (1 to 30).map(i => s"reshard:$i").toVector

      val program =
        formCluster(container).flatMap { _ =>
          connectAndUse(config) { client =>
            for {
              _         <- writeAll(client, keys)
              onVictim  <- CIO.blocking(parseKeys(cli(container, victim, "keys", "*")))
              probe      = onVictim.head
              first     <- client.cached(Commands.get[String, String](probe), 1.minute)
              dest      <- CIO.blocking(masterPortsExcludingVictim(container).head)
              fromId    <- CIO.blocking(masterId(container, victim))
              toId      <- CIO.blocking(masterId(container, dest))
              moved     <- CIO.blocking(ownSlotCount(container, victim))
              _         <- CIO.blocking(reshard(container, fromId, toId, moved))
              _         <- awaitClusterOk(container, 60)
              _         <- writeDirect(container, dest, probe, "reshard-fresh", 50)
              recovered <- recoverCached(client, probe, "reshard-fresh", probe, 150)
            } yield {
              assert(onVictim.nonEmpty, "no keys landed on the victim master; cannot prove reshard recovery")
              assertEquals(first, Some(probe))
              assert(recovered, "cached read did not observe the resharded slot's new owner's current value")
            }
          }
        }
      program.unsafeRun
    }
  }

  test("a cached read recovers from the promoted master after a failover, never serving the dead master's entry") {
    withContainers { container =>
      val seeds  = ports.map(p => Endpoint("127.0.0.1", p)).toVector
      val config = SageConfig(topology = Topology.Cluster(seeds, ClusterConfig(minRefreshInterval = 500.millis)))
      val keys   = (1 to 30).map(i => s"cachedfailover:$i").toVector

      val program =
        formCluster(container).flatMap { _ =>
          connectAndUse(config) { client =>
            for {
              _           <- writeAll(client, keys)
              onVictim    <- CIO.blocking(parseKeys(cli(container, victim, "keys", "*")))
              probe        = onVictim.head
              _           <- client.cached(Commands.get[String, String](probe), 1.minute)
              replicaPort <- CIO.blocking(victimReplicaPort(container))
              _           <- awaitReplicated(container, replicaPort, onVictim.toSet, 100)
              _           <- CIO.blocking(Try(cli(container, victim, "shutdown", "nosave")))
              _           <- writeDirect(container, replicaPort, probe, "failover-fresh", 150)
              recovered   <- recoverCached(client, probe, "failover-fresh", probe, 150)
            } yield {
              assert(onVictim.nonEmpty, "no keys landed on the victim master; cannot prove cached failover recovery")
              assert(recovered, "cached read did not observe the promoted master's current value after the victim crashed")
            }
          }
        }
      program.unsafeRun
    }
  }
}

class RedisClusterFailoverSuite extends ClusterFailoverSuite(Images.redis, "redis-server")

class ValkeyClusterFailoverSuite extends ClusterFailoverSuite(Images.valkey, "valkey-server")

package sage.integration.cluster

import scala.concurrent.duration.*

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import com.dimafeng.testcontainers.munit.TestContainersForEach
import kyo.compat.*

import sage.Bytes
import sage.client.internal.Client
import sage.cluster.{Node, Slot}
import sage.commands.{Commands, Connection}
import sage.integration.{Eventually, Images}

/**
  * Drives cluster failover recovery against a real multi-node cluster: three masters and their replicas in one container. When a master
  * crashes, the cluster promotes its replica automatically, and the client reconnects to the new owner. After losing the connection, the
  * client refreshes `CLUSTER SLOTS` and retries up to `maxRedirects` times. An election may take longer, so the test retries as an
  * application would while the client updates its topology.
  *
  * The fixed host ports are 7100-7105; see [[MultiNodeCluster]] for why a multi-node cluster cannot use mapped random ports.
  */
abstract class ClusterFailoverSuite(image: String, serverBinary: String)
  extends MultiNodeCluster(image, serverBinary, nodeCount = 6, replicasPerMaster = 1)
  with TestContainersForEach {

  private val victim = basePort // redis-cli --cluster-create makes the first nodes masters, so 7100 is a master with a replica to promote

  private def crashVictim(onVictim: Vector[String]): CIO[Int] =
    for {
      topology <- clusterTopology(victim)
      replica  <- required(s"a replica of victim $victim", topology.replicasForMaster(Node("127.0.0.1", victim)).headOption)
      // The replication barrier: poll the victim's own replica until it holds every victim-owned key. WAIT keys off the calling connection's last
      // write, so it would not cover writes the cluster client sent on its own routed connections; reading the replica directly proves recovery.
      _        <- Eventually(100)(onNode(replica.port)(_.keys("*")).satisfies(keys => onVictim.toSet.subsetOf(keys.toSet)))
      _        <- shutdown(node(victim))
    } yield replica.port

  // `cluster_state:ok` flips before every node will actually serve writes, so a freshly formed cluster can briefly answer CLUSTERDOWN; retry
  // each write across that warm-up window, as a real application would, so the failover the test means to exercise is not masked by a startup race
  private def seedVictim(client: Client[CIO, String], prefix: String): CIO[(String, Vector[String])] =
    inSequence((1 to 30).map(i => s"$prefix:$i"))(key => Eventually.succeeds(150, 200.millis)(client.set(key, key)))
      .flatMap(_ => onNode(victim)(_.keys("*")))
      .flatMap {
        case keys @ (probe +: _) => CIO.value((probe, keys))
        case _                   => CIO.fail(new AssertionError("no keys landed on the victim master; cannot prove failover recovery"))
      }

  // retries a transport error, but stops at once on the stale value: retrying a stale success would let it slip through behind a later refresh
  private def awaitRead(read: CIO[Option[String]], expected: String, stale: Option[String]): CIO[Unit] =
    Eventually
      .succeeds(150, 200.millis)(read.map(_.filter(v => v == expected || stale.contains(v))).flatMap(required("a fresh or stale read", _)))
      .is(expected)

  private def recoverAll(client: Client[CIO, String], keys: Vector[String]): CIO[Unit] =
    inSequence(keys)(key => awaitRead(client.get[String](key), key, None))

  // retries until the node accepts the write (e.g. a replica once it is promoted to master)
  private def writeDirect(port: Int, key: String, value: String): CIO[Unit] =
    Eventually.succeeds(150, 200.millis)(onNode(port)(_.set(key, value)).unit)

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

  clusterTest("distributed locks confirm acquisition and renewal on each master's replica") { (_, client) =>
    val keys = Vector("orders", "delta", "epsilon").map(tag => s"replicated-lock:{$tag}")
    clusterTopology(basePort).flatMap { topology =>
      val placed = keys.flatMap(key =>
        topology.nodeForSlot(Slot.of(Bytes.utf8(s"4:lock:$key"))).flatMap(owner => topology.replicasForMaster(owner).headOption.map((key, owner, _)))
      )
      assertEquals(placed.map(_._2).distinct.size, 3)
      inSequence(placed) { (key, owner, replica) =>
        onNode(replica.port) { reader =>
          for {
            _ <- reader.run(Connection.readonly)
            // Initial replica synchronization can finish after the cluster starts accepting writes.
            _ <- Eventually(100)(reader.run(Commands.role).satisfies(_.isConnectedReplica))
            _ <- awaitCalls(onNode(owner.port)(_.info("commandstats")), "wait", 2)(replicated =>
                   client.lock[String](900.millis).withLock(key, 5.seconds) {
                     reader.exists(s"4:lock:$key").is(1L) >> replicated >> reader.exists(s"4:lock:$key").is(1L)
                   }
                 )
          } yield ()
        }
      }
    }
  }

  clusterTest("the client recovers reads after a master crashes and its replica is promoted") { (_, client) =>
    for {
      // reading the failed node's keys proves that the client connected to the promoted replica.
      (_, onVictim) <- seedVictim(client, "failover")
      _             <- crashVictim(onVictim)
      _             <- recoverAll(client, onVictim)
    } yield ()
  }

  clusterTest("a cached read follows MOVED to the new owner after its slot is resharded off its master") { (container, client) =>
    for {
      (probe, _) <- seedVictim(client, "reshard")
      _          <- client.cached(Commands.get[String, String](probe), 1.minute).is(Some(probe))
      topology   <- clusterTopology(victim)
      dest       <- required("another master", topology.masters.find(_.port != victim))
      owned       = (0 until Slot.Count).flatMap(Slot.at).count(topology.nodeForSlot(_).exists(_.port == victim))
      fromId     <- onNode(victim)(_.clusterMyId)
      toId       <- onNode(dest.port)(_.clusterMyId)
      _          <- CIO.blocking(reshard(container, fromId, toId, owned))
      _          <- awaitClusterOk
      _          <- writeDirect(dest.port, probe, "reshard-fresh")
      _          <- awaitRead(client.cached(Commands.get[String, String](probe), 1.minute), "reshard-fresh", Some(probe))
    } yield ()
  }

  clusterTest("a cached read recovers from the promoted master after a failover, never serving the dead master's entry") { (_, client) =>
    for {
      (probe, onVictim) <- seedVictim(client, "cachedfailover")
      _                 <- client.cached(Commands.get[String, String](probe), 1.minute)
      replicaPort       <- crashVictim(onVictim)
      _                 <- writeDirect(replicaPort, probe, "failover-fresh")
      _                 <- awaitRead(client.cached(Commands.get[String, String](probe), 1.minute), "failover-fresh", Some(probe))
    } yield ()
  }
}

class RedisClusterFailoverSuite extends ClusterFailoverSuite(Images.redis, "redis-server")

class ValkeyClusterFailoverSuite extends ClusterFailoverSuite(Images.valkey, "valkey-server")

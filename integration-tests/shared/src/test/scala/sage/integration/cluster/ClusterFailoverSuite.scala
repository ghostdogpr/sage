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
import sage.integration.{ContainerClient, Images}

/**
  * Drives cluster failover recovery against a real multi-node cluster: three masters and their replicas in one container. When a master
  * crashes, the cluster promotes its replica automatically, and the client re-homes on its own — on the connection loss it force-refreshes
  * `CLUSTER SLOTS` and re-dispatches, bounded by `maxRedirects`. That bound is shorter than an election, so, as with a real application, the
  * caller retries across the failover window while the client refreshes the topology underneath.
  *
  * A cluster node announces a single address used for both gossip and clients, so the testcontainers-mapped random ports the other suites use
  * cannot work here: gossip needs an address the nodes reach each other on. The escape is a fixed 1:1 host-port mapping plus
  * `cluster-announce-ip 127.0.0.1`, so `127.0.0.1:<port>` resolves to the same node inside the container (gossip) and from the host (the test).
  * The cost is fixed host ports (7100-7105), which must be free on the host — the only single-host scheme a multi-node cluster admits short of
  * Linux-only host networking.
  */
abstract class ClusterFailoverSuite(image: String, serverBinary: String) extends munit.FunSuite with TestContainerForEach with ContainerClient {

  private val ports  = 7100 to 7105
  private val victim = 7100 // redis-cli --cluster-create makes the first nodes masters, so 7100 is a master with a replica to promote

  // each node needs its own cluster-config-file (they otherwise collide on nodes.conf); a low node-timeout keeps the failover election short
  override val containerDef: FixedHostPortGenericContainer.Def = {
    val starts = ports
      .map(p =>
        s"$serverBinary --port $p --cluster-enabled yes --cluster-config-file nodes-$p.conf --cluster-node-timeout 2000 " +
          // --repl-diskless-sync-delay 0: start the replica's initial sync immediately, not after the default 5s window, so it is a live copy
          // before the failover rather than still in wait_bgsave
          s"--cluster-announce-ip 127.0.0.1 --repl-diskless-sync-delay 0 --save '' --appendonly no --protected-mode no --daemonize yes"
      )
      .mkString("; ")
    FixedHostPortGenericContainer.Def(
      image,
      command = Seq("sh", "-c", s"$starts; tail -f /dev/null"),
      portBindings = ports.map(p => (p, p)).toSeq
    )
  }

  given ExecutionContext = munitExecutionContext

  // forming a six-node cluster and waiting out an election runs past munit's 30s default on a loaded CI box
  override def munitTimeout: Duration = 120.seconds

  private def exec(container: FixedHostPortGenericContainer, args: String*): String = {
    val result = container.execInContainer(args*)
    result.getStdout + result.getStderr
  }

  private def cli(container: FixedHostPortGenericContainer, port: Int, args: String*): String =
    exec(container, ("redis-cli" +: "-p" +: port.toString +: args)*)

  private def parseKeys(out: String): Vector[String] =
    out.split("\n").iterator.map(_.trim).filter(_.nonEmpty).toVector

  // the victim master's own replica, parsed from CLUSTER NODES (a `slave` line whose master id is the victim's own id)
  private def victimReplicaPort(container: FixedHostPortGenericContainer): Int = {
    val myId  = cli(container, victim, "cluster", "myid").trim
    val nodes = cli(container, victim, "cluster", "nodes")
    parseKeys(nodes).iterator
      .map(_.split("\\s+"))
      .collectFirst { case f if f.length > 3 && f(2).contains("slave") && f(3) == myId => f(1).split("@")(0).split(":")(1).toInt }
      .getOrElse(throw new RuntimeException(s"no replica found for victim $victim:\n$nodes"))
  }

  // the replication barrier: poll the victim's own replica until it holds every victim-owned key. WAIT keys off the calling connection's last
  // write, so it would not cover writes the cluster client sent on its own routed connections; reading the replica directly proves recovery.
  private def awaitReplicated(container: FixedHostPortGenericContainer, replicaPort: Int, expected: Set[String], attempts: Int): CIO[Unit] =
    CIO.blocking(parseKeys(cli(container, replicaPort, "keys", "*")).toSet).flatMap { have =>
      if (expected.subsetOf(have)) CIO.value(())
      else if (attempts <= 0) CIO.fail(new RuntimeException(s"victim's replica did not catch up; missing ${(expected -- have).take(5)}"))
      else CIO.sleep(100.millis).flatMap(_ => awaitReplicated(container, replicaPort, expected, attempts - 1))
    }

  // wait for every node to answer, form the cluster (idempotent across a re-run sharing the container), and wait for it to converge
  private def formCluster(container: FixedHostPortGenericContainer): CIO[Unit] =
    awaitPortsUp(container, 60).flatMap { _ =>
      CIO.blocking(cli(container, victim, "cluster", "info").contains("cluster_state:ok")).flatMap { ok =>
        if (ok) CIO.value(())
        else
          CIO
            .blocking {
              val create = Vector("redis-cli", "--cluster", "create") ++ ports.map(p => s"127.0.0.1:$p") ++
                Vector("--cluster-replicas", "1", "--cluster-yes")
              exec(container, create*)
            }
            .flatMap(_ => awaitClusterOk(container, 60))
      }
    }

  private def awaitPortsUp(container: FixedHostPortGenericContainer, attempts: Int): CIO[Unit] =
    CIO.blocking(ports.forall(p => cli(container, p, "ping").contains("PONG"))).flatMap { up =>
      if (up) CIO.value(())
      else if (attempts <= 0) CIO.fail(new RuntimeException("cluster nodes did not start"))
      else CIO.sleep(300.millis).flatMap(_ => awaitPortsUp(container, attempts - 1))
    }

  private def awaitClusterOk(container: FixedHostPortGenericContainer, attempts: Int): CIO[Unit] =
    CIO.blocking(cli(container, victim, "cluster", "info").contains("cluster_state:ok")).flatMap { ok =>
      if (ok) CIO.value(())
      else if (attempts <= 0) CIO.fail(new RuntimeException("cluster did not converge"))
      else CIO.sleep(500.millis).flatMap(_ => awaitClusterOk(container, attempts - 1))
    }

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

  // retry a read across the failover window until the client refreshes onto the promoted master; each key stores its own name
  private def recoverKey(client: Client[CIO, String], key: String, attempts: Int): CIO[Boolean] =
    client
      .get[String](key)
      .fold(
        {
          case Some(v) if v == key => CIO.value(true)
          case _ if attempts <= 0  => CIO.value(false)
          case _                   => CIO.sleep(200.millis).flatMap(_ => recoverKey(client, key, attempts - 1))
        },
        _ => if (attempts <= 0) CIO.value(false) else CIO.sleep(200.millis).flatMap(_ => recoverKey(client, key, attempts - 1))
      )

  private def recoverAll(client: Client[CIO, String], keys: Vector[String]): CIO[Boolean] =
    keys.foldLeft(CIO.value(true))((acc, key) => acc.flatMap(ok => if (!ok) CIO.value(false) else recoverKey(client, key, 150)))

  private def recoverCached(client: Client[CIO, String], key: String, attempts: Int): CIO[Boolean] =
    client
      .cached(Commands.get[String, String](key), 1.minute)
      .fold(
        {
          case Some(v) if v == key => CIO.value(true)
          case _ if attempts <= 0  => CIO.value(false)
          case _                   => CIO.sleep(200.millis).flatMap(_ => recoverCached(client, key, attempts - 1))
        },
        _ => if (attempts <= 0) CIO.value(false) else CIO.sleep(200.millis).flatMap(_ => recoverCached(client, key, attempts - 1))
      )

  private def masterId(container: FixedHostPortGenericContainer, port: Int): String = cli(container, port, "cluster", "myid").trim

  // queried via the victim, so its own line carries the `myself` flag
  private def clusterNodeLines(container: FixedHostPortGenericContainer): Vector[Array[String]] =
    parseKeys(cli(container, victim, "cluster", "nodes")).map(_.split("\\s+"))

  private def otherMasterPort(container: FixedHostPortGenericContainer): Int =
    clusterNodeLines(container).iterator
      .filter(f => f.length > 2 && f(2).contains("master"))
      .map(f => f(1).split("@")(0).split(":")(1).toInt)
      .find(_ != victim)
      .getOrElse(throw new RuntimeException("no other master found to receive resharded slots"))

  // slot tokens are `start-end` or a single slot; `[...]` migration markers are not owned slots
  private def slotCount(fields: Array[String]): Int =
    fields
      .drop(8)
      .filterNot(_.startsWith("["))
      .map { token =>
        val parts = token.split("-")
        if (parts.length == 2) parts(1).toInt - parts(0).toInt + 1 else 1
      }
      .sum

  private def victimSlotCount(container: FixedHostPortGenericContainer): Int =
    clusterNodeLines(container).collectFirst { case f if f.length > 2 && f(2).contains("myself") => slotCount(f) }.getOrElse(0)

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
              fromId    <- CIO.blocking(masterId(container, victim))
              toPort    <- CIO.blocking(otherMasterPort(container))
              toId      <- CIO.blocking(masterId(container, toPort))
              moved     <- CIO.blocking(victimSlotCount(container))
              _         <- CIO.blocking(reshard(container, fromId, toId, moved))
              _         <- awaitClusterOk(container, 60)
              recovered <- recoverCached(client, probe, 150)
            } yield {
              assert(onVictim.nonEmpty, "no keys landed on the victim master; cannot prove reshard recovery")
              assertEquals(first, Some(probe))
              assert(recovered, "cached read did not follow MOVED to the resharded slot's new owner")
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
              recovered   <- recoverCached(client, probe, 150)
            } yield {
              assert(onVictim.nonEmpty, "no keys landed on the victim master; cannot prove cached failover recovery")
              assert(recovered, "cached read did not recover from the promoted master after the victim crashed")
            }
          }
        }
      program.unsafeRun
    }
  }
}

class RedisClusterFailoverSuite extends ClusterFailoverSuite(Images.redis, "redis-server")

class ValkeyClusterFailoverSuite extends ClusterFailoverSuite(Images.valkey, "valkey-server")

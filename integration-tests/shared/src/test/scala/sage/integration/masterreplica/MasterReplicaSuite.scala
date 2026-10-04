package sage.integration.masterreplica

import scala.concurrent.duration.*

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.{TestContainersForAll, TestContainersForEach}
import kyo.compat.*
import munit.{Location, TestOptions}

import sage.Message
import sage.SageException.{LockLost, TimedOut}
import sage.client.{Endpoint, MasterReplicaConfig, ReadFrom, SageConfig, Topology}
import sage.client.internal.Client
import sage.commands.Commands
import sage.integration.{ContainerClient, Eventually, Images}

/**
  * Shared setup for the master-replica suites. One container runs a master on port 6379 and a replica on port 6380. Like
  * [[sage.integration.cluster.ClusterSuite]], this setup keeps both processes in one container while testing `ROLE` discovery, a separate
  * replica-pool endpoint, [[ReadFrom]] routing, and replication. The replica advertises the host and port mapped by Testcontainers so that
  * the test can reach the address from the master's `ROLE` reply.
  */
abstract class MasterReplicaSuiteBase(image: String, serverBinary: String) extends ContainerClient {

  override type Containers = GenericContainer

  protected val masterPort  = 6379
  protected val replicaPort = 6380
  protected val marker      = "mr:replica-only-marker"

  // --protected-mode no admits the testcontainers-mapped (non-loopback) connection; --save '' / --appendonly no keep the nodes in-memory.
  // Both servers run in the background, so a fault can shut down either one without stopping the container.
  override def startContainers(): GenericContainer = {
    def server(port: Int) = s"$serverBinary --port $port --save '' --appendonly no --protected-mode no --repl-diskless-sync-delay 0"
    serverDef(image, Seq(masterPort, replicaPort), Seq("sh", "-c", s"${server(masterPort)} & ${server(replicaPort)} & exec tail -f /dev/null"))
      .start()
  }

  final protected class Deployment(server: GenericContainer) {
    private def at(port: Int) = Endpoint(server.host, server.mappedPort(port))
    val master: SageConfig    = SageConfig(topology = Topology.Standalone(at(masterPort)))
    val replica: SageConfig   = SageConfig(topology = Topology.Standalone(at(replicaPort)))

    def reading(policy: ReadFrom, minRefreshInterval: FiniteDuration = 5.seconds): SageConfig =
      SageConfig(
        topology = Topology.MasterReplica(Vector(at(masterPort)), MasterReplicaConfig(minRefreshInterval = minRefreshInterval)),
        readFrom = policy
      )
  }

  protected def replicationTest(options: TestOptions)(body: Deployment => CIO[Any])(using Location): Unit =
    containerTest(options)(server => body(new Deployment(server)))

  // Follow the master and announce the host-mapped port, then write a marker key the master never has, so a read's origin is observable. The
  // marker goes in after the link is up, since REPLICAOF triggers a full resync that would wipe an earlier write.
  override def afterContainersStart(server: GenericContainer): Unit =
    prepare(connectAndUse(new Deployment(server).replica) { replica =>
      replica.configSet("replica-announce-ip" -> server.host, "replica-announce-port" -> server.mappedPort(replicaPort).toString) >>
        replica.run(admin("REPLICAOF", "127.0.0.1", masterPort.toString)) >>
        // Valkey's async full-sync takes a few seconds even for an empty dataset, so budget generously under CI load
        Eventually(150)(replica.role.satisfies(_.isConnectedReplica)) >>
        replica.configSet("replica-read-only" -> "no") >>
        replica.set(marker, "from-replica") >>
        // restore read-only so a misrouted write to the replica fails loudly; replication still applies (it bypasses read-only) and the marker persists
        replica.configSet("replica-read-only" -> "yes")
    })
}

/**
  * Read routing and master-pinned operations against a real master-replica deployment. Routing is proven with a marker key that lives only on
  * the replica: a read that sees it was served by the replica, and one that does not was served by the master, which never has it.
  */
abstract class MasterReplicaSuite(image: String, serverBinary: String) extends MasterReplicaSuiteBase(image, serverBinary) with TestContainersForAll {

  private def lockFailsWhenReplicaStops(phase: String)(scenario: (Client[CIO, String], String, CIO[Unit]) => CIO[Unit]): Unit =
    replicationTest(s"distributed locks fail when a replica stops acknowledging $phase") { d =>
      connectAndUse(d.replica) { replica =>
        connectAndUse(d.reading(ReadFrom.Replica)) { client =>
          connectAndUse(d.master) { master =>
            val key = s"mr:unconfirmed-lock:$phase"
            CIO
              .ensure(replica.run(admin("CLIENT", "UNPAUSE")))(scenario(client, key, replica.run(admin("CLIENT", "PAUSE", "800", "ALL"))))
              .flatMap(_ => master.exists(s"4:lock:$key").is(0L))
          }
        }
      }
    }

  lockFailsWhenReplicaStops("acquisition") { (client, key, pause) =>
    pause
      .flatMap(_ =>
        failsWith[TimedOut](client.lock[String](600.millis).tryWithLock(key)(CIO.fail(new AssertionError("body ran without a confirmed lock"))))
      )
      .map(e => assert(e.getMessage.contains("replication confirmed by"), e.getMessage))
  }

  lockFailsWhenReplicaStops("renewal") { (client, key, pause) =>
    val stopped = new java.util.concurrent.atomic.AtomicBoolean(false)
    failsWith[LockLost](
      client.lock[String](600.millis).tryWithLock(key)(CIO.ensure(CIO.defer(stopped.set(true)))(pause.flatMap(_ => CIO.never)))
    )
      .map(_ => assert(stopped.get(), "the body was not interrupted"))
  }

  replicationTest("distributed locks acquire, renew, and release on the master with Replica reads") { d =>
    connectAndUse(d.reading(ReadFrom.Replica)) { client =>
      connectAndUse(d.master) { master =>
        client.get[String](marker).is(Some("from-replica")) >>
          awaitCalls(master.info("commandstats"), "wait", 2)(contend(client.lock(leaseDuration = 900.millis), master, "mr:distributed-lock", _))
      }
    }
  }

  replicationTest("distributed locks can skip replica acknowledgement") { d =>
    connectAndUse(d.reading(ReadFrom.Replica)) { client =>
      connectAndUse(d.master) { master =>
        for {
          before <- master.info("commandstats")
          _      <- client
                      .lock[String](leaseDuration = 900.millis, replicaAcknowledgement = false)
                      .tryWithLock("mr:no-replica-ack")(awaitRenewal(master, "4:lock:mr:no-replica-ack").map(_ => 42))
                      .is(Some(42))
          after  <- master.info("commandstats")
        } yield {
          assertEquals(commandCalls(after, "wait"), commandCalls(before, "wait"))
          assert(commandCalls(after, "role") > commandCalls(before, "role"))
        }
      }
    }
  }

  replicationTest("reads honor the ReadFrom policy and writes always reach the master") { d =>
    connectAndUse(d.reading(ReadFrom.Replica)) { client =>
      client.get[String](marker).is(Some("from-replica")) >>
        // the write always goes to the master; reading it back off the replica proves both replication and replica routing
        client.set("mr:k", "v") >>
        Eventually(50)(client.get[String]("mr:k").is(Some("v")))
    }
      .flatMap(_ => connectAndUse(d.reading(ReadFrom.Master))(_.get[String](marker).is(None)))
      .flatMap(_ => connectAndUse(d.reading(ReadFrom.ReplicaPreferred))(_.get[String](marker).is(Some("from-replica"))))
      .flatMap(_ => connectAndUse(d.reading(ReadFrom.MasterPreferred))(_.get[String](marker).is(None)))
  }

  replicationTest("transactions and pub/sub run on the master under the master-replica runtime") { d =>
    connectAndUse(d.reading(ReadFrom.ReplicaPreferred)) { client =>
      client.transaction(tx => tx.exec(Vector(Commands.incr[String]("mr:c"), Commands.incr[String]("mr:c")))).is(Some(Vector(1L, 2L))) >>
        withSubscription(client.subscribeChannels[String]("mr:news"))(sub =>
          client.publish("mr:news", "hello").is(1L) >> sub.next.is(Some(Message("mr:news", "hello")))
        )
    }
  }

  replicationTest("pub/sub works when a subscription is the client's first operation") { d =>
    connectAndUse(d.reading(ReadFrom.ReplicaPreferred)) { client =>
      withSubscription(client.subscribeChannels[String]("mr:first"))(sub =>
        client.publish("mr:first", "hello").is(1L) >> sub.next.is(Some(Message("mr:first", "hello")))
      )
    }
  }
}

/**
  * Faults that permanently change the deployment, so each test starts its own container.
  */
abstract class MasterReplicaFaultSuite(image: String, serverBinary: String)
  extends MasterReplicaSuiteBase(image, serverBinary)
  with TestContainersForEach {

  // Promotes the replica, then takes the old master out of write service without telling the client. The first write to the old master
  // triggers role discovery, and the caller retries against the promoted node.
  private def recoversAfterPromotion(fault: String)(retireOldMaster: Deployment => CIO[Unit]): Unit =
    replicationTest(s"the client recovers writes after the replica is promoted to master ($fault)") { d =>
      // short refresh interval so the event-driven re-discovery is not throttled away during the retry window
      connectAndUse(d.reading(ReadFrom.Master, minRefreshInterval = 100.millis)) { client =>
        client.set("fo:before", "v1") >>
          client.get[String]("fo:before").is(Some("v1")) >>
          connectAndUse(d.replica)(_.run(admin("REPLICAOF", "NO", "ONE"))) >>
          retireOldMaster(d) >>
          // the first attempt reaches the old master and starts discovery, and a later attempt reaches the promoted master
          Eventually.succeeds(50)(client.set("fo:after", "v2")) >>
          client.get[String]("fo:after").is(Some("v2"))
      }
    }

  // The old master follows the new master and answers writes with `READONLY`, which tests topology discovery after an ownership failure.
  recoversAfterPromotion("demoted master")(d => connectAndUse(d.master)(_.run(admin("REPLICAOF", "127.0.0.1", replicaPort.toString))))

  // The old master stops, so the next write gets a connection-refused error and discovery runs after a connection loss.
  recoversAfterPromotion("master down")(d => shutdown(d.master))

  // Both clients connect before the shutdown, so they use the failed replica instead of omitting it during discovery.
  replicationTest("a strict Replica read fails when the replica is down, while ReplicaPreferred falls back to the master") { d =>
    connectAndUse(d.reading(ReadFrom.Replica)) { strict =>
      connectAndUse(d.reading(ReadFrom.ReplicaPreferred)) { preferred =>
        // a master-backed key for the fallback read; the replica-only marker for the warm-up reads
        preferred.set("rd:k", "v") >>
          strict.get[String](marker).is(Some("from-replica")) >>
          preferred.get[String](marker).is(Some("from-replica")) >>
          shutdown(d.replica) >>
          failsWith[Throwable](strict.get[String]("rd:k")) >>
          Eventually(50)(preferred.get[String]("rd:k").is(Some("v")))
      }
    }
  }

  // With `replica-serve-stale-data no` and a broken link, the replica stays reachable but answers reads with `-MASTERDOWN`. 6399 listens to
  // nothing, so the link stays down.
  private def refuseStaleReads(replica: Client[CIO, String]): CIO[Unit] =
    replica.configSet("replica-serve-stale-data" -> "no") >>
      replica.run(admin("REPLICAOF", "127.0.0.1", "6399")) >>
      Eventually(100)(replica.role.satisfies(!_.isConnectedReplica))

  // Both clients connect before the link breaks, so they first try the stale replica.
  replicationTest("a replica answering MASTERDOWN falls through to the master under ReplicaPreferred, and fails a strict Replica read") { d =>
    connectAndUse(d.reading(ReadFrom.Replica)) { strict =>
      connectAndUse(d.reading(ReadFrom.ReplicaPreferred)) { preferred =>
        strict.get[String](marker).is(Some("from-replica")) >>
          preferred.get[String](marker).is(Some("from-replica")) >>
          connectAndUse(d.replica)(refuseStaleReads) >>
          preferred.set("sd:k", "v") >>
          preferred.get[String]("sd:k").is(Some("v")) >>
          preferred.pipeline(Seq.fill(2)(Commands.get[String, String]("sd:k"))).is(Vector(Some("v"), Some("v"))) >>
          failsWith[Throwable](strict.get[String]("sd:k"))
      }
    }
  }
}

class RedisMasterReplicaSuite extends MasterReplicaSuite(Images.redis, "redis-server")

class ValkeyMasterReplicaSuite extends MasterReplicaSuite(Images.valkey, "valkey-server")

class RedisMasterReplicaFaultSuite extends MasterReplicaFaultSuite(Images.redis, "redis-server")

class ValkeyMasterReplicaFaultSuite extends MasterReplicaFaultSuite(Images.valkey, "valkey-server")

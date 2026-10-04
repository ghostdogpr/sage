package sage.integration.cluster

import scala.concurrent.duration.*

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import kyo.compat.*
import munit.{Location, TestOptions}

import sage.client.{ClusterConfig, Endpoint, SageConfig, Topology}
import sage.client.internal.Client
import sage.cluster.{ClusterTopology, Node}
import sage.commands.Cluster
import sage.integration.{ContainerClient, Eventually}

/**
  * Boots several cluster nodes in one container and forms them into a cluster.
  *
  * A cluster node announces a single address used for both gossip and clients, so the testcontainers-mapped random ports the single-node
  * suites use cannot work here: gossip needs an address the nodes reach each other on. The escape is a fixed 1:1 host-port mapping plus
  * `cluster-announce-ip 127.0.0.1`, so `127.0.0.1:<port>` resolves to the same node inside the container (gossip) and from the host (the
  * test). The cost is fixed host ports, which must be free on the host.
  */
trait MultiNodeCluster(image: String, serverBinary: String, nodeCount: Int, replicasPerMaster: Int = 0) extends ContainerClient {

  override type Containers = FixedHostPortGenericContainer

  final protected val basePort = 7100

  final protected val ports: Range = basePort until basePort + nodeCount

  // waiting out an election runs past munit's 30s default on a loaded CI box
  override def munitTimeout: Duration = 120.seconds

  // short refresh interval so the topology refresh keeps pace with the caller's retries during an election
  protected val clusterConfig: SageConfig =
    SageConfig(topology = Topology.Cluster(ports.map(p => Endpoint("127.0.0.1", p)).toVector, ClusterConfig(minRefreshInterval = 500.millis)))

  protected def clusterTest(options: TestOptions)(body: (FixedHostPortGenericContainer, Client[CIO, String]) => CIO[Any])(using Location): Unit =
    containerTest(options)(container => connectAndUse(clusterConfig)(body(container, _)))

  // each node needs its own cluster-config-file (they otherwise collide on nodes.conf); a low node-timeout keeps a failover election short
  override def startContainers(): FixedHostPortGenericContainer = {
    val starts = ports
      .map(p =>
        Vector(
          serverBinary,
          s"--port $p",
          "--cluster-enabled yes",
          s"--cluster-config-file nodes-$p.conf",
          "--cluster-node-timeout 2000",
          "--cluster-announce-ip 127.0.0.1",
          // start a replica's initial sync immediately, not after the default 5s window, so it is a live copy before a failover
          "--repl-diskless-sync-delay 0",
          // on an idle master, a replica's replication offset first moves with this PING (every 10s by default)
          "--repl-ping-replica-period 1",
          "--save ''",
          "--appendonly no",
          "--protected-mode no",
          "--daemonize yes"
        ).mkString(" ")
      )
      .mkString("; ")
    FixedHostPortGenericContainer
      .Def(image, command = Seq("sh", "-c", s"$starts; tail -f /dev/null"), portBindings = ports.map(p => (p, p)).toSeq)
      .start()
  }

  override def afterContainersStart(container: FixedHostPortGenericContainer): Unit = prepare(formCluster(container))

  final protected def exec(container: FixedHostPortGenericContainer, args: String*): String = {
    val result = container.execInContainer(args*)
    result.getStdout + result.getStderr
  }

  final protected def node(port: Int): SageConfig = SageConfig(topology = Topology.Standalone(Endpoint("127.0.0.1", port)))

  final protected def onNode[A](port: Int)(body: Client[CIO, String] => CIO[A]): CIO[A] = connectAndUse(node(port))(body)

  // the slot owners and replicas as `port` sees them, decoded the way the client decodes them
  final protected def clusterTopology(port: Int): CIO[ClusterTopology] =
    onNode(port)(_.run(Cluster.slots(Node("127.0.0.1", port)))).map(ClusterTopology.from)

  private def awaitPortsUp: CIO[Unit] = Eventually.succeeds(60, 300.millis)(inSequence(ports)(onNode(_)(_.ping())))

  final protected def awaitClusterOk: CIO[Unit] =
    Eventually(60, 500.millis)(onNode(basePort)(_.clusterInfo).satisfies(_.contains("cluster_state:ok")))

  private def formCluster(container: FixedHostPortGenericContainer): CIO[Unit] = {
    val create = Vector("redis-cli", "--cluster", "create") ++ ports.map(p => s"127.0.0.1:$p") ++
      Vector("--cluster-replicas", replicasPerMaster.toString, "--cluster-yes")
    awaitPortsUp.flatMap(_ => CIO.blocking(exec(container, create*))).flatMap(_ => awaitClusterOk) >>
      // CLUSTER SLOTS lists a replica only once its replication offset is nonzero
      Eventually(100)(clusterTopology(basePort).satisfies(_.shards.forall(_.replicas.size == replicasPerMaster)))
  }
}

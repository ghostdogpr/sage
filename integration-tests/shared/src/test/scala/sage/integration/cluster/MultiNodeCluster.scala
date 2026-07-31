package sage.integration.cluster

import scala.concurrent.duration.*

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import kyo.compat.*

import sage.integration.Eventually

/**
  * One `CLUSTER NODES` row. The wire format is positional — `<id> <ip:port@cport> <flags> <master-id> …` with the owned slot ranges from
  * field 8 on — so it is parsed once here rather than indexed at each use site.
  */
final case class ClusterNode(id: String, port: Int, flags: Set[String], masterId: String, slots: Vector[Range]) {
  def isMaster: Boolean        = flags.contains("master")
  def isReplica: Boolean       = flags.contains("slave")
  def isMyself: Boolean        = flags.contains("myself")
  def owns(slot: Int): Boolean = slots.exists(_.contains(slot))
  def ownedSlotCount: Int      = slots.map(_.size).sum
}

/**
  * Boots several cluster nodes in one container and forms them into a cluster.
  *
  * A cluster node announces a single address used for both gossip and clients, so the testcontainers-mapped random ports the single-node
  * suites use cannot work here: gossip needs an address the nodes reach each other on. The escape is a fixed 1:1 host-port mapping plus
  * `cluster-announce-ip 127.0.0.1`, so `127.0.0.1:<port>` resolves to the same node inside the container (gossip) and from the host (the
  * test). The cost is fixed host ports, which must be free on the host and must not overlap between suites.
  */
trait MultiNodeCluster {

  protected def image: String
  protected def serverBinary: String
  protected def ports: Seq[Int]

  /**
    * Replicas per master for `--cluster create`; `None` makes every node a master.
    */
  protected def replicasPerMaster: Option[Int] = None

  /**
    * Server flags this suite needs on top of the shared ones.
    */
  protected def extraServerFlags: Vector[String] = Vector.empty

  // each node needs its own cluster-config-file (they otherwise collide on nodes.conf); a low node-timeout keeps a failover election short
  final protected def clusterContainerDef: FixedHostPortGenericContainer.Def = {
    val starts = ports
      .map(p =>
        (Vector(
          serverBinary,
          s"--port $p",
          "--cluster-enabled yes",
          s"--cluster-config-file nodes-$p.conf",
          "--cluster-node-timeout 2000",
          "--cluster-announce-ip 127.0.0.1"
        ) ++ extraServerFlags ++ Vector("--save ''", "--appendonly no", "--protected-mode no", "--daemonize yes")).mkString(" ")
      )
      .mkString("; ")
    FixedHostPortGenericContainer.Def(image, command = Seq("sh", "-c", s"$starts; tail -f /dev/null"), portBindings = ports.map(p => (p, p)).toSeq)
  }

  final protected def exec(container: FixedHostPortGenericContainer, args: String*): String = {
    val result = container.execInContainer(args*)
    result.getStdout + result.getStderr
  }

  final protected def cli(container: FixedHostPortGenericContainer, port: Int, args: String*): String =
    exec(container, ("redis-cli" +: "-p" +: port.toString +: args)*)

  /**
    * The cluster as `port` currently sees it, one entry per node.
    */
  final protected def clusterNodes(container: FixedHostPortGenericContainer, port: Int): Vector[ClusterNode] =
    cli(container, port, "cluster", "nodes").linesIterator.map(_.trim).filter(_.nonEmpty).flatMap(parseNode).toVector

  // a replica owns no slots, so its row stops at the link-state field and the slot tokens are absent
  private def parseNode(line: String): Option[ClusterNode] = {
    val fields = line.split("\\s+")
    Option.when(fields.length >= 8)(
      ClusterNode(
        id = fields(0),
        port = fields(1).split("@")(0).split(":").last.toInt,
        flags = fields(2).split(",").toSet,
        masterId = fields(3),
        // a `[slot-<-nodeid]` marker is an in-flight migration, not an owned slot
        slots = fields.drop(8).filterNot(_.startsWith("[")).toVector.flatMap(slotRange)
      )
    )
  }

  private def slotRange(token: String): Option[Range] =
    token.split("-") match {
      case Array(only)     => only.toIntOption.map(slot => slot to slot)
      case Array(from, to) => from.toIntOption.zip(to.toIntOption).map((low, high) => low to high)
      case _               => None
    }

  final protected def awaitPortsUp(container: FixedHostPortGenericContainer, attempts: Int): CIO[Unit] =
    Eventually.converges(attempts, 300.millis)(() => CIO.blocking(ports.forall(p => cli(container, p, "ping").contains("PONG"))))(identity)(_ =>
      "cluster nodes did not start"
    )

  final protected def awaitClusterOk(container: FixedHostPortGenericContainer, attempts: Int): CIO[Unit] =
    Eventually.converges(attempts, 500.millis)(() => CIO.blocking(clusterIsOk(container)))(identity)(_ => "cluster did not converge")

  private def clusterIsOk(container: FixedHostPortGenericContainer): Boolean =
    cli(container, ports.head, "cluster", "info").contains("cluster_state:ok")

  // idempotent, so a suite sharing one container across tests forms the cluster on the first test only
  final protected def formCluster(container: FixedHostPortGenericContainer, attempts: Int = 60): CIO[Unit] =
    awaitPortsUp(container, attempts).flatMap { _ =>
      CIO.blocking(clusterIsOk(container)).flatMap { ok =>
        if (ok) CIO.value(())
        else {
          val replicas = replicasPerMaster.toVector.flatMap(n => Vector("--cluster-replicas", n.toString))
          val create   = Vector("redis-cli", "--cluster", "create") ++ ports.map(p => s"127.0.0.1:$p") ++ replicas ++ Vector("--cluster-yes")
          CIO.blocking(exec(container, create*)).flatMap(_ => awaitClusterOk(container, attempts))
        }
      }
    }
}

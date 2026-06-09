package sage.integration.cluster

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.client.{Endpoint, SageConfig, Topology}
import sage.client.internal.Client
import sage.commands.Command
import sage.integration.{ContainerClient, Images}
import sage.protocol.Frame

/**
  * Drives the cluster runtime against a real cluster-enabled server. One node owns all 16384 slots, with `cluster-announce` pointed at the
  * testcontainers-mapped host port so the address the node reports in `CLUSTER SLOTS` is reachable from the test. This exercises topology
  * discovery, the `CLUSTER SLOTS` decoder against real wire output, and single-key routing; redirects and failover need multiple nodes.
  */
abstract class ClusterSuite(image: String, serverBinary: String) extends munit.FunSuite with TestContainerForAll with ContainerClient {

  override val containerDef: GenericContainer.Def[GenericContainer] =
    GenericContainer.Def(image, exposedPorts = Seq(6379), command = Seq(serverBinary, "--cluster-enabled", "yes"))

  given ExecutionContext = munitExecutionContext

  private def admin(name: String, args: String*): Command[Unit] =
    Command(name, Command.NoKeys, args.toVector.map(Bytes.utf8), _ => Right(()))

  private val clusterInfo: Command[String] =
    Command(
      "CLUSTER",
      Command.NoKeys,
      Vector(Bytes.utf8("INFO")),
      {
        case Frame.BulkString(bytes)        => Right(bytes.asUtf8String)
        case Frame.VerbatimString(_, bytes) => Right(bytes.asUtf8String)
        case other                          => Left(DecodeError("bulk or verbatim string", Frame.describe(other)))
      }
    )

  // a single node owning every slot, announcing the host-mapped endpoint so the address it reports is reachable from the test
  private def formSingleNodeCluster(admin0: Client[CIO], host: String, port: Int): CIO[Unit] =
    for {
      _ <- admin0.run(admin("CONFIG", "SET", "cluster-announce-ip", host))
      _ <- admin0.run(admin("CONFIG", "SET", "cluster-announce-port", port.toString))
      _ <- admin0.run(admin("CLUSTER", "ADDSLOTSRANGE", "0", "16383"))
      _ <- awaitClusterOk(admin0, 50)
    } yield ()

  private def awaitClusterOk(admin0: Client[CIO], attempts: Int): CIO[Unit] =
    admin0.run(clusterInfo).flatMap { info =>
      if (info.contains("cluster_state:ok")) CIO.value(())
      else if (attempts <= 0) CIO.fail(new RuntimeException(s"cluster did not converge: $info"))
      else CIO.sleep(100.millis).flatMap(_ => awaitClusterOk(admin0, attempts - 1))
    }

  test("single-key commands route against a real cluster") {
    withContainers { server =>
      val host       = server.host
      val port       = server.mappedPort(6379)
      val standalone = SageConfig(host = host, port = port)
      val clustered  = SageConfig(host = host, port = port, topology = Topology.Cluster(Vector(Endpoint(host, port))))

      val program =
        connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
          connectAndUse(clustered) { client =>
            for {
              _     <- client.set("greeting", "hello")
              value <- client.get[String, String]("greeting")
              count <- client.incr("counter")
            } yield {
              assertEquals(value, Some("hello"))
              assertEquals(count, 1L)
            }
          }
        }
      program.unsafeRun
    }
  }
}

class RedisClusterSuite extends ClusterSuite(Images.redis, "redis-server")

class ValkeyClusterSuite extends ClusterSuite(Images.valkey, "valkey-server")

package sage.integration.cluster

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*

import sage.{Bytes, Message}
import sage.SageException.DecodeError
import sage.client.{Endpoint, SageConfig, Topology}
import sage.client.internal.Client
import sage.commands.{Command, Commands, Pipeline}
import sage.commands.Pipeline.pipeline
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
      _    <- admin0.run(admin("CONFIG", "SET", "cluster-announce-ip", host))
      _    <- admin0.run(admin("CONFIG", "SET", "cluster-announce-port", port.toString))
      // idempotent across tests sharing one container: only claim the slots if they are not already assigned
      info <- admin0.run(clusterInfo)
      _    <- if (info.contains("cluster_state:ok")) CIO.value(()) else admin0.run(admin("CLUSTER", "ADDSLOTSRANGE", "0", "16383"))
      _    <- awaitClusterOk(admin0, 50)
    } yield ()

  private def awaitClusterOk(admin0: Client[CIO], attempts: Int): CIO[Unit] =
    admin0.run(clusterInfo).flatMap { info =>
      if (info.contains("cluster_state:ok")) CIO.value(())
      else if (attempts <= 0) CIO.fail(new RuntimeException(s"cluster did not converge: $info"))
      else CIO.sleep(100.millis).flatMap(_ => awaitClusterOk(admin0, attempts - 1))
    }

  // one shared container per suite, so the cluster is formed once and all routing exercised in a single test
  test("single-key commands, pipelines, and transactions route against a real cluster") {
    withContainers { server =>
      val host       = server.host
      val port       = server.mappedPort(6379)
      val standalone = SageConfig(host = host, port = port)
      val clustered  = SageConfig(host = host, port = port, topology = Topology.Cluster(Vector(Endpoint(host, port))))

      val program =
        connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
          connectAndUse(clustered) { client =>
            for {
              _      <- client.set("greeting", "hello")
              value  <- client.get[String, String]("greeting")
              count  <- client.incr("counter")
              _      <- client.set("{t}a", "1")
              _      <- client.set("{t}b", "2")
              piped  <- client.pipeline((Commands.get[String, String]("{t}a"), Commands.get[String, String]("{t}b")).pipeline)
              commit <- client.transaction(tx => tx.exec(Pipeline.sequence(Vector(Commands.incr[String]("{t}c"), Commands.incr[String]("{t}c")))))
            } yield {
              assertEquals(value, Some("hello"))
              assertEquals(count, 1L)
              assertEquals(piped, (Some("1"), Some("2")))
              assertEquals(commit, Some(Vector(1L, 2L)))
            }
          }
        }
      program.unsafeRun
    }
  }

  // sharded and classic pub/sub against a real (single-node) cluster: SSUBSCRIBE/SPUBLISH route by slot and coexist with classic SUBSCRIBE.
  // Resubscription on slot migration needs multiple nodes and is covered deterministically by ClusterClientSpec.
  test("sharded and classic pub/sub coexist on a cluster client") {
    withContainers { server =>
      val host       = server.host
      val port       = server.mappedPort(6379)
      val standalone = SageConfig(host = host, port = port)
      val clustered  = SageConfig(host = host, port = port, topology = Topology.Cluster(Vector(Endpoint(host, port))))

      val program =
        connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
          connectAndUse(clustered) { client =>
            for {
              shard    <- client.subscribeShardChannels[String]("orders")
              classic  <- client.subscribeChannels[String]("news")
              _        <- CIO.sleep(300.millis) // let both subscriptions register before publishing
              sCount   <- client.sPublish("orders", "placed")
              cCount   <- client.publish("news", "hello")
              sMsg     <- shard.next
              cMsg     <- classic.next
              channels <- client.pubsubShardChannels()
              _        <- shard.close
              _        <- classic.close
            } yield {
              assertEquals(sCount, 1L)
              assertEquals(cCount, 1L)
              assertEquals(sMsg, Some(Message("orders", "placed")))
              assertEquals(cMsg, Some(Message("news", "hello")))
              assert(channels.contains("orders"), channels)
            }
          }
        }
      program.unsafeRun
    }
  }
}

class RedisClusterSuite extends ClusterSuite(Images.redis, "redis-server")

class ValkeyClusterSuite extends ClusterSuite(Images.valkey, "valkey-server")

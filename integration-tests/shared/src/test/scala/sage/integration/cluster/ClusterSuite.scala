package sage.integration.cluster

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*

import sage.{Bytes, Message}
import sage.SageException.{DecodeError, ServerError}
import sage.client.{Endpoint, SageConfig, Topology}
import sage.client.internal.{Client, ScanTarget}
import sage.commands.{Command, Commands, FlushMode, ScanCursor}
import sage.integration.{ContainerClient, Images}
import sage.protocol.Frame

/**
  * Drives the cluster runtime against a real cluster-enabled server. One node owns all 16384 slots, with `cluster-announce` pointed at the
  * testcontainers-mapped host port so the address the node reports in `CLUSTER SLOTS` is reachable from the test. This exercises topology
  * discovery, the `CLUSTER SLOTS` decoder against real wire output, and single-key routing; redirects and failover need multiple nodes.
  */
abstract class ClusterSuite(image: String, serverBinary: String, supportsNumberedDatabases: Boolean = false)
  extends munit.FunSuite
  with TestContainerForAll
  with ContainerClient {

  override val containerDef: GenericContainer.Def[GenericContainer] = {
    val numberedDatabases = if (supportsNumberedDatabases) Seq("--cluster-databases", "16") else Seq.empty
    GenericContainer.Def(image, exposedPorts = Seq(6379), command = Seq(serverBinary, "--cluster-enabled", "yes") ++ numberedDatabases)
  }

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
  private def formSingleNodeCluster(admin0: Client[CIO, String], host: String, port: Int): CIO[Unit] =
    for {
      _    <- admin0.run(admin("CONFIG", "SET", "cluster-announce-ip", host))
      _    <- admin0.run(admin("CONFIG", "SET", "cluster-announce-port", port.toString))
      // idempotent across tests sharing one container: only claim the slots if they are not already assigned
      info <- admin0.run(clusterInfo)
      _    <- if (info.contains("cluster_state:ok")) CIO.value(()) else admin0.run(admin("CLUSTER", "ADDSLOTSRANGE", "0", "16383"))
      _    <- awaitClusterOk(admin0, 50)
    } yield ()

  private def awaitClusterOk(admin0: Client[CIO, String], attempts: Int): CIO[Unit] =
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
      val standalone = SageConfig(topology = Topology.Standalone(Endpoint(host, port)))
      val clustered  = SageConfig(topology = Topology.Cluster(Vector(Endpoint(host, port))))

      val program =
        connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
          connectAndUse(clustered) { client =>
            for {
              _      <- client.set("greeting", "hello")
              value  <- client.get[String]("greeting")
              count  <- client.incr("counter")
              _      <- client.set("{t}a", "1")
              _      <- client.set("{t}b", "2")
              piped  <- client.pipeline((Commands.get[String, String]("{t}a"), Commands.get[String, String]("{t}b")))
              commit <- client.transaction(tx => tx.exec(Vector(Commands.incr[String]("{t}c"), Commands.incr[String]("{t}c"))))
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

  test("supported cross-slot commands are transparently split and merged against a real cluster") {
    withContainers { server =>
      val host       = server.host
      val port       = server.mappedPort(6379)
      val standalone = SageConfig(topology = Topology.Standalone(Endpoint(host, port)))
      val clustered  = SageConfig(topology = Topology.Cluster(Vector(Endpoint(host, port))))
      val keyA       = "{mget-a}value"
      val keyB       = "{mget-b}value"
      val missingA   = "{mget-a}missing"
      val missingB   = "{mget-b}missing"
      val msetA      = "{mset-a}value"
      val msetB      = "{mset-b}value"

      val program =
        connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
          connectAndUse(clustered) { client =>
            for {
              _         <- client.set(keyA, "a")
              _         <- client.set(keyB, "b")
              values    <- client.mGet[String](keyA, keyB, missingA, keyB)
              piped     <- client.pipeline((Commands.mGet[String, String](keyA, keyB), Commands.get[String, String](keyA)))
              exists    <- client.exists(keyA, keyB, missingA, keyB)
              touched   <- client.touch(keyA, keyB, missingA)
              _         <- client.mSet(msetA -> "set-a", msetB -> "set-b")
              setValues <- client.mGet[String](msetA, msetB)
              deleted   <- client.del(keyA, missingB)
              unlinked  <- client.unlink(keyB, missingA)
            } yield {
              assertEquals(values, Vector(Some("a"), Some("b"), None, Some("b")))
              assertEquals(piped, (Vector(Some("a"), Some("b")), Some("a")))
              assertEquals(exists, 3L)
              assertEquals(touched, 2L)
              assertEquals(setValues, Vector(Some("set-a"), Some("set-b")))
              assertEquals(deleted, 1L)
              assertEquals(unlinked, 1L)
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
      val standalone = SageConfig(topology = Topology.Standalone(Endpoint(host, port)))
      val clustered  = SageConfig(topology = Topology.Cluster(Vector(Endpoint(host, port))))

      val program =
        connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
          connectAndUse(clustered) { client =>
            for {
              shard    <- client.subscribeShardChannels[String]("orders")
              classic  <- client.subscribeChannels[String]("news")
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
  // the keyless SCAN walk: scanTargets enumerates the slot-owning masters and runOn pins each page to one node (no rerouting), so the
  // sweep covers the whole keyspace. A single-node cluster owns every slot, so one target is expected; multi-master coverage rides the same path.
  test("scanAll sweeps every slot-owning master via node-pinned runOn") {
    withContainers { server =>
      val host       = server.host
      val port       = server.mappedPort(6379)
      val standalone = SageConfig(topology = Topology.Standalone(Endpoint(host, port)))
      val clustered  = SageConfig(topology = Topology.Cluster(Vector(Endpoint(host, port))))
      val expected   = (1 to 50).map(i => s"cscan:$i").toSet

      def writeKeys(client: Client[CIO, String], i: Int): CIO[Unit] =
        if (i > 50) CIO.value(()) else client.set(s"cscan:$i", i.toString).flatMap(_ => writeKeys(client, i + 1))

      def scanNode(client: Client[CIO, String], target: ScanTarget, cursor: ScanCursor, found: Set[String]): CIO[Set[String]] =
        client.runOn(target, Commands.scan[String](cursor, pattern = Some("cscan:*"), count = Some(10L))).flatMap { page =>
          page.next match {
            case Some(next) => scanNode(client, target, next, found ++ page.items)
            case None       => CIO.value(found ++ page.items)
          }
        }

      def sweep(client: Client[CIO, String], targets: Vector[ScanTarget], found: Set[String]): CIO[Set[String]] =
        targets match {
          case head +: tail => scanNode(client, head, ScanCursor.start, found).flatMap(sweep(client, tail, _))
          case _            => CIO.value(found)
        }

      val program =
        connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
          connectAndUse(clustered) { client =>
            for {
              _       <- writeKeys(client, 1)
              targets <- client.scanTargets
              found   <- sweep(client, targets, Set.empty[String])
            } yield {
              assert(targets.forall(_.node.isDefined), s"cluster scan targets must be node-pinned: $targets")
              assertEquals(found, expected)
            }
          }
        }
      program.unsafeRun
    }
  }

  // the All-Masters broadcast path: SCRIPT LOAD and FUNCTION LOAD fan out to every master, so a key-routed EVALSHA/FCALL finds them. One
  // node owns all slots here, so the broadcast reaches one master; multi-master fan-out rides the same dispatch branch.
  test("SCRIPT LOAD and FUNCTION LOAD broadcast so a key-routed EVALSHA and FCALL resolve") {
    withContainers { server =>
      val host       = server.host
      val port       = server.mappedPort(6379)
      val standalone = SageConfig(topology = Topology.Standalone(Endpoint(host, port)))
      val clustered  = SageConfig(topology = Topology.Cluster(Vector(Endpoint(host, port))))
      val library    =
        """#!lua name=clib
          |redis.register_function('clib_get', function(keys, args) return redis.call('get', keys[1]) end)
          |""".stripMargin

      val program =
        connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
          connectAndUse(clustered) { client =>
            for {
              sha   <- client.scriptLoad("return redis.call('get', KEYS[1])")
              _     <- client.set("bcast-key", "v")
              eval  <- client.evalSha(sha, Seq("bcast-key"))
              _     <- client.functionFlush(Some(FlushMode.Sync))
              name  <- client.functionLoad(library)
              fcall <- client.fCall("clib_get", Seq("bcast-key"))
            } yield {
              assertEquals(sha.length, 40)
              assertEquals(name, "clib")
              eval match {
                case Frame.BulkString(b) => assertEquals(b.asUtf8String, "v")
                case other               => fail(s"expected bulk string, got $other")
              }
              fcall match {
                case Frame.BulkString(b) => assertEquals(b.asUtf8String, "v")
                case other               => fail(s"expected bulk string, got $other")
              }
            }
          }
        }
      program.unsafeRun
    }
  }

  if (supportsNumberedDatabases)
    test("a numbered database is selected on a Valkey cluster connection") {
      withContainers { server =>
        val host       = server.host
        val port       = server.mappedPort(6379)
        val standalone = SageConfig(topology = Topology.Standalone(Endpoint(host, port)))
        val clustered  = SageConfig(topology = Topology.Cluster(Vector(Endpoint(host, port))), database = 2)
        val key        = "cluster-numbered-database"

        val program =
          connectAndUse(standalone)(formSingleNodeCluster(_, host, port)).flatMap { _ =>
            connectAndUse(standalone)(_.set(key, "database-0")).flatMap { _ =>
              connectAndUse(clustered) { client =>
                client.set(key, "database-2").flatMap(_ => client.get[String](key)).map(value => assertEquals(value, Some("database-2")))
              }.flatMap { _ =>
                connectAndUse(standalone)(_.get[String](key)).map(value => assertEquals(value, Some("database-0")))
              }
            }
          }
        program.unsafeRun
      }
    }

  if (!supportsNumberedDatabases)
    test("an unsupported cluster server rejects a numbered database during bootstrap") {
      withContainers { server =>
        val endpoint  = Endpoint(server.host, server.mappedPort(6379))
        val clustered = SageConfig(topology = Topology.Cluster(Vector(endpoint)), database = 2)

        val attempted: CIO[Unit] = connectAndUse(clustered)(_ => CIO.value(()))
        val program: CIO[Unit]   = attempted.fold(
          _ => CIO.fail(new AssertionError("expected the cluster connection to reject database 2")),
          error => CIO.value(assert(error.isInstanceOf[ServerError], s"expected the server's SELECT error, got $error"))
        )
        program.unsafeRun
      }
    }
}

class RedisClusterSuite extends ClusterSuite(Images.redis, "redis-server")

class ValkeyClusterSuite extends ClusterSuite(Images.valkey, "valkey-server", supportsNumberedDatabases = true)

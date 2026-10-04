package sage.integration.cluster

import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.lifecycle.and
import kyo.compat.*
import munit.{Location, TestOptions}

import sage.Message
import sage.SageException.ServerError
import sage.client.{Endpoint, SageConfig, Topology}
import sage.client.internal.{Client, Paged}
import sage.commands.{Commands, FlushMode}
import sage.integration.{BothServersSuite, Eventually, Images}
import sage.protocol.Frames

/**
  * Drives the cluster runtime against a real cluster-enabled server. One node owns all 16384 slots, with `cluster-announce` pointed at the
  * testcontainers-mapped host port so the address the node reports in `CLUSTER SLOTS` is reachable from the test. This exercises topology
  * discovery, the `CLUSTER SLOTS` decoder against real wire output, and single-key routing; redirects and failover need multiple nodes.
  */
class ClusterSuite extends BothServersSuite {

  override protected def redisDef: GenericContainer.Def[GenericContainer]  = serverDef(Images.redis, command = Seq("--cluster-enabled", "yes"))
  override protected def valkeyDef: GenericContainer.Def[GenericContainer] =
    serverDef(Images.valkey, command = Seq("--cluster-enabled", "yes", "--cluster-databases", "16"))

  override def afterContainersStart(containers: Containers): Unit = containers match {
    case redis and valkey => prepare(formCluster(redis) >> formCluster(valkey))
  }

  // a single node owning every slot, announcing the host-mapped endpoint so the address it reports is reachable from the test
  private def formCluster(server: GenericContainer): CIO[Unit] =
    connectAndUse(configOf(server)) { admin0 =>
      admin0.configSet("cluster-announce-ip" -> server.host, "cluster-announce-port" -> server.mappedPort(6379).toString) >>
        admin0.run(admin("CLUSTER", "ADDSLOTSRANGE", "0", "16383")) >>
        Eventually(50)(admin0.clusterInfo.satisfies(_.contains("cluster_state:ok")))
    }

  private def clusterConfig(server: GenericContainer, database: Int = 0): SageConfig =
    SageConfig(topology = Topology.Cluster(Vector(Endpoint(server.host, server.mappedPort(6379)))), database = database)

  private def clusterTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit =
    serverTest(options)(server => connectAndUse(clusterConfig(server))(body))

  // one shared container per server, so each cluster is formed once and all routing exercised in a single test
  clusterTest("single-key commands, pipelines, and transactions route against a real cluster") { client =>
    client.set("greeting", "hello") >>
      client.get[String]("greeting").is(Some("hello")) >>
      client.incr("counter").is(1L) >>
      client.set("{t}a", "1") >>
      client.set("{t}b", "2") >>
      client.pipeline((Commands.get[String, String]("{t}a"), Commands.get[String, String]("{t}b"))).is((Some("1"), Some("2"))) >>
      client.transaction(tx => tx.exec(Vector(Commands.incr[String]("{t}c"), Commands.incr[String]("{t}c")))).is(Some(Vector(1L, 2L)))
  }

  clusterTest("supported cross-slot commands are transparently split and merged against a real cluster") { client =>
    val keyA     = "{mget-a}value"
    val keyB     = "{mget-b}value"
    val missingA = "{mget-a}missing"
    val missingB = "{mget-b}missing"
    val msetA    = "{mset-a}value"
    val msetB    = "{mset-b}value"

    client.set(keyA, "a") >>
      client.set(keyB, "b") >>
      client.mGet[String](keyA, keyB, missingA, keyB).is(Vector(Some("a"), Some("b"), None, Some("b"))) >>
      client
        .pipeline((Commands.mGet[String, String](keyA, keyB), Commands.get[String, String](keyA)))
        .is((Vector(Some("a"), Some("b")), Some("a"))) >>
      client.exists(keyA, keyB, missingA, keyB).is(3L) >>
      client.touch(keyA, keyB, missingA).is(2L) >>
      client.mSet(msetA -> "set-a", msetB -> "set-b") >>
      client.mGet[String](msetA, msetB).is(Vector(Some("set-a"), Some("set-b"))) >>
      client.del(keyA, missingB).is(1L) >>
      client.unlink(keyB, missingA).is(1L)
  }

  // Sharded and classic pub/sub against a real (single-node) cluster: SSUBSCRIBE/SPUBLISH route by slot and coexist with classic SUBSCRIBE.
  // Resubscription on slot migration needs multiple nodes and is covered deterministically by ClusterClientSpec.
  clusterTest("sharded and classic pub/sub coexist on a cluster client") { client =>
    withSubscription(client.subscribeShardChannels[String]("orders")) { shard =>
      withSubscription(client.subscribeChannels[String]("news")) { classic =>
        client.sPublish("orders", "placed").is(1L) >>
          client.publish("news", "hello").is(1L) >>
          shard.next.is(Some(Message("orders", "placed"))) >>
          classic.next.is(Some(Message("news", "hello"))) >>
          client.pubsubShardChannels().satisfies(_.contains("orders"))
      }
    }
  }
  // scanTargets returns one target per slot-owning master, and each target runs every page on the node that created its cursor. This fixture
  // has one master for all slots, so one target is expected.
  clusterTest("scanAll sweeps every slot-owning master through node-pinned scan targets") { client =>
    val expected = (1 to 50).map(i => s"cscan:$i").toSet

    CIO.foreachDiscard(1 to 50)(i => client.set(s"cscan:$i", i.toString)) >>
      client.runner.scanTargets.satisfies(targets => targets.nonEmpty && !targets.contains(client.runner)) >>
      drain(Paged.scanAll[String](client.runner, Some("cscan:*"), Some(10L), None)).is(expected)
  }

  // SCRIPT LOAD and FUNCTION LOAD run on every master, allowing key-routed EVALSHA and FCALL to find them. This fixture has one master for
  // all slots, so the broadcast has one target. Multi-master clusters use the same dispatch logic.
  clusterTest("SCRIPT LOAD and FUNCTION LOAD broadcast so a key-routed EVALSHA and FCALL resolve") { client =>
    val library =
      """#!lua name=clib
        |redis.register_function('clib_get', function(keys, args) return redis.call('get', keys[1]) end)
        |""".stripMargin

    for {
      sha <- client.scriptLoad("return redis.call('get', KEYS[1])")
      _   <- client.set("bcast-key", "v")
      _   <- client.evalSha(sha, Seq("bcast-key")).is(Frames.bulk("v"))
      _   <- client.functionFlush(Some(FlushMode.Sync))
      _   <- client.functionLoad(library).is("clib")
      _   <- client.fCall("clib_get", Seq("bcast-key")).is(Frames.bulk("v"))
    } yield assertEquals(sha.length, 40)
  }

  serverTest("a cluster cached read is served locally and a server-side write evicts it via invalidation") { server =>
    connectAndUse(clusterConfig(server))(reader => connectAndUse(clusterConfig(server))(cachedReadIsInvalidated(reader, _, "csc:cluster")))
  }

  onValkey("a numbered database is selected on a Valkey cluster connection") { server =>
    val key = "cluster-numbered-database"
    connectAndUse(configOf(server))(_.set(key, "database-0")).flatMap { _ =>
      connectAndUse(clusterConfig(server, database = 2))(client =>
        client.set(key, "database-2").flatMap(_ => client.get[String](key).is(Some("database-2")))
      )
        .flatMap(_ => connectAndUse(configOf(server))(_.get[String](key).is(Some("database-0"))))
    }
  }

  onRedis("an unsupported cluster server rejects a numbered database during bootstrap") { server =>
    failsWith[ServerError](connectAndUse(clusterConfig(server, database = 2))(_ => CIO.unit))
  }
}

package sage.integration.cluster

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import kyo.compat.*

import sage.SageException.InvalidArgument
import sage.client.{Endpoint, SageConfig, Topology}
import sage.client.internal.Client
import sage.commands.Commands
import sage.integration.{ContainerClient, Eventually, Images}

/**
  * Drives the keyless broadcast routing against a real multi-master cluster: three masters, no replicas, in one container. A node answers
  * `PUBSUB` introspection only for the subscribers attached to it, so the merge across masters needs more than one master to be visible;
  * [[ClusterSuite]] forms a single-node cluster owning every slot and cannot express it. Subscribers are attached with `redis-cli` on chosen
  * nodes, so no assertion depends on which master Sage pins its own subscription connection to.
  *
  * Each backend takes its own port range so the Redis and Valkey rows cannot collide on the host.
  */
abstract class ClusterMultiMasterSuite(val image: String, val serverBinary: String, basePort: Int)
  extends munit.FunSuite
  with TestContainerForAll
  with ContainerClient
  with MultiNodeCluster {

  protected val ports: Seq[Int] = basePort to (basePort + 2)

  override val containerDef: FixedHostPortGenericContainer.Def = clusterContainerDef

  given ExecutionContext = munitExecutionContext

  override def munitTimeout: Duration = 120.seconds

  private val config = SageConfig(topology = Topology.Cluster(Vector(Endpoint("127.0.0.1", basePort))))

  private def subscribeOn(container: FixedHostPortGenericContainer, port: Int, verb: String, name: String): Unit = {
    exec(container, "sh", "-c", s"nohup redis-cli -p $port $verb $name > /dev/null 2>&1 &")
    ()
  }

  private def awaitOn(container: FixedHostPortGenericContainer, port: Int, form: String, name: String): CIO[Unit] =
    Eventually.converges(50, 100.millis)(() => CIO.blocking(cli(container, port, "pubsub", form)))(_.contains(name))(out =>
      s"$name did not appear in $form on $port: $out"
    )

  private def onCluster[A](body: (FixedHostPortGenericContainer, Client[CIO, String]) => CIO[A]): Future[A] =
    withContainers { container =>
      formCluster(container).flatMap(_ => connectAndUse(config)(client => body(container, client))).unsafeRun
    }

  test("PUBSUB CHANNELS returns a channel whose only subscriber sits on a master the client never picked") {
    onCluster { (container, client) =>
      ports.foreach(p => subscribeOn(container, p, "subscribe", s"only-$p"))
      val expected = ports.map(p => s"only-$p").toSet
      for {
        _        <- ports.foldLeft(CIO.value(()))((acc, p) => acc.flatMap(_ => awaitOn(container, p, "channels", s"only-$p")))
        channels <- client.pubsubChannels()
      } yield assert(expected.subsetOf(channels.toSet), s"swept channels $channels missed ${expected -- channels.toSet}")
    }
  }

  test("PUBSUB CHANNELS reports a channel held on two masters once, rather than once per master") {
    onCluster { (container, client) =>
      subscribeOn(container, ports(0), "subscribe", "twice")
      subscribeOn(container, ports(1), "subscribe", "twice")
      for {
        _        <- awaitOn(container, ports(0), "channels", "twice")
        _        <- awaitOn(container, ports(1), "channels", "twice")
        channels <- client.pubsubChannels()
      } yield assertEquals(channels.count(_ == "twice"), 1, s"expected one entry for a channel held on two masters, got $channels")
    }
  }

  test("PUBSUB NUMSUB sums a channel's subscribers across masters instead of reporting one master's count") {
    onCluster { (container, client) =>
      subscribeOn(container, ports(0), "subscribe", "summed")
      subscribeOn(container, ports(1), "subscribe", "summed")
      for {
        _      <- awaitOn(container, ports(0), "channels", "summed")
        _      <- awaitOn(container, ports(1), "channels", "summed")
        counts <- client.pubsubNumSub("summed")
      } yield assertEquals(counts.get("summed"), Some(2L))
    }
  }

  test("PUBSUB SHARDCHANNELS concatenates the shard channels of every master, one per shard") {
    onCluster { (container, client) =>
      val oneChannelPerShard                  = Vector("orders", "delta", "epsilon")
      oneChannelPerShard.foreach(channel => subscribeOn(container, ownerOfChannel(container, channel), "ssubscribe", channel))
      val sweptAll: Vector[String] => Boolean = found => oneChannelPerShard.forall(found.contains)
      Eventually.converges(50, 200.millis)(() => client.pubsubShardChannels())(sweptAll)(found =>
        s"swept shard channels $found missed ${oneChannelPerShard.filterNot(found.contains)}"
      )
    }
  }

  test("PUBSUB SHARDNUMSUB attributes each shard channel's subscribers to it, across all three owners") {
    onCluster { (container, client) =>
      // one channel per slot range, distinct counts: no single master's reply can satisfy this
      val expected = Map("sn-d" -> 1L, "sn-a" -> 2L, "sn-c" -> 3L)
      expected.foreach { case (channel, subscribers) =>
        val owner = ownerOfChannel(container, channel)
        (1L to subscribers).foreach(_ => subscribeOn(container, owner, "ssubscribe", channel))
      }
      Eventually.converges(50, 200.millis)(() => client.pubsubShardNumSub(expected.keys.toSeq*))(_ == expected)(counts =>
        s"expected $expected, got $counts"
      )
    }
  }

  test("PUBSUB NUMPAT sums the distinct patterns of every master") {
    onCluster { (container, client) =>
      subscribeOn(container, ports(0), "psubscribe", "pat-a.*")
      subscribeOn(container, ports(1), "psubscribe", "pat-b.*")
      Eventually.converges(50, 200.millis)(() => client.pubsubNumPat)(_ >= 2L)(total => s"expected both masters' patterns to be counted, got $total")
    }
  }

  test("MEMORY PURGE runs on every master, not just the one the client picked") {
    onCluster { (container, client) =>
      ports.foreach(p => cli(container, p, "config", "resetstat"))
      client.memoryPurge.map { _ =>
        val unpurged = ports.filterNot(p => cli(container, p, "info", "commandstats").contains("cmdstat_memory|purge:calls="))
        assert(unpurged.isEmpty, s"masters that never saw MEMORY PURGE: $unpurged")
      }
    }
  }

  test("a Pipeline rejects PUBSUB introspection, since a broadcast cannot be batched onto one node") {
    onCluster { (_, client) =>
      client
        .pipeline((Commands.pubsubNumPat, Commands.get[String, String]("unused")))
        .fold(
          results => CIO.value(fail(s"expected the pipeline to be rejected, got $results")),
          {
            case _: InvalidArgument => CIO.value(())
            case other              => CIO.value(fail(s"expected InvalidArgument, got $other"))
          }
        )
    }
  }

  private def ownerOfChannel(container: FixedHostPortGenericContainer, channel: String): Int = {
    val slot = cli(container, basePort, "cluster", "keyslot", channel).trim.toInt
    clusterNodes(container, basePort).find(node => node.isMaster && node.owns(slot)).map(_.port).getOrElse(fail(s"no master owns slot $slot"))
  }
}

class RedisClusterMultiMasterSuite extends ClusterMultiMasterSuite(Images.redis, "redis-server", 7200)

class ValkeyClusterMultiMasterSuite extends ClusterMultiMasterSuite(Images.valkey, "valkey-server", 7210)

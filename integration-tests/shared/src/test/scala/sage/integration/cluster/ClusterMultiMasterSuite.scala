package sage.integration.cluster

import scala.concurrent.duration.*

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import com.dimafeng.testcontainers.munit.TestContainersForAll
import kyo.compat.*

import sage.Bytes
import sage.SageException.InvalidArgument
import sage.cluster.Slot
import sage.commands.Commands
import sage.integration.{Eventually, Images}

/**
  * Drives the keyless broadcast routing against a real multi-master cluster: three masters, no replicas, in one container. A node answers
  * `PUBSUB` introspection only for the subscribers attached to it, so the merge across masters needs more than one master to be visible;
  * [[ClusterSuite]] forms a single-node cluster owning every slot and cannot express it. Subscribers are attached with `redis-cli` on chosen
  * nodes, so no assertion depends on which master Sage pins its own subscription connection to.
  */
abstract class ClusterMultiMasterSuite(image: String, serverBinary: String)
  extends MultiNodeCluster(image, serverBinary, nodeCount = 3)
  with TestContainersForAll {

  private def subscribeOn(container: FixedHostPortGenericContainer, port: Int, verb: String, name: String): Unit =
    exec(container, "sh", "-c", s"nohup redis-cli -p $port $verb $name > /dev/null 2>&1 &"): Unit

  private def subscribed(container: FixedHostPortGenericContainer, port: Int, channel: String): CIO[Unit] =
    CIO.blocking(subscribeOn(container, port, "subscribe", channel)) >>
      Eventually(50)(onNode(port)(_.pubsubChannels()).satisfies(_.contains(channel)))

  clusterTest("distributed locks acquire and release keys owned by different masters") { (_, first) =>
    val keys = Vector("orders", "delta", "epsilon").map(tag => s"distributed:{$tag}")
    slotOwner.map(owner => keys.map(key => owner(s"4:lock:$key")).toSet).is(ports.toSet) >>
      connectAndUse(clusterConfig)(second => inSequence(keys)(contend(first.lock(), second, _)))
  }

  clusterTest("PUBSUB CHANNELS returns a channel whose only subscriber sits on a master the client never picked") { (container, client) =>
    val expected = ports.map(p => s"only-$p").toSet
    inSequence(ports)(p => subscribed(container, p, s"only-$p")).flatMap(_ =>
      client.pubsubChannels().satisfies(channels => expected.subsetOf(channels.toSet))
    )
  }

  clusterTest("PUBSUB CHANNELS reports a channel held on two masters once, rather than once per master") { (container, client) =>
    inSequence(ports.take(2))(subscribed(container, _, "twice")).flatMap(_ => client.pubsubChannels().map(_.count(_ == "twice")).is(1))
  }

  clusterTest("PUBSUB NUMSUB sums a channel's subscribers across masters instead of reporting one master's count") { (container, client) =>
    inSequence(ports.take(2))(subscribed(container, _, "summed")).flatMap(_ => client.pubsubNumSub("summed").map(_.get("summed")).is(Some(2L)))
  }

  clusterTest("PUBSUB SHARDCHANNELS concatenates the shard channels of every master, one per shard") { (container, client) =>
    val oneChannelPerShard = Vector("orders", "delta", "epsilon")
    slotOwner.map(owner => oneChannelPerShard.foreach(channel => subscribeOn(container, owner(channel), "ssubscribe", channel))) >>
      Eventually(50, 200.millis)(client.pubsubShardChannels().satisfies(found => oneChannelPerShard.forall(found.contains)))
  }

  clusterTest("PUBSUB SHARDNUMSUB attributes each shard channel's subscribers to it, across all three owners") { (container, client) =>
    // Use one channel per slot range and give each a distinct subscriber count. The expected result therefore requires replies from all masters.
    val expected = Map("sn-d" -> 1L, "sn-a" -> 2L, "sn-c" -> 3L)
    slotOwner.map(owner =>
      expected.foreach((channel, subscribers) => (1L to subscribers).foreach(_ => subscribeOn(container, owner(channel), "ssubscribe", channel)))
    ) >>
      Eventually(50, 200.millis)(client.pubsubShardNumSub(expected.keys.toSeq*).is(expected))
  }

  clusterTest("PUBSUB NUMPAT sums the distinct patterns of every master") { (container, client) =>
    subscribeOn(container, basePort, "psubscribe", "pat-a.*")
    subscribeOn(container, basePort + 1, "psubscribe", "pat-b.*")
    Eventually(50, 200.millis)(client.pubsubNumPat.satisfies(_ >= 2L))
  }

  clusterTest("MEMORY PURGE runs on every master, not just the one the client picked") { (_, client) =>
    inSequence(ports)(onNode(_)(_.run(admin("CONFIG", "RESETSTAT")))) >>
      client.memoryPurge >>
      inSequence(ports)(onNode(_)(_.info("commandstats")).map(commandCalls(_, "memory|purge")).is(1L))
  }

  clusterTest("a Pipeline rejects PUBSUB introspection, since a broadcast cannot be batched onto one node") { (_, client) =>
    failsWith[InvalidArgument](client.pipeline((Commands.pubsubNumPat, Commands.get[String, String]("unused"))))
  }

  private def slotOwner: CIO[String => Int] =
    clusterTopology(basePort).map(topology => key => topology.nodeForSlot(Slot.of(Bytes.utf8(key))).fold(fail(s"no master owns $key"))(_.port))
}

class RedisClusterMultiMasterSuite extends ClusterMultiMasterSuite(Images.redis, "redis-server")

class ValkeyClusterMultiMasterSuite extends ClusterMultiMasterSuite(Images.valkey, "valkey-server")

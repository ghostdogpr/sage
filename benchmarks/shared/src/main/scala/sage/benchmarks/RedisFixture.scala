package sage.benchmarks

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.images.builder.Transferable

/**
  * A self-provisioned Redis for one JMH trial: started in `@Setup(Level.Trial)` and stopped in `@TearDown`, so every trial measures against a
  * fresh, isolated server. Pinned to the same image as the integration tests.
  */
object RedisFixture {
  val Image = "redis:8.8.0"

  def start(clusterEnabled: Boolean, value: String): GenericContainer = {
    val command     = if (clusterEnabled) Seq("redis-server", "--cluster-enabled", "yes") else Seq.empty
    val c           = GenericContainer(Image, exposedPorts = Seq(6379), command = command)
    c.start()
    // The node claims every slot and announces the mapped host and port, so the address it reports in CLUSTER SLOTS is reachable from the host.
    // A cluster that never reaches cluster_state:ok fails the seed check below with CLUSTERDOWN replies.
    val formCluster =
      if (!clusterEnabled) ""
      else
        s"redis-cli config set cluster-announce-ip ${c.host} cluster-announce-port ${c.mappedPort(6379)}; redis-cli cluster addslotsrange 0 16383; " +
          "for i in $(seq 100); do redis-cli cluster info | grep -q cluster_state:ok && break; sleep 0.1; done; "
    // redis-cli runs one command per input line; one SET per key avoids CROSSSLOT errors on the cluster-enabled server
    val commands    = Payloads.Keys.all.map(k => s"SET $k $value") ++ (0 until Payloads.HashFields).map(i => s"HSET ${Payloads.HashKey} f$i $value")
    c.container.copyFileToContainer(Transferable.of(commands.mkString("", "\n", "\n")), "/tmp/seed.txt")
    val replies     = c.execInContainer("sh", "-c", s"${formCluster}redis-cli < /tmp/seed.txt").getStdout
    val failures    = replies.linesIterator.filter(r => r.nonEmpty && r != "OK" && r.toLongOption.isEmpty).toVector
    if (failures.nonEmpty) throw new IllegalStateException(s"setup failed: ${failures.take(3).mkString("; ")}")
    c
  }
}

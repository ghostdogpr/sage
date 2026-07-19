package sage.benchmarks

import com.dimafeng.testcontainers.GenericContainer

/**
  * A self-provisioned Redis for one JMH trial: started in `@Setup(Level.Trial)` and stopped in `@TearDown`, so every trial measures against a
  * fresh, isolated server. Pinned to the same image as the integration tests.
  */
final class RedisFixture {

  private var container: Option[GenericContainer] = None

  var host: String = ""
  var port: Int    = 0

  def start(clusterEnabled: Boolean = false): Unit = {
    val command = if (clusterEnabled) Seq("redis-server", "--cluster-enabled", "yes") else Seq.empty
    val c       = GenericContainer(RedisFixture.Image, exposedPorts = Seq(6379), command = command)
    c.start()
    container = Some(c)
    host = c.host
    port = c.mappedPort(6379)
  }

  def stop(): Unit = {
    container.foreach(_.stop())
    container = None
  }
}

object RedisFixture {
  val Image = "redis:8.8.0"
}

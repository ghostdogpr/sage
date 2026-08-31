package sage.benchmarks

import org.openjdk.jmh.annotations.{Level, Setup, TearDown}

/**
  * Shared JMH state for running the same workload against standalone, cluster, and master-replica clients. Each client uses one provisioned
  * server. The cluster trial runs a `--cluster-enabled` server in a separate container. The result therefore includes server-mode and
  * instance differences as well as client dispatch.
  */
abstract class TopologyBenchState {

  val fixture: RedisFixture = new RedisFixture
  var subject: BenchClient  = null
  var keys: Array[String]   = Array.empty

  protected def topologyName: String

  protected def seedValueBytes: Int

  protected def buildClient(host: String, port: Int, topology: String): BenchClient

  @Setup(Level.Trial)
  def setupTrial(): Unit = {
    val cluster = topologyName == "cluster"
    fixture.start(clusterEnabled = cluster)
    if (cluster) ClusterFormation.formSingleNodeCluster(fixture.host, fixture.port)
    subject = buildClient(fixture.host, fixture.port, topologyName)
    keys = Payloads.keys("bench")
    subject.seed("bench", Payloads.KeyCount, Payloads.value(seedValueBytes), Payloads.HashKey, Payloads.HashFields)
  }

  @TearDown(Level.Trial)
  def tearDownTrial(): Unit = {
    if (subject != null) subject.close()
    fixture.stop()
  }
}

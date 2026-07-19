package sage.benchmarks

import org.openjdk.jmh.annotations.{Level, Setup, TearDown}

/**
  * Shared JMH state for the topology benchmarks: the same workload against the standalone, cluster, and master-replica runtimes, each on a
  * single self-provisioned server. The comparison is end-to-end per-topology overhead — the cluster trial runs a `--cluster-enabled` server
  * in its own container, so server-mode and instance variance are included, not client dispatch alone.
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

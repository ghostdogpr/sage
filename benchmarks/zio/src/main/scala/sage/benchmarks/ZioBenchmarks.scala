package sage.benchmarks

import org.openjdk.jmh.annotations.*

class ThroughputBench extends ThroughputBenchBase {

  @Param(Array("sage-zio", "zio-redis")) var client: String = "sage-zio"
}

class CollectionBench extends CollectionBenchBase {

  @Param(Array("sage-zio", "zio-redis")) var client: String = "sage-zio"
}

// Measures the throughput workload across Sage client topologies. The cluster trial runs a `--cluster-enabled` server in a separate
// container, so the result includes server-mode and instance differences as well as client dispatch.
class TopologyBench extends ThroughputWorkload {

  @Param(Array("sage-zio")) var client: String                                  = "sage-zio"
  @Param(Array("standalone", "cluster", "master-replica")) var topology: String = "standalone"
  @Param(Array("16")) var valueSize: Int                                        = 16

  override protected def clusterEnabled: Boolean                           = topology == "cluster"
  override protected def buildClient(host: String, port: Int): BenchClient = Clients.buildTopology(host, port, client, topology)
}

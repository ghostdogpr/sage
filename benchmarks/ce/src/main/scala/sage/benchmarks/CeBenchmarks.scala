package sage.benchmarks

import org.openjdk.jmh.annotations.Param

class ThroughputBench extends ThroughputBenchBase {

  @Param(Array("sage-ce", "redis4cats")) var client: String = "sage-ce"
}

class CollectionBench extends CollectionBenchBase {

  @Param(Array("sage-ce", "redis4cats")) var client: String = "sage-ce"
}

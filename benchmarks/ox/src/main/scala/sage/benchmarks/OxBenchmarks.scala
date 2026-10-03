package sage.benchmarks

import org.openjdk.jmh.annotations.Param

class ThroughputBench extends ThroughputBenchBase {

  @Param(Array("sage-ox", "lettuce", "rediscala", "jedis")) var client: String = "sage-ox"
}

class CollectionBench extends CollectionBenchBase {

  @Param(Array("sage-ox", "lettuce", "rediscala", "jedis")) var client: String = "sage-ox"
}

package sage.benchmarks

import org.openjdk.jmh.annotations.Param

class ThroughputBench extends ThroughputBenchBase {

  @Param(Array("sage-direct")) var client: String = "sage-direct"
}

class CollectionBench extends CollectionBenchBase {

  @Param(Array("sage-direct")) var client: String = "sage-direct"
}

package sage.benchmarks

import org.openjdk.jmh.annotations.Param

class ThroughputBench extends ThroughputBenchBase {

  @Param(Array("sage-pekko")) var client: String = "sage-pekko"
}

class CollectionBench extends CollectionBenchBase {

  @Param(Array("sage-pekko")) var client: String = "sage-pekko"
}

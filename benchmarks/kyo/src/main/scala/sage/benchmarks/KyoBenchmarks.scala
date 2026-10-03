package sage.benchmarks

import org.openjdk.jmh.annotations.Param

class ThroughputBench extends ThroughputBenchBase {

  @Param(Array("sage-kyo")) var client: String = "sage-kyo"
}

class CollectionBench extends CollectionBenchBase {

  @Param(Array("sage-kyo")) var client: String = "sage-kyo"
}

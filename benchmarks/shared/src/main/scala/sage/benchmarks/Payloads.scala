package sage.benchmarks

import scala.collection.immutable.ArraySeq

/**
  * Fixed shapes shared by every cell's benchmarks, so sage and the competitors are measured on identical data.
  */
object Payloads {

  /**
    * The number of keys a throughput/MGET workload touches per invocation.
    */
  final val KeyCount = 1000

  /**
    * Fields in the HGETALL hash.
    */
  final val HashFields = 1000

  final val HashKey = "bench:hash"

  // The split matches MGET's `(first, rest*)` signature without copying the keys on each call.
  object Keys {
    val first: String          = "bench:0"
    val rest: ArraySeq[String] = ArraySeq.tabulate(KeyCount - 1)(i => s"bench:${i + 1}")
    val all: Array[String]     = (first +: rest).toArray
    val set: Set[String]       = all.toSet
  }

  /**
    * The split of `Keys` into `concurrency` lanes, key `i` going to lane `i % concurrency`. Running each lane sequentially in its own fiber
    * bounds in-flight commands to `concurrency`.
    */
  final class Workload(val concurrency: Int) {
    require(concurrency > 0, s"concurrency must be positive, got $concurrency")
    val lanes: List[List[String]] = List.tabulate(concurrency)(lane => List.range(lane, Keys.all.length, concurrency).map(Keys.all(_)))
  }
}

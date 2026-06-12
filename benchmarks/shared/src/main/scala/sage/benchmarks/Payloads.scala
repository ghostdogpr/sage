package sage.benchmarks

/**
  * Fixed shapes shared by every cell's benchmarks, so sage and the competitors are measured on identical data.
  */
object Payloads {

  /**
    * The number of keys a throughput/MGET workload touches per invocation. Kept in sync with the literal in `@OperationsPerInvocation`.
    */
  final val KeyCount = 1000

  /**
    * Fields in the HGETALL hash.
    */
  final val HashFields = 1000

  final val HashKey = "bench:hash"

  def value(size: Int): String = "v" * size

  def keys(prefix: String): Array[String] = Array.tabulate(KeyCount)(i => s"$prefix:$i")

  /**
    * Split keys into `concurrency` near-equal groups; running each group sequentially in its own fiber bounds in-flight commands to `concurrency`.
    */
  def groups(keys: Array[String], concurrency: Int): Array[Array[String]] = {
    val g     = math.max(1, concurrency)
    val lanes = Array.fill(g)(Array.newBuilder[String])
    var i     = 0
    while (i < keys.length) {
      lanes(i % g) += keys(i)
      i += 1
    }
    lanes.map(_.result())
  }
}

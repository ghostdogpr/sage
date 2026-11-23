package sage.benchmarks

/**
  * One client under benchmark. Each method waits for its effect to finish, allowing JMH to measure the full round trip.
  */
trait BenchClient extends AutoCloseable {

  /**
    * GET every key with `work.concurrency` commands in flight.
    */
  def getAll(work: Payloads.Workload): Unit

  /**
    * SET every key to `value` with `work.concurrency` commands in flight.
    */
  def setAll(work: Payloads.Workload, value: String): Unit

  def mget(): Unit

  def hgetall(): Unit
}

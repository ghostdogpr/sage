package sage.benchmarks

import sage.client.internal.Client

/**
  * Sage through one backend's public `SageClient`. Every cell sends the same commands, and each cell defines only how its effect type runs and
  * waits.
  */
abstract class SageBench[F[_]] extends BenchClient {

  protected val client: Client[F, String]

  protected def run[A](effect: F[A]): Unit

  /**
    * Runs the lanes in parallel and the keys of each lane one after another.
    */
  protected def inLanes[A](work: Payloads.Workload)(perKey: String => F[A]): F[Any]

  def getAll(work: Payloads.Workload): Unit = run(inLanes(work)(client.get[String](_)))

  def setAll(work: Payloads.Workload, value: String): Unit = run(inLanes(work)(client.set(_, value)))

  def mget(): Unit = run(client.mGet[String](Payloads.Keys.first, Payloads.Keys.rest*))

  def hgetall(): Unit = run(client.hGetAll[String, String](Payloads.HashKey))

  def close(): Unit = run(client.close)
}

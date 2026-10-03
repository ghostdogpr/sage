package sage.benchmarks

import kyo.*

import sage.*
import sage.backend.*
import sage.client.{Endpoint, SageConfig, Topology}

/**
  * The Kyo benchmark cell contains only Sage because the suite has no native Kyo client to compare it with.
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-kyo" => new SageKyoBench(host, port)
    case other      => throw new IllegalArgumentException(s"unknown client: $other")
  }
}

private object Run {
  import AllowUnsafe.embrace.danger
  def apply[A](program: A < (Abort[Throwable] & Async)): A =
    KyoApp.Unsafe.runAndBlock(Duration.Infinity)(program).getOrThrow
}

final class SageKyoBench(host: String, port: Int) extends SageBench[[A] =>> A < (Abort[SageException] & Async)] {

  protected val client: SageClient =
    Run(SageClient.connect(SageConfig(topology = Topology.Standalone(Endpoint(host, port)))))

  protected def run[A](effect: A < (Abort[SageException] & Async)): Unit = Run(effect): Unit

  protected def inLanes[A](work: Payloads.Workload)(perKey: String => A < (Abort[SageException] & Async)): Unit < (Abort[SageException] & Async) =
    Async.foreachDiscard(work.lanes, work.concurrency)(Kyo.foreachDiscard(_)(perKey))
}

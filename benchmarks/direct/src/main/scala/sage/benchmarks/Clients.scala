package sage.benchmarks

import java.util.concurrent.Executors

import scala.util.Using

import sage.backend.*
import sage.client.{Endpoint, SageConfig, Topology}

/**
  * The direct benchmark cell contains only Sage. The Ox cell already measures Jedis and Lettuce from blocking code on virtual threads.
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-direct" => new SageDirectBench(host, port)
    case other         => throw new IllegalArgumentException(s"unknown client: $other")
  }
}

/**
  * Runs each lane on its own virtual thread, as a framework that serves each request on a virtual thread would.
  */
final class SageDirectBench(host: String, port: Int) extends SageBench[[A] =>> A] {

  protected val client: SageClient = SageClient.connect(SageConfig(topology = Topology.Standalone(Endpoint(host, port))))

  protected def run[A](result: A): Unit = ()

  protected def inLanes[A](work: Payloads.Workload)(perKey: String => A): Any =
    Using.resource(Executors.newVirtualThreadPerTaskExecutor()) { executor =>
      work.lanes.map(lane => executor.submit[Unit](() => lane.foreach(perKey))).foreach(_.get())
    }
}

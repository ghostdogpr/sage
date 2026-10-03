package sage.benchmarks

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import sage.*
import sage.backend.*
import sage.client.{Endpoint, SageConfig, Topology}

/**
  * The Pekko benchmark includes only Sage because there is no comparable Pekko-based Redis client in this benchmark suite.
  */
object Clients {
  def build(host: String, port: Int, name: String): BenchClient = name match {
    case "sage-pekko" => new SagePekkoBench(host, port)
    case other        => throw new IllegalArgumentException(s"unknown client: $other")
  }
}

final class SagePekkoBench(host: String, port: Int) extends SageBench[Future] {

  private given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "sage-bench")
  private given ExecutionContext             = system.executionContext

  protected val client: SageClient =
    Await.result(SageClient.connect(SageConfig(topology = Topology.Standalone(Endpoint(host, port)))), 30.seconds)

  protected def run[A](effect: Future[A]): Unit = Await.result(effect, 120.seconds): Unit

  protected def inLanes[A](work: Payloads.Workload)(perKey: String => Future[A]): Future[Any] =
    Future.traverse(work.lanes)(_.foldLeft(Future.unit)((previous, key) => previous.flatMap(_ => perKey(key).map(_ => ()))))

  override def close(): Unit = {
    super.close()
    system.terminate()
    Await.ready(system.whenTerminated, 10.seconds): Unit
  }
}

package sage.examples.pekko

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import sage.*
import sage.backend.*

/**
  * Runnable Pekko tour. A user-provided typed `ActorSystem` supplies the stream `Materializer` and the `ExecutionContext`; `use` connects the
  * client, shares it across every snippet, and closes it when the program finishes. Start a server on localhost:6379 first (see
  * examples/README.md), then `sbt examplesPekko/run`.
  */
object Tour {

  private val config = SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379)))

  def main(args: Array[String]): Unit = {
    given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "sage-tour")
    given ExecutionContext             = system.executionContext

    val program: Future[Unit] =
      SageClient.use(config) { client =>
        for {
          _ <- CommandsExample.run(client)
          _ <- PipelinesExample.run(client)
          _ <- TransactionsExample.run(client)
          _ <- PubSubExample.run(client)
          _ <- CachedReadsExample.run(client)
          _ <- RateLimiterExample.run(client)
          _ <- StreamsExample.run(client)
        } yield ()
      }

    try Await.result(program, 60.seconds)
    finally {
      system.terminate()
      val _ = Await.ready(system.whenTerminated, 10.seconds)
    }
  }
}

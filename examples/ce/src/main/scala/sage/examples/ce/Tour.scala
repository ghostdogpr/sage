package sage.examples.ce

import cats.effect.{IO, IOApp}

import sage.*
import sage.backend.*

/**
  * Runnable Cats Effect tour. It acquires the client as a `Resource` and shares the client across the examples. Start a server on
  * localhost:6379 as described in examples/README.md, then run `sbt examplesCe/run`.
  */
object Tour extends IOApp.Simple {

  private val config = SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379)))

  def run: IO[Unit] =
    SageClient.resource(config).use { client =>
      CommandsExample.run(client) *>
        PipelinesExample.run(client) *>
        TransactionsExample.run(client) *>
        PubSubExample.run(client) *>
        CachedReadsExample.run(client) *>
        RateLimiterExample.run(client) *>
        LockExample.run(client) *>
        StreamsExample.run(client)
    }
}

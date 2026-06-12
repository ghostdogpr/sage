package sage.examples.ce

import cats.effect.{IO, IOApp}

import sage.*
import sage.ce.*

/**
  * Runnable cats-effect tour. The client is acquired as a `Resource`, the idiomatic cats-effect construction form, and shared across every
  * snippet. Start a server on localhost:6379 first (see examples/README.md), then `sbt examplesCe/run`.
  */
object Tour extends IOApp.Simple {

  private val config = SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379)))

  def run: IO[Unit] =
    SageClient.resource(config).use { client =>
      CommandsExample.run(client) *>
        PipelinesExample.run(client) *>
        TransactionsExample.run(client) *>
        PubSubExample.run(client) *>
        CachedReadsExample.run(client)
    }
}

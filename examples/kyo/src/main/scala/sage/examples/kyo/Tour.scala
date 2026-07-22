package sage.examples.kyo

import kyo.*

import sage.*
import sage.backend.*

/**
  * Runnable Kyo tour. The client is opened with `scoped` and its `Scope` discharged with `Scope.run`, the idiomatic Kyo construction form,
  * then shared across every snippet. Start a server on localhost:6379 first (see examples/README.md), then `sbt exampleKyo`.
  */
object Tour extends KyoApp {

  private val config = SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379)))

  run {
    Scope.run {
      for {
        client <- SageClient.scoped(config)
        _      <- CommandsExample.run(client)
        _      <- PipelinesExample.run(client)
        _      <- TransactionsExample.run(client)
        _      <- PubSubExample.run(client)
        _      <- CachedReadsExample.run(client)
        _      <- RateLimiterExample.run(client)
        _      <- StreamsExample.run(client)
      } yield ()
    }
  }
}

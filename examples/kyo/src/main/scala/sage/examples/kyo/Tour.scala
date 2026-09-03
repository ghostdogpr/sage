package sage.examples.kyo

import kyo.*

import sage.*
import sage.backend.*

/**
  * Runnable Kyo tour. It opens the client with `scoped`, handles its `Scope` with `Scope.run`, and shares the client across the examples.
  * Start a server on localhost:6379 as described in examples/README.md, then run `sbt exampleKyo`.
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
        _      <- LockExample.run(client)
        _      <- StreamsExample.run(client)
      } yield ()
    }
  }
}

package sage.examples.ox

import ox.supervised

import sage.*
import sage.backend.*

/**
  * Runnable Ox tour. It opens the client with `scoped` inside a `supervised` concurrency scope and shares that client across the examples.
  * Start a server on localhost:6379 as described in examples/README.md, then run `sbt examplesOx/run`.
  */
@main def tour(): Unit = {
  val config = SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379)))
  supervised {
    val client = SageClient.scoped(config)
    CommandsExample.run(client)
    PipelinesExample.run(client)
    TransactionsExample.run(client)
    PubSubExample.run(client)
    CachedReadsExample.run(client)
    RateLimiterExample.run(client)
    StreamsExample.run(client)
  }
}

package sage.examples.ox

import ox.supervised

import sage.*
import sage.backend.*

/**
  * Runnable Ox tour. The client is opened with `scoped` inside a `supervised` concurrency scope — the idiomatic Ox direct-style construction
  * form — and shared across every snippet. Start a server on localhost:6379 first (see examples/README.md), then `sbt examplesOx/run`.
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

package sage.examples.direct

import sage.*
import sage.backend.*

/**
  * Runnable tour of the direct client. It opens the client with `use`, which closes it when the block returns, and shares that client across
  * the examples. Start a server on localhost:6379 as described in examples/README.md, then run `sbt examplesDirect/run`.
  */
@main def tour(): Unit = {
  val config = SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379)))
  SageClient.use(config) { client =>
    CommandsExample.run(client)
    PipelinesExample.run(client)
    TransactionsExample.run(client)
    PubSubExample.run(client)
    CachedReadsExample.run(client)
    RateLimiterExample.run(client)
    LockExample.run(client)
    StreamsExample.run(client)
  }
}

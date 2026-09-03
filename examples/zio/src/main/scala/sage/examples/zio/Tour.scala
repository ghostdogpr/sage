package sage.examples.zio

import zio.*

import sage.*
import sage.backend.*

/**
  * Runnable ZIO tour. It provides the client as a `ZLayer` and shares the client across the examples. Start a server on localhost:6379 as
  * described in examples/README.md, then run `sbt examplesZio/run`.
  */
object Tour extends ZIOAppDefault {

  private val config = SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379)))

  def run: ZIO[Any, Throwable, Unit] =
    (CommandsExample.run *>
      PipelinesExample.run *>
      TransactionsExample.run *>
      PubSubExample.run *>
      CachedReadsExample.run *>
      RateLimiterExample.run *>
      LockExample.run *>
      StreamsExample.run).provide(SageClient.layer(config))
}

package sage.examples.zio

import zio.*

import sage.*
import sage.zio.*

/**
  * Runnable ZIO tour. The client is wired as a `ZLayer` and shared across every snippet, the idiomatic ZIO construction form. Start a server
  * on localhost:6379 first (see examples/README.md), then `sbt examplesZio/run`.
  */
object Tour extends ZIOAppDefault {

  private val config = SageConfig(topology = Topology.Standalone(Endpoint("localhost", 6379)))

  def run: ZIO[Any, Throwable, Unit] =
    (CommandsExample.run *>
      PipelinesExample.run *>
      TransactionsExample.run *>
      PubSubExample.run *>
      CachedReadsExample.run *>
      StreamsExample.run).provide(SageClient.layer(config))
}

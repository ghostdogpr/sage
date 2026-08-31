package sage.examples.zio

import zio.*

import sage.*
import sage.backend.*

/**
  * Cluster example. The same client type discovers the topology from the configured seeds and routes each command to the owning node. This
  * example requires a running cluster, so the localhost `Tour` does not run it. It also demonstrates sharded pub/sub. `SSUBSCRIBE` and
  * `SPUBLISH` stay within the shard that owns the channel's slot.
  */
object ClusterExample {

  private val config =
    SageConfig(topology = Topology.Cluster(Vector(Endpoint("localhost", 7000), Endpoint("localhost", 7001))))

  val run: ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for {
        client   <- SageClient.scoped(config)
        stream   <- client.sSubscribeScoped[String]("orders")
        _        <- client.sPublish("orders", "placed")
        messages <- stream.take(1).runCollect
        _        <- Console.printLine(s"sharded=${messages.map(_.payload).toList}")
      } yield ()
    }
}

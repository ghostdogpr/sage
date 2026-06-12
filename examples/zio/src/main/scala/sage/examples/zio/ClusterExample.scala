package sage.examples.zio

import zio.*

import sage.*
import sage.zio.*

/**
  * Cluster spotlight. A cluster is just configuration — the same client type discovers the topology from the seeds and routes every command
  * to the owning node. This needs a running cluster, so it is not part of the localhost `Tour`; it exists to show the wiring and sharded
  * pub/sub, which only makes sense in a cluster (`SSUBSCRIBE`/`SPUBLISH` stay within the shard owning the channel's slot).
  */
object ClusterExample {

  private val config =
    SageConfig(topology = Topology.Cluster(Vector(Endpoint("localhost", 7000), Endpoint("localhost", 7001))))

  val run: ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for {
        client     <- SageClient.scoped(config)
        subscriber <- client.sSubscribe[String]("orders").take(1).runCollect.fork
        _          <- ZIO.sleep(Duration.fromMillis(300)) // let SSUBSCRIBE register before publishing
        _          <- client.sPublish("orders", "placed")
        messages   <- subscriber.join
        _          <- Console.printLine(s"sharded=${messages.map(_.payload).toList}")
      } yield ()
    }
}

package sage.examples.ox

import ox.Ox

import sage.*
import sage.backend.*

/**
  * Connects to a master-replica deployment. The client discovers node roles from the configured seeds, sends writes to the master, and routes
  * reads according to `ReadFrom.ReplicaPreferred`. The example expects servers on ports 6379 and 6380.
  */
object MasterReplicaExample {

  private val config =
    SageConfig(
      topology = Topology.MasterReplica(Vector(Endpoint("localhost", 6379), Endpoint("localhost", 6380))),
      readFrom = ReadFrom.ReplicaPreferred
    )

  def run(using Ox): Unit = {
    val client = SageClient.scoped(config)
    client.set("k", "v") // writes always go to the master
    val value = client.get[String]("k") // reads may be served by a replica, per the policy
    println(s"value=$value")
  }
}

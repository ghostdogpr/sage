package sage.examples.ox

import ox.Ox

import sage.*
import sage.ox.*

/**
  * Master-replica spotlight. The deployment is selected by `Topology.MasterReplica`; the same client discovers the master and its replicas
  * from the seeds and routes reads per the `ReadFrom` policy while writes always go to the master. This needs a master-replica deployment, so
  * it is not part of the localhost `Tour` — it exists to show the wiring.
  */
object MasterReplicaExample {

  private val config =
    SageConfig(
      topology = Topology.MasterReplica(Vector(Endpoint("localhost", 6379), Endpoint("localhost", 6380))),
      readFrom = ReadFrom.ReplicaPreferred
    )

  def run(using Ox): Unit = {
    val client = SageClient.scoped(config)
    val _      = client.set("k", "v")            // writes always go to the master
    val value  = client.get[String, String]("k") // reads may be served by a replica, per the policy
    println(s"value=$value")
  }
}

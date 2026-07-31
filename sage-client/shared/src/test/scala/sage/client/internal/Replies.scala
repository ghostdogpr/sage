package sage.client.internal

import sage.Bytes
import sage.cluster.{Node, Slot}
import sage.protocol.Frame

/**
  * The scripted server replies the client specs share: the RESP3 handshake, `ROLE` and `CLUSTER SLOTS`.
  */
object Replies {

  def bulk(value: String): Frame = Frame.BulkString(Bytes.utf8(value))

  val ok: Frame     = Frame.SimpleString("OK")
  val pong: Frame   = Frame.SimpleString("PONG")
  val queued: Frame = Frame.SimpleString("QUEUED")

  val hello: Frame =
    Frame.Map(
      Vector(
        bulk("server")  -> bulk("redis"),
        bulk("version") -> bulk("8.0.0"),
        bulk("proto")   -> Frame.Integer(3),
        bulk("role")    -> bulk("master")
      )
    )

  /**
    * One `CLUSTER SLOTS` entry: host, port, then the id the server appends.
    */
  private def nodeEntry(node: Node): Frame = Frame.Array(Vector(bulk(node.host), Frame.Integer(node.port.toLong), bulk(s"${node.host}-id")))

  def clusterSlots(ranges: (Node, Int, Int)*): Frame =
    Frame.Array(ranges.toVector.map { case (node, start, end) =>
      Frame.Array(Vector(Frame.Integer(start.toLong), Frame.Integer(end.toLong), nodeEntry(node)))
    })

  /**
    * A one-shard topology covering the whole slot space, its master followed by its replicas.
    */
  def clusterShard(master: Node, replicas: Node*): Frame =
    Frame.Array(
      Vector(
        Frame.Array(
          Vector(Frame.Integer(0L), Frame.Integer((Slot.Count - 1).toLong), nodeEntry(master)) ++ replicas.toVector.map(nodeEntry)
        )
      )
    )

  def masterRole(replicas: Node*): Frame =
    Frame.Array(
      Vector(
        bulk("master"),
        Frame.Integer(0L),
        Frame.Array(replicas.toVector.map(replica => Frame.Array(Vector(bulk(replica.host), bulk(replica.port.toString), bulk("0")))))
      )
    )

  def replicaRole(master: Node, state: String = "connected"): Frame =
    Frame.Array(Vector(bulk("slave"), bulk(master.host), Frame.Integer(master.port.toLong), bulk(state), Frame.Integer(0L)))
}

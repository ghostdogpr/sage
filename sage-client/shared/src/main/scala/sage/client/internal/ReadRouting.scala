package sage.client.internal

import sage.client.ReadFrom
import sage.cluster.Node
import sage.commands.Command

/**
  * The shared read-routing policy, used by both the cluster and master-replica runtimes. It decides nothing about connection liveness — it
  * only turns a [[ReadFrom]] policy plus a master and its replicas into the ordered list of candidate Nodes to try; the runtime walks that
  * list, skipping nodes it cannot establish, and falls back exactly as the order dictates.
  */
private[client] object ReadRouting {

  // an ordinary (non-blocking) read; writes, blocking reads, cursor-bound scans (whose cursor is node-local), and `cached` reads (gated
  // separately) never reach here
  def replicaEligible(command: Command[?]): Boolean = command.isReadOnly && !command.isBlocking && !command.cursorBound

  // the ordered candidates for an eligible read, round-robin-rotated by `rr` across the replicas; an empty result means strict Replica fails
  def candidates(readFrom: ReadFrom, master: Node, replicas: Vector[Node], rr: Int): Vector[Node] = {
    val rotated =
      if (replicas.isEmpty) Vector.empty
      else { val k = ((rr % replicas.length) + replicas.length) % replicas.length; replicas.drop(k) ++ replicas.take(k) }
    readFrom match {
      case ReadFrom.Master           => Vector(master)
      case ReadFrom.MasterPreferred  => master +: rotated
      case ReadFrom.Replica          => rotated
      case ReadFrom.ReplicaPreferred => rotated :+ master
    }
  }
}

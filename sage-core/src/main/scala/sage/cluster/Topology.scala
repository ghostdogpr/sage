package sage.cluster

import scala.collection.mutable

import sage.commands.{Command, Pipeline}

/**
  * One server process in a cluster or master-replica deployment, addressed by host and port. The only cluster type users name directly —
  * it appears on the [[sage.SageEvent]]s a [[sage.SageListener]] observes (which node served a command, the masters after a topology
  * change). Routing targets the runtime chooses are otherwise internal.
  */
final case class Node(host: String, port: Int)

/**
  * Inclusive on both ends.
  */
final private[sage] case class SlotRange(start: Slot, end: Slot)

final private[sage] case class Shard(master: Node, replicas: Vector[Node], slots: Vector[SlotRange])

/**
  * A pure snapshot of which masters own which slots. Routing and splitting are total classifications over it — they never raise, choose a
  * connection, or decide a retry.
  */
final private[sage] class ClusterTopology private (val shards: Vector[Shard], owners: Array[Node], shardOwners: Array[Shard]) {

  def nodeForSlot(slot: Slot): Option[Node] = Option(owners(slot.value))

  // the core only locates the owning shard; selecting a live replica and applying the read policy is the runtime's job
  def shardForSlot(slot: Slot): Option[Shard] = Option(shardOwners(slot.value))

  def route(command: Command[?]): Route =
    if (command.hasMalformedKeys) Route.Malformed
    else
      command.keyIndices.length match {
        case 0 => Route.Keyless
        case 1 => routeSlot(Slot.of(command.args(command.keyIndices.head)))
        case _ =>
          val keyIndices = command.keyIndices
          val first      = Slot.of(command.args(keyIndices.head))
          var i          = 1
          var crossed    = false
          while (i < keyIndices.length && !crossed) {
            if (Slot.of(command.args(keyIndices(i))) != first) crossed = true
            i += 1
          }
          if (crossed) Route.CrossSlot(slotsOf(command)) else routeSlot(first)
      }

  def split(pipeline: Pipeline[?, ?]): SplitPlan = {
    val perNode  = mutable.LinkedHashMap.empty[Node, mutable.ArrayBuffer[Int]]
    val keyless  = mutable.ArrayBuffer.empty[Int]
    val rejected = mutable.ArrayBuffer.empty[(Int, Rejected)]
    pipeline.commands.iterator.zipWithIndex.foreach { case (command, index) =>
      route(command) match {
        case Route.ToNode(node, _)  => perNode.getOrElseUpdate(node, mutable.ArrayBuffer.empty) += index
        case Route.Keyless          => keyless += index
        case Route.Unowned(slot)    => rejected += ((index, Rejected.Unowned(slot)))
        case Route.CrossSlot(slots) => rejected += ((index, Rejected.CrossSlot(slots)))
        case Route.Malformed        => rejected += ((index, Rejected.Malformed))
      }
    }
    SplitPlan(
      perNode.iterator.map { case (node, indices) => NodeGroup(node, indices.toVector) }.toVector,
      keyless.toVector,
      rejected.toVector
    )
  }

  private def routeSlot(slot: Slot): Route =
    nodeForSlot(slot) match {
      case Some(node) => Route.ToNode(node, slot)
      case None       => Route.Unowned(slot)
    }

  // safe to index args directly: route rejects out-of-range keyIndices as Malformed before calling this
  private def slotsOf(command: Command[?]): Set[Slot] =
    command.keyIndices.iterator.map(index => Slot.of(command.args(index))).toSet
}

private[sage] object ClusterTopology {

  /**
    * Total: uncovered slots stay unowned (routed as a refresh) and an overlapping range resolves last-listed-wins — the server is the
    * authority, the core only represents what it is given.
    */
  def from(shards: Vector[Shard]): ClusterTopology = {
    val owners      = new Array[Node](Slot.Count)
    val shardOwners = new Array[Shard](Slot.Count)
    shards.foreach { shard =>
      shard.slots.foreach { range =>
        var slot = range.start.value
        while (slot <= range.end.value) {
          owners(slot) = shard.master
          shardOwners(slot) = shard
          slot += 1
        }
      }
    }
    new ClusterTopology(shards, owners, shardOwners)
  }
}

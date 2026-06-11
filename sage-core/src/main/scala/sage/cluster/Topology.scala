package sage.cluster

import scala.collection.mutable

import sage.commands.{Command, Pipeline}

final case class Node(host: String, port: Int)

/**
  * Inclusive on both ends.
  */
final case class SlotRange(start: Slot, end: Slot)

final case class Shard(master: Node, replicas: Vector[Node], slots: Vector[SlotRange])

/**
  * A pure snapshot of which masters own which slots. Routing and splitting are total classifications over it — they never raise, choose a
  * connection, or decide a retry.
  */
final class ClusterTopology private (val shards: Vector[Shard], owners: Array[Node], shardOwners: Array[Shard]) {

  def nodeForSlot(slot: Slot): Option[Node] = Option(owners(slot.value))

  // the Shard owning a slot, exposing its replicas — the runtime's input for replica routing (the core only locates them; selecting a live
  // one and applying the read policy is the runtime's job, since it alone knows connection liveness)
  def shardForSlot(slot: Slot): Option[Shard] = Option(shardOwners(slot.value))

  def route(command: Command[?]): Route =
    if (command.hasMalformedKeys) Route.Malformed
    else
      command.keyIndices.length match {
        case 0 => Route.Keyless
        case 1 => routeSlot(Slot.of(command.args(command.keyIndices.head)))
        case _ =>
          val slots = slotsOf(command)
          if (slots.sizeIs > 1) Route.CrossSlot(slots) else routeSlot(slots.head)
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

object ClusterTopology {

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

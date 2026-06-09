package sage.cluster

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
final class ClusterTopology private (val shards: Vector[Shard], owners: Array[Node]) {

  def nodeForSlot(slot: Slot): Option[Node] = Option(owners(slot.value))

  def route(command: Command[?]): Route =
    if (command.keyIndices.exists(index => index < 0 || index >= command.args.length)) Route.Malformed
    else {
      val slots = slotsOf(command)
      if (slots.isEmpty) Route.Keyless
      else if (slots.sizeIs > 1) Route.CrossSlot(slots)
      else
        nodeForSlot(slots.head) match {
          case Some(node) => Route.ToNode(node, slots.head)
          case None       => Route.Unowned(slots.head)
        }
    }

  def split(pipeline: Pipeline[?, ?]): SplitPlan = {
    val routes    = pipeline.commands.zipWithIndex.map { case (command, index) => (index, route(command)) }
    val nodeOrder = routes.collect { case (_, Route.ToNode(node, _)) => node }.distinct
    val perNode   = nodeOrder.map { node =>
      NodeGroup(node, routes.collect { case (index, Route.ToNode(`node`, _)) => index })
    }
    val keyless   = routes.collect { case (index, Route.Keyless) => index }
    val rejected  = routes.collect {
      case (index, Route.Unowned(slot))    => (index, Rejected.Unowned(slot))
      case (index, Route.CrossSlot(slots)) => (index, Rejected.CrossSlot(slots))
      case (index, Route.Malformed)        => (index, Rejected.Malformed)
    }
    SplitPlan(perNode, keyless, rejected)
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
    val owners = new Array[Node](Slot.Count)
    shards.foreach { shard =>
      shard.slots.foreach { range =>
        var slot = range.start.value
        while (slot <= range.end.value) {
          owners(slot) = shard.master
          slot += 1
        }
      }
    }
    new ClusterTopology(shards, owners)
  }
}

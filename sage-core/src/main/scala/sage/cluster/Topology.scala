package sage.cluster

import scala.collection.mutable

import sage.commands.Command

/**
  * One server process in a cluster or master-replica deployment, addressed by host and port. [[sage.SageEvent]] values expose a `Node` to
  * identify the server that handled a command or the masters after a topology change. Other routing types remain internal.
  */
final case class Node(host: String, port: Int)

/**
  * One `CLUSTER SLOTS` row: the slots from `start` to `end`, inclusive on both ends, and the nodes that serve them.
  */
final private[sage] case class SlotRange(start: Slot, end: Slot, master: Node, replicas: Vector[Node])

final private[sage] case class Shard(master: Node, replicas: Vector[Node])

/**
  * Records the master and replicas for each slot. Routing and pipeline splitting return a classification for every command. The runtime uses
  * that result to choose connections and decide whether to retry.
  */
final private[sage] class ClusterTopology private (private val owners: Array[Shard]) {

  // derived from the owner array, in slot order, so broadcasts, scans and topology events use the same ownership as route
  val shards: Vector[Shard] = owners.iterator.filter(_ != null).distinct.toVector

  val masters: Vector[Node] = shards.map(_.master)

  private def masterAt(slot: Int): Node = {
    val shard = owners(slot)
    if (shard == null) null else shard.master
  }

  def nodeForSlot(slot: Slot): Option[Node] = Option(masterAt(slot.value))

  // compares the master for every slot so shard subscriptions stay in place when a refresh finds the same topology
  def sameOwnership(other: ClusterTopology): Boolean = (0 until Slot.Count).forall(slot => masterAt(slot) == other.masterAt(slot))

  def replicasForMaster(master: Node): Vector[Node] = shards.find(_.master == master).fold(Vector.empty[Node])(_.replicas)

  // masters in `previous` that no longer own a slot they hold in this (new) topology
  def mastersLosingSlots(previous: ClusterTopology): Set[Node] = {
    val losing = mutable.Set.empty[Node]
    var slot   = 0
    while (slot < Slot.Count) {
      val before = previous.masterAt(slot)
      if (before != null && !before.equals(masterAt(slot))) losing += before
      slot += 1
    }
    losing.toSet
  }

  def route(command: Command[?]): Route =
    if (command.hasMalformedKeys) Route.Malformed
    else if (command.keyIndices.isEmpty) Route.Keyless
    else {
      val keyIndices = command.keyIndices
      val first      = Slot.of(command.args(keyIndices(0)))
      var i          = 1
      while (i < keyIndices.length && Slot.of(command.args(keyIndices(i))) == first) i += 1
      if (i < keyIndices.length) Route.CrossSlot else routeSlot(first)
    }

  def split(commands: Vector[Command[?]]): SplitPlan = {
    val routes  = commands.map(route)
    val perNode = mutable.LinkedHashMap.empty[Node, (Shard, mutable.ArrayBuffer[Int])]
    // keyless positions join the first node's batch, which adopts this buffer so its positions stay ascending
    val first   = mutable.ArrayBuffer.empty[Int]
    routes.iterator.zipWithIndex.foreach {
      case (Route.ToNode(shard, _), index) =>
        perNode.getOrElseUpdate(shard.master, (shard, if (perNode.isEmpty) first else mutable.ArrayBuffer.empty))._2 += index
      case (Route.Keyless, index)          => first += index
      case _                               => ()
    }
    SplitPlan(routes, perNode.valuesIterator.map { case (shard, indices) => NodeGroup(shard, indices.toVector) }.toVector)
  }

  private def routeSlot(slot: Slot): Route = {
    val shard = owners(slot.value)
    if (shard == null) Route.Unowned(slot) else Route.ToNode(shard, slot)
  }
}

private[sage] object ClusterTopology {

  /**
    * Builds a topology even when slot ranges are incomplete or overlap. Uncovered slots remain unowned and trigger a refresh when routed.
    * For overlapping ranges, the last listed range owns the slot. A master's shard lists the replicas of all its ranges.
    */
  def from(ranges: Vector[SlotRange]): ClusterTopology = {
    val shardOf = ranges.groupMapReduce(_.master)(_.replicas)(_ ++ _).map((master, replicas) => master -> Shard(master, replicas.distinct))
    val owners  = new Array[Shard](Slot.Count)
    for {
      range <- ranges
      shard <- shardOf.get(range.master)
    } {
      var slot = range.start.value
      while (slot <= range.end.value) {
        owners(slot) = shard
        slot += 1
      }
    }
    new ClusterTopology(owners)
  }
}

package sage.cluster

final private[sage] case class NodeGroup(node: Node, positions: Vector[Int])

private[sage] enum Rejected {
  case CrossSlot(slots: Set[Slot])
  case Unowned(slot: Slot)
  case Malformed
}

/**
  * Describes how to run a pipeline across a cluster. `perNode` groups commands by target node and keeps their original positions. `keyless`
  * contains commands that can be added to any node group. `rejected` contains commands that cannot be planned. Each pipeline position appears
  * in exactly one of these collections.
  */
final private[sage] case class SplitPlan(
  perNode: Vector[NodeGroup],
  keyless: Vector[Int],
  rejected: Vector[(Int, Rejected)]
)

package sage.cluster

final private[sage] case class NodeGroup(node: Node, positions: Vector[Int])

private[sage] enum Rejected {
  case CrossSlot(slots: Set[Slot])
  case Unowned(slot: Slot)
  case Malformed
}

/**
  * The plan for running a Pipeline across a cluster. `perNode` groups keep their original positions so results merge back in submission
  * order; `keyless` positions the runtime folds into any group; `rejected` positions fail per-position. Every index appears in one bucket.
  */
final private[sage] case class SplitPlan(
  perNode: Vector[NodeGroup],
  keyless: Vector[Int],
  rejected: Vector[(Int, Rejected)]
)

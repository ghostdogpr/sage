package sage.cluster

final case class NodeGroup(node: Node, positions: Vector[Int])

enum Rejected {
  case CrossSlot(slots: Set[Slot])
  case Unowned(slot: Slot)
  case Malformed
}

/**
  * The plan for running a Pipeline across a cluster. `perNode` groups keep their original positions so results merge back in submission
  * order; `keyless` positions the runtime folds into any group; `rejected` positions fail per-position. Every index appears in one bucket.
  */
final case class SplitPlan(
  perNode: Vector[NodeGroup],
  keyless: Vector[Int],
  rejected: Vector[(Int, Rejected)]
)

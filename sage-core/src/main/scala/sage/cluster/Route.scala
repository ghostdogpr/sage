package sage.cluster

/**
  * Describes the routing result for one command in a [[ClusterTopology]]. The result can identify a node, allow any node for a keyless
  * command, report an unowned slot, or reject a cross-slot or malformed command. `Malformed` means the command declares key positions outside
  * its arguments.
  */
private[sage] enum Route {
  case ToNode(node: Node, slot: Slot)
  case Keyless
  case Unowned(slot: Slot)
  case CrossSlot(slots: Set[Slot])
  case Malformed
}

package sage.cluster

/**
  * The outcome of routing one command against a [[ClusterTopology]]: a pure classification, never an effect. The runtime decides what
  * each case means — send to the node, pick any node (`Keyless`), refresh the topology (`Unowned`), or fail (`CrossSlot`, `Malformed`).
  * `Malformed` means the command's declared key positions don't match its arguments — a programmer error the runtime fails, never routes.
  */
private[sage] enum Route {
  case ToNode(node: Node, slot: Slot)
  case Keyless
  case Unowned(slot: Slot)
  case CrossSlot(slots: Set[Slot])
  case Malformed
}

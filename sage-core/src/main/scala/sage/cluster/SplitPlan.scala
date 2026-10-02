package sage.cluster

final private[sage] case class NodeGroup(shard: Shard, positions: Vector[Int])

/**
  * Describes how to run a pipeline across a cluster. `routes` holds the route of every pipeline position. `perNode` groups the positions
  * routed to a node in their original order; the first group also holds every keyless position. With no group, keyless positions run alone.
  */
final private[sage] case class SplitPlan(routes: Vector[Route], perNode: Vector[NodeGroup])

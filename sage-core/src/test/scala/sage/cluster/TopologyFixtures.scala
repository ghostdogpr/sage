package sage.cluster

import sage.Bytes
import sage.commands.Command

/**
  * The synthetic commands and shards the routing specs share.
  */
object TopologyFixtures {

  def keyed(keys: String*): Command[Long] =
    Command("X", keys.indices.toVector, keys.toVector.map(Bytes.utf8), _ => Right(0L))

  val keyless: Command[Long] = Command("PING", Command.NoKeys, Vector.empty, _ => Right(0L))

  def covering(node: Node, from: Int, to: Int): SlotRange =
    SlotRange(Slot.at(from).get, Slot.at(to).get, node, Vector.empty)
}

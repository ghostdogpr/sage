package sage.cluster

import sage.Bytes
import sage.commands.Command

class TopologySpec extends munit.FunSuite {

  private val a = Node("a", 6379)
  private val b = Node("b", 6379)

  private def keyed(keys: String*): Command[Long] =
    Command("X", keys.indices.toVector, keys.toVector.map(Bytes.utf8), _ => Right(0L))

  private val keyless: Command[Long] = Command("PING", Command.NoKeys, Vector.empty, _ => Right(0L))

  private def covering(node: Node, from: Int, to: Int): Shard =
    Shard(node, Vector.empty, Vector(SlotRange(Slot.unsafe(from), Slot.unsafe(to))))

  private val whole = ClusterTopology.from(Vector(covering(a, 0, Slot.Count - 1)))

  test("nodeForSlot returns the owning master, None for an uncovered slot") {
    val partial = ClusterTopology.from(Vector(covering(a, 0, 100)))
    assertEquals(partial.nodeForSlot(Slot.unsafe(50)), Some(a))
    assertEquals(partial.nodeForSlot(Slot.unsafe(200)), None)
  }

  test("overlapping ranges resolve last-listed-wins") {
    val topo = ClusterTopology.from(Vector(covering(a, 0, 10), covering(b, 5, 20)))
    assertEquals(topo.nodeForSlot(Slot.unsafe(2)), Some(a))
    assertEquals(topo.nodeForSlot(Slot.unsafe(7)), Some(b))
    assertEquals(topo.nodeForSlot(Slot.unsafe(15)), Some(b))
  }

  test("shardForSlot exposes the owning shard's replicas, None for an uncovered slot") {
    val r1    = Node("r1", 6379)
    val shard = Shard(a, Vector(r1), Vector(SlotRange(Slot.unsafe(0), Slot.unsafe(100))))
    val topo  = ClusterTopology.from(Vector(shard))
    assertEquals(topo.shardForSlot(Slot.unsafe(50)).map(_.replicas), Some(Vector(r1)))
    assertEquals(topo.shardForSlot(Slot.unsafe(200)), None)
  }

  test("a single-slot command routes to its owner") {
    assertEquals(whole.route(keyed("foo")), Route.ToNode(a, Slot.of(Bytes.utf8("foo"))))
  }

  test("a multi-key command sharing a slot routes to one node") {
    assertEquals(whole.route(keyed("{tag}.a", "{tag}.b")), Route.ToNode(a, Slot.of(Bytes.utf8("tag"))))
  }

  test("a keyless command routes to any node") {
    assertEquals(whole.route(keyless), Route.Keyless)
  }

  test("a command whose keys span slots is classified cross-slot") {
    val sFoo = Slot.of(Bytes.utf8("foo"))
    val sBar = Slot.of(Bytes.utf8("bar"))
    assertNotEquals(sFoo, sBar)
    assertEquals(whole.route(keyed("foo", "bar")), Route.CrossSlot(Set(sFoo, sBar)))
  }

  test("a command on an uncovered slot is classified unowned") {
    val partial = ClusterTopology.from(Vector(covering(a, 0, 100)))
    assertEquals(partial.route(keyed("foo")), Route.Unowned(Slot.of(Bytes.utf8("foo"))))
  }

  test("a command whose key indices don't match its args is classified malformed, not routed") {
    assertEquals(whole.route(Command("BAD", Vector(5), Vector(Bytes.utf8("k")), _ => Right(0L))), Route.Malformed)
    assertEquals(whole.route(Command("BAD", Vector(-1), Vector(Bytes.utf8("k")), _ => Right(0L))), Route.Malformed)
  }

  test("sameOwnership compares the slot->owner mapping, ignoring shard order but catching a migration") {
    val ab        = ClusterTopology.from(Vector(covering(a, 0, 100), covering(b, 101, 200)))
    val reordered = ClusterTopology.from(Vector(covering(b, 101, 200), covering(a, 0, 100)))
    val migrated  = ClusterTopology.from(Vector(covering(a, 0, 150), covering(b, 151, 200)))
    assert(ab.sameOwnership(reordered), "same slot ownership regardless of shard order")
    assert(!ab.sameOwnership(migrated), "a slot moving from b to a is a change")
    assert(!whole.sameOwnership(ab), "differing covered ranges are a change")
  }
}

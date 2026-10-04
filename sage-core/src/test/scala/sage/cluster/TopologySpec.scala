package sage.cluster

import sage.Bytes
import sage.cluster.TopologyFixtures.{covering, keyed, keyless}
import sage.commands.Command

class TopologySpec extends munit.FunSuite {

  private val a = Node("a", 6379)
  private val b = Node("b", 6379)

  private val wholeShard = Shard(a, Vector.empty)
  private val whole      = ClusterTopology.from(Vector(covering(a, 0, Slot.Count - 1)))

  test("nodeForSlot returns the owning master, None for an uncovered slot") {
    val partial = ClusterTopology.from(Vector(covering(a, 0, 100)))
    assertEquals(partial.nodeForSlot(Slot.at(50).get), Some(a))
    assertEquals(partial.nodeForSlot(Slot.at(200).get), None)
  }

  test("overlapping ranges resolve last-listed-wins") {
    val topo = ClusterTopology.from(Vector(covering(a, 0, 10), covering(b, 5, 20)))
    assertEquals(topo.nodeForSlot(Slot.at(2).get), Some(a))
    assertEquals(topo.nodeForSlot(Slot.at(7).get), Some(b))
    assertEquals(topo.nodeForSlot(Slot.at(15).get), Some(b))
  }

  test("masters lists, in slot order, only the masters that route owns a slot for") {
    val c = Node("c", 6379)
    assertEquals(ClusterTopology.from(Vector(covering(a, 0, 10), covering(b, 11, 20))).masters, Vector(a, b))
    assertEquals(ClusterTopology.from(Vector(covering(a, 0, 100), covering(b, 0, 100))).masters, Vector(b))
    assertEquals(ClusterTopology.from(Vector(covering(c, 100, 50), covering(a, 0, 10))).masters, Vector(a))
  }

  test("a routed command carries the owning shard's replicas") {
    val r1    = Node("r1", 6379)
    val range = SlotRange(Slot.at(0).get, Slot.at(Slot.Count - 1).get, a, Vector(r1))
    assertEquals(ClusterTopology.from(Vector(range)).route(keyed("foo")), Route.ToNode(Shard(a, Vector(r1)), Slot.of(Bytes.utf8("foo"))))
  }

  test("ranges served by the same master form one shard listing the replicas of every range") {
    val (r1, r2) = (Node("r1", 6379), Node("r2", 6379))
    val ranges   = Vector(covering(a, 0, 10).copy(replicas = Vector(r1)), covering(a, 100, 110).copy(replicas = Vector(r2, r1)))
    assertEquals(ClusterTopology.from(ranges).shards, Vector(Shard(a, Vector(r1, r2))))
  }

  test("a single-slot command routes to its owner") {
    assertEquals(whole.route(keyed("foo")), Route.ToNode(wholeShard, Slot.of(Bytes.utf8("foo"))))
  }

  test("a multi-key command sharing a slot routes to one node") {
    assertEquals(whole.route(keyed("{tag}.a", "{tag}.b")), Route.ToNode(wholeShard, Slot.of(Bytes.utf8("tag"))))
  }

  test("a keyless command routes to any node") {
    assertEquals(whole.route(keyless), Route.Keyless)
  }

  test("a command whose keys span slots is classified cross-slot") {
    val sFoo = Slot.of(Bytes.utf8("foo"))
    val sBar = Slot.of(Bytes.utf8("bar"))
    assertNotEquals(sFoo, sBar)
    assertEquals(whole.route(keyed("foo", "bar")), Route.CrossSlot)
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

  test("mastersLosingSlots names only masters that gave up a slot, so a cache flush targets exactly them") {
    val before = ClusterTopology.from(Vector(covering(a, 0, 100), covering(b, 101, 200)))
    val after  = ClusterTopology.from(Vector(covering(a, 0, 150), covering(b, 151, 200)))
    assertEquals(after.mastersLosingSlots(before), Set(b))
    assert(!after.mastersLosingSlots(before).contains(a))
    assertEquals(before.mastersLosingSlots(before), Set.empty[Node])
    val shrunk = ClusterTopology.from(Vector(covering(a, 0, 100)))
    assertEquals(shrunk.mastersLosingSlots(before), Set(b))
  }
}

package sage.cluster

import sage.Bytes
import sage.cluster.TopologyFixtures.{keyed, keyless}
import sage.commands.Command

class SplitPlanSpec extends munit.FunSuite {

  private val a = Node("a", 6379)
  private val b = Node("b", 6379)

  private val sFoo = Slot.of(Bytes.utf8("foo"))
  private val sBar = Slot.of(Bytes.utf8("bar"))

  private val shardA  = Shard(a, Vector.empty)
  private val shardB  = Shard(b, Vector.empty)
  private val rangeA  = SlotRange(sFoo, sFoo, a, Vector.empty)
  private val twoNode = ClusterTopology.from(Vector(rangeA, SlotRange(sBar, sBar, b, Vector.empty)))

  test("commands group per node, positions kept in submission order") {
    val plan = twoNode.split(Vector(keyed("foo"), keyed("bar"), keyed("foo")))
    assertEquals(plan.perNode, Vector(NodeGroup(shardA, Vector(0, 2)), NodeGroup(shardB, Vector(1))))
  }

  test("keyless positions join the first group in submission order, rejected positions are left out") {
    val plan = twoNode.split(Vector(keyless, keyed("bar"), keyless, keyed("foo", "bar"), keyed("foo")))
    assertEquals(plan.perNode, Vector(NodeGroup(shardB, Vector(0, 1, 2)), NodeGroup(shardA, Vector(4))))
    assertEquals(plan.routes(3), Route.CrossSlot)
  }

  test("keyless positions stay out of every group when no command has a key") {
    val plan = twoNode.split(Vector(keyless, keyless))
    assertEquals(plan.perNode, Vector.empty)
    assertEquals(plan.routes, Vector(Route.Keyless, Route.Keyless))
  }

  test("an uncovered command is rejected as unowned, not dropped") {
    val onlyA = ClusterTopology.from(Vector(rangeA))
    val plan  = onlyA.split(Vector(keyed("foo"), keyed("bar")))
    assertEquals(plan.perNode, Vector(NodeGroup(shardA, Vector(0))))
    assertEquals(plan.routes(1), Route.Unowned(sBar))
  }

  test("a malformed command is rejected in place, not routed") {
    val malformed = Command("BAD", Vector(5), Vector(Bytes.utf8("k")), _ => Right(0L))
    val plan      = twoNode.split(Vector(keyed("foo"), malformed))
    assertEquals(plan.perNode, Vector(NodeGroup(shardA, Vector(0))))
    assertEquals(plan.routes(1), Route.Malformed)
  }

  test("an empty pipeline yields an empty plan") {
    val plan = twoNode.split(Vector.empty)
    assertEquals(plan, SplitPlan(Vector.empty, Vector.empty))
  }
}

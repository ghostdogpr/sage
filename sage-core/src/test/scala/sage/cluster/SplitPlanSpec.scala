package sage.cluster

import sage.Bytes
import sage.commands.{Command, Pipeline}

class SplitPlanSpec extends munit.FunSuite {

  private val a = Node("a", 6379)
  private val b = Node("b", 6379)

  private def keyed(keys: String*): Command[Long] =
    Command("X", keys.indices.toVector, keys.toVector.map(Bytes.utf8), _ => Right(0L))

  private val keyless: Command[Long] = Command("PING", Command.NoKeys, Vector.empty, _ => Right(0L))

  private val sFoo = Slot.of(Bytes.utf8("foo"))
  private val sBar = Slot.of(Bytes.utf8("bar"))

  private val twoNode = ClusterTopology.from(
    Vector(
      Shard(a, Vector.empty, Vector(SlotRange(sFoo, sFoo))),
      Shard(b, Vector.empty, Vector(SlotRange(sBar, sBar)))
    )
  )

  test("commands group per node, positions kept in submission order") {
    val plan = twoNode.split(Pipeline.sequence(Seq(keyed("foo"), keyed("bar"), keyed("foo"))))
    assertEquals(plan.perNode, Vector(NodeGroup(a, Vector(0, 2)), NodeGroup(b, Vector(1))))
    assertEquals(plan.keyless, Vector.empty)
    assertEquals(plan.rejected, Vector.empty)
  }

  test("keyless and rejected positions are partitioned out, every index placed once") {
    val plan = twoNode.split(Pipeline.sequence(Seq(keyed("foo"), keyless, keyed("foo", "bar"), keyed("bar"))))
    assertEquals(plan.perNode, Vector(NodeGroup(a, Vector(0)), NodeGroup(b, Vector(3))))
    assertEquals(plan.keyless, Vector(1))
    assertEquals(plan.rejected, Vector(2 -> Rejected.CrossSlot(Set(sFoo, sBar))))
  }

  test("an uncovered command is rejected as unowned, not dropped") {
    val onlyA = ClusterTopology.from(Vector(Shard(a, Vector.empty, Vector(SlotRange(sFoo, sFoo)))))
    val plan  = onlyA.split(Pipeline.sequence(Seq(keyed("foo"), keyed("bar"))))
    assertEquals(plan.perNode, Vector(NodeGroup(a, Vector(0))))
    assertEquals(plan.rejected, Vector(1 -> Rejected.Unowned(sBar)))
  }

  test("a malformed command is rejected in place, not routed") {
    val malformed = Command("BAD", Vector(5), Vector(Bytes.utf8("k")), _ => Right(0L))
    val plan      = twoNode.split(Pipeline.sequence(Seq(keyed("foo"), malformed)))
    assertEquals(plan.perNode, Vector(NodeGroup(a, Vector(0))))
    assertEquals(plan.rejected, Vector(1 -> Rejected.Malformed))
  }

  test("an empty pipeline yields an empty plan") {
    val plan = twoNode.split(Pipeline.sequence(Seq.empty[Command[Long]]))
    assertEquals(plan, SplitPlan(Vector.empty, Vector.empty, Vector.empty))
  }
}

package sage.client.internal

import scala.collection.mutable

import SubscriptionConnection.{Kind, Sink}

import sage.cluster.Node

class PlacementSpec extends munit.FunSuite {

  private val n1 = Node("h1", 1)
  private val n2 = Node("h2", 2)

  // a fake owner that records which names are currently subscribed; attach throws for any name in `failOn`
  final private class FakeConn extends Placement.ShardConn {
    val subscribed          = mutable.LinkedHashSet.empty[String]
    var failOn: Set[String] = Set.empty

    def attach(sink: Sink, names: Vector[String], kind: Kind): Unit    = {
      if (names.exists(failOn)) throw new RuntimeException("attach refused")
      subscribed ++= names
    }
    def detach(sink: Sink, names: Vector[String], kind: Kind): Boolean = {
      subscribed --= names
      subscribed.isEmpty
    }
  }

  final private class FakePool(unavailable: Set[Node] = Set.empty) extends Placement.Conns {
    val byNode                                          = mutable.HashMap.empty[Node, FakeConn]
    def ensure(node: Node): Option[Placement.ShardConn] = if (unavailable(node)) None else Some(byNode.getOrElseUpdate(node, new FakeConn))
    def get(node: Node): Option[Placement.ShardConn]    = byNode.get(node)
  }

  private def sink(names: String*): Sink = new Sink(names.toVector, Kind.Shard, 16)

  private def groups(g: Vector[String]*): Vector[Vector[String]] = g.toVector

  test("place attaches each group on its owner and reports full coverage") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"), Vector("a", "b"))

    placement.place(Map(n1 -> groups(Vector("a"), Vector("b"))), pool)

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a", "b"))
    assert(placement.fullyPlaced, "every channel landed, so the placement is full")
  }

  test("place leaves an unowned channel unplaced — coverage is not full") {
    val pool      = new FakePool(unavailable = Set(n2))
    val placement = new Placement(sink("a", "b"), Vector("a", "b"))

    placement.place(Map(n1 -> groups(Vector("a")), n2 -> groups(Vector("b"))), pool)

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"))
    assert(!placement.fullyPlaced, "b's owner was unavailable, so coverage is incomplete")
  }

  test("place leaves a failed attach unplaced instead of propagating, recording what landed for roll-back") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"), Vector("a", "b"))
    pool.byNode.getOrElseUpdate(n1, new FakeConn).failOn = Set("b")

    placement.place(Map(n1 -> groups(Vector("a"), Vector("b"))), pool)

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"), "a landed; b's attach failed but did not propagate")
    assert(!placement.fullyPlaced, "b is unplaced, so coverage is incomplete and the caller retries")
    // roll-back: reconcile to the empty plan detaches exactly what was placed
    placement.reconcile(Map.empty, pool)
    assert(pool.byNode(n1).subscribed.isEmpty, "the empty plan detaches the landed channel")
  }

  test("reconcile re-homes a channel to its new owner, leaving nothing on the old one") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"), Vector("a", "b"))

    placement.reconcile(Map(n1 -> groups(Vector("a"), Vector("b"))), pool)
    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a", "b"))

    // b migrates to n2
    placement.reconcile(Map(n1 -> groups(Vector("a")), n2 -> groups(Vector("b"))), pool)

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"), "b is detached from its old owner — no stale subscription")
    assertEquals(pool.byNode(n2).subscribed.toSet, Set("b"))
  }

  test("reconcile reports incomplete when an attach fails, then converges once the owner accepts") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"), Vector("a", "b"))
    val owner     = pool.byNode.getOrElseUpdate(n1, new FakeConn)
    owner.failOn = Set("b")

    assert(placement.reconcile(Map(n1 -> groups(Vector("a"), Vector("b"))), pool), "b's attach failed, so the pass is incomplete")
    assertEquals(owner.subscribed.toSet, Set("a"))
    assert(!placement.fullyPlaced)

    owner.failOn = Set.empty
    assert(!placement.reconcile(Map(n1 -> groups(Vector("a"), Vector("b"))), pool), "retry now lands b — complete")
    assertEquals(owner.subscribed.toSet, Set("a", "b"))
    assert(placement.fullyPlaced)
  }

  test("fullyPlaced counts distinct channels, so one double-recorded across owners cannot mask an unplaced channel") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"), Vector("a", "b"))

    placement.reconcile(Map(n2 -> groups(Vector("a"))), pool) // fresh topology records a on n2
    placement.place(Map(n1 -> groups(Vector("a"))), pool) // a stale concurrent place re-records a on n1, never touching b

    assert(!placement.fullyPlaced, "b never landed; the duplicate a on n1 and n2 must not count as full coverage")
  }

  test("a partial attach followed by a topology shift never duplicates a channel across owners") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"), Vector("a", "b"))
    pool.byNode.getOrElseUpdate(n1, new FakeConn).failOn = Set("b")

    // first pass: a lands on n1, b is refused
    assert(placement.reconcile(Map(n1 -> groups(Vector("a"), Vector("b"))), pool))

    // before the retry, b migrates to n2; a stays on n1
    placement.reconcile(Map(n1 -> groups(Vector("a")), n2 -> groups(Vector("b"))), pool)

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"), "a is undisturbed on n1")
    assertEquals(pool.byNode(n2).subscribed.toSet, Set("b"), "b lands on its new owner")
    assert(placement.fullyPlaced)
  }
}

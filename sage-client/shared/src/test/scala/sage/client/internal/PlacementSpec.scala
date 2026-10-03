package sage.client.internal

import scala.collection.mutable

import SubscriptionConnection.{Kind, Sink}

import sage.cluster.Node

class PlacementSpec extends munit.FunSuite {

  private val n1 = Node("h1", 1)
  private val n2 = Node("h2", 2)

  // a fake owner that records which names are currently subscribed; like the server, it rejects each name in `failOn` and keeps the others
  final private class FakeConn extends ClusterSubscriptions.ShardConn {
    val subscribed          = mutable.LinkedHashSet.empty[String]
    var failOn: Set[String] = Set.empty
    var closed              = false

    def attach(sink: Sink, names: Vector[String]): Unit =
      if (closed) throw sage.SageException.NotConnected() else subscribed ++= names.filterNot(failOn)
    def detach(sink: Sink, names: Vector[String]): Unit = subscribed --= names
    def namesOf(sink: Sink): Vector[String]             = subscribed.toVector
  }

  final private class FakePool(unavailable: Set[Node] = Set.empty) {
    val byNode                                                     = mutable.HashMap.empty[Node, FakeConn]
    def ensure(node: Node): Option[ClusterSubscriptions.ShardConn] =
      if (unavailable(node)) None else Some(byNode.getOrElseUpdate(node, new FakeConn))
  }

  // the placement of one sink, as ClusterSubscriptions drives it
  final private class Placement(sink: Sink) {
    def reconcile(plan: ClusterSubscriptions.Plan, pool: FakePool): Boolean =
      ClusterSubscriptions.reconcile(sink, plan, pool.byNode.toVector, pool.ensure)
  }

  private def sink(names: String*): Sink = new Sink(names.toVector, Kind.Shard, 16)

  test("reconcile attaches each channel on its owner and reports full coverage") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"))

    assert(placement.reconcile(Map(n1 -> Vector("a", "b")), pool), "every channel landed, so the placement is full")

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a", "b"))
  }

  test("reconcile leaves a channel unplaced when its owner is unavailable — coverage is not full") {
    val pool      = new FakePool(unavailable = Set(n2))
    val placement = new Placement(sink("a", "b"))

    assert(
      !placement.reconcile(Map(n1 -> Vector("a"), n2 -> Vector("b")), pool),
      "b's owner was unavailable, so coverage is incomplete"
    )

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"))
  }

  test("reconcile counts distinct channels, so one recorded on two owners cannot mask an unplaced channel") {
    val placement = new Placement(sink("a", "b"))

    assert(
      !placement.reconcile(Map(n1 -> Vector("a"), n2 -> Vector("a")), new FakePool),
      "b never landed; a recorded on n1 and n2 must not count as full coverage"
    )
  }

  test("reconcile leaves a failed attach unplaced instead of propagating, recording what landed for roll-back") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"))
    pool.byNode.getOrElseUpdate(n1, new FakeConn).failOn = Set("b")

    assert(!placement.reconcile(Map(n1 -> Vector("a", "b")), pool), "b is unplaced, so coverage is incomplete and the caller retries")

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"), "a landed; b's attach failed but did not propagate")
    // roll-back: reconcile to the empty plan detaches exactly what was placed
    placement.reconcile(Map.empty, pool)
    assert(pool.byNode(n1).subscribed.isEmpty, "the empty plan detaches the landed channel")
  }

  test("reconcile leaves a channel pending when its connection throws on attach, and places the other channels") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"))
    pool.byNode.getOrElseUpdate(n2, new FakeConn).closed = true

    assert(!placement.reconcile(Map(n1 -> Vector("a"), n2 -> Vector("b")), pool), "b's connection threw, so coverage is incomplete")

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"))
    assert(pool.byNode(n2).subscribed.isEmpty)
  }

  test("reconcile re-homes a channel to its new owner, leaving nothing on the old one") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"))

    placement.reconcile(Map(n1 -> Vector("a", "b")), pool)
    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a", "b"))

    // b migrates to n2
    placement.reconcile(Map(n1 -> Vector("a"), n2 -> Vector("b")), pool)

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"), "b is detached from its old owner — no stale subscription")
    assertEquals(pool.byNode(n2).subscribed.toSet, Set("b"))
  }

  test("reconcile reports incomplete when an attach fails, then converges once the owner accepts") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"))
    val owner     = pool.byNode.getOrElseUpdate(n1, new FakeConn)
    owner.failOn = Set("b")

    assert(!placement.reconcile(Map(n1 -> Vector("a", "b")), pool), "b's attach failed, so the pass is incomplete")
    assertEquals(owner.subscribed.toSet, Set("a"))

    owner.failOn = Set.empty
    assert(placement.reconcile(Map(n1 -> Vector("a", "b")), pool), "retry now lands b — complete")
    assertEquals(owner.subscribed.toSet, Set("a", "b"))
  }

  test("a partial attach followed by a topology shift never duplicates a channel across owners") {
    val pool      = new FakePool
    val placement = new Placement(sink("a", "b"))
    pool.byNode.getOrElseUpdate(n1, new FakeConn).failOn = Set("b")

    // first pass: a is assigned to n1, while b is refused
    assert(!placement.reconcile(Map(n1 -> Vector("a", "b")), pool))

    // before the retry, b migrates to n2; a stays on n1
    assert(placement.reconcile(Map(n1 -> Vector("a"), n2 -> Vector("b")), pool))

    assertEquals(pool.byNode(n1).subscribed.toSet, Set("a"), "a is undisturbed on n1")
    assertEquals(pool.byNode(n2).subscribed.toSet, Set("b"), "b lands on its new owner")
  }
}

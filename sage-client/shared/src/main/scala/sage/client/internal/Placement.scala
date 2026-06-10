package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.util.control.NonFatal

import SubscriptionConnection.{Kind, Sink}

import sage.cluster.Node

/**
  * The shard placement ledger for one subscription: it owns the record of which Node carries which of the subscription's Shard Channels and
  * is the single place that keeps that record in step with the wire. A Node owning several slot ranges needs one `SSUBSCRIBE` per Slot (a
  * batched cross-slot subscribe returns `CROSSSLOT`), so a plan groups a Node's channels into the groups that each become one `SSUBSCRIBE`.
  *
  * Two ways to apply a plan, differing only in their failure handling:
  *   - [[place]] is fail-fast for the initial subscribe — an attach failure propagates so the caller can roll back — and records each group
  *     as it lands, so a roll-back ([[reconcile]] to the empty plan) detaches exactly what was already placed.
  *   - [[reconcile]] is best-effort for re-homing — it detaches channels that left the plan, attaches the rest, records only what actually
  *     lands, and reports whether any attach failed so the caller can retry.
  */
final private[internal] class Placement(sink: Sink, requested: Vector[String]) {

  private val lock                             = new ReentrantLock()
  private var placedAt: Map[Node, Set[String]] = Map.empty

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  def place(plan: Placement.Plan, conns: Placement.Conns): Unit =
    plan.foreach { case (node, groups) =>
      conns.ensure(node).foreach { conn =>
        groups.foreach { group =>
          conn.attach(sink, group, Kind.Shard)
          locked { placedAt = placedAt.updatedWith(node)(prev => Some(prev.getOrElse(Set.empty) ++ group)) }
        }
      }
    }

  // true when some attach failed (an unowned Slot is dropped from the plan by the caller, not reported here); the caller retries until it converges
  def reconcile(plan: Placement.Plan, conns: Placement.Conns): Boolean = {
    val desired    = plan.view.mapValues(_.flatten.toSet).toMap
    var incomplete = false
    locked(placedAt).foreach { case (node, had) =>
      val gone = (had -- desired.getOrElse(node, Set.empty)).toVector
      if (gone.nonEmpty) conns.get(node).foreach(_.detach(sink, gone, Kind.Shard))
    }
    val actual     = mutable.HashMap.empty[Node, Set[String]]
    plan.foreach { case (node, groups) =>
      conns.ensure(node) match {
        case None       => incomplete = true
        case Some(conn) =>
          groups.foreach { group =>
            try { conn.attach(sink, group, Kind.Shard); actual.update(node, actual.getOrElse(node, Set.empty) ++ group) }
            catch { case NonFatal(_) => incomplete = true }
          }
      }
    }
    locked { placedAt = actual.toMap }
    incomplete
  }

  // every requested channel is placed on some owner; false means an unowned Slot or unreachable owner is still outstanding
  def fullyPlaced: Boolean = locked(placedAt.valuesIterator.map(_.size).sum) >= requested.distinct.size
}

private[internal] object Placement {

  // a Node's channels grouped so each inner Vector is one SSUBSCRIBE (one Slot), never spanning Slots
  type Plan = Map[Node, Vector[Vector[String]]]

  /**
    * The Sharded Subscription Connections a placement attaches to, looked up under the manager's lock. `ensure` is empty once the manager is closed.
    */
  trait Conns {
    def ensure(node: Node): Option[ShardConn]
    def get(node: Node): Option[ShardConn]
  }

  /**
    * What a placement needs of a connection: register/unregister a sink under names. [[SubscriptionConnection]] is the production adapter.
    */
  trait ShardConn {
    def attach(sink: Sink, names: Vector[String], kind: Kind): Unit
    def detach(sink: Sink, names: Vector[String], kind: Kind): Boolean
  }
}

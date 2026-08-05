package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.util.control.NonFatal

import SubscriptionConnection.{Kind, Sink}

import sage.cluster.Node

/**
  * Tracks which node handles each shard channel for one subscription. A node that owns several slot ranges needs a separate `SSUBSCRIBE`
  * for each slot because a cross-slot subscription returns `CROSSSLOT`. Placement plans therefore group channels by node and slot.
  *
  * Plan updates are atomic with respect to unsubscribe and reassignment:
  *   - [[place]] performs the initial subscription. It records successful groups and leaves failed groups for the caller to retry.
  *   - [[reconcile]] removes obsolete groups, attaches new ones, records successful changes, and reports whether any attachment failed.
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
    locked {
      plan.foreach { case (node, groups) =>
        conns.ensure(node).foreach { conn =>
          groups.foreach { group =>
            // a concurrent eviction can close `conn` before attach. Leave the channel pending for a retry instead of failing reconciliation.
            try {
              conn.attach(sink, group, Kind.Shard)
              placedAt = placedAt.updatedWith(node)(prev => Some(prev.getOrElse(Set.empty) ++ group))
            } catch { case NonFatal(_) => () }
          }
        }
      }
    }

  // Return true when any connection or attachment failed. The caller omits unowned slots from the plan and retries until every requested
  // channel is attached.
  def reconcile(plan: Placement.Plan, conns: Placement.Conns): Boolean =
    locked {
      val desired    = plan.view.mapValues(_.flatten.toSet).toMap
      var incomplete = false
      placedAt.foreach { case (node, had) =>
        val gone = (had -- desired.getOrElse(node, Set.empty)).toVector
        if (gone.nonEmpty) conns.get(node).foreach(_.detach(sink, gone, Kind.Shard))
      }
      val actual     = mutable.HashMap.empty[Node, Set[String]]
      plan.foreach { case (node, groups) =>
        conns.ensure(node) match {
          case None       => incomplete = true
          case Some(conn) =>
            groups.foreach { group =>
              try {
                conn.attach(sink, group, Kind.Shard)
                actual.update(node, actual.getOrElse(node, Set.empty) ++ group)
              } catch { case NonFatal(_) => incomplete = true }
            }
        }
      }
      placedAt = actual.toMap
      incomplete
    }

  // count distinct attached channels across all nodes. Counting each node separately could hide a missing channel when another is recorded twice.
  def fullyPlaced: Boolean = locked(placedAt.valuesIterator.flatten.toSet.size) >= requested.distinct.size
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

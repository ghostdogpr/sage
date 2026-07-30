package sage.client.internal

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.SageException.NotConnected
import sage.client.ReadFrom
import sage.cluster.Node
import sage.commands.Command

/**
  * The read policy's pure half, shared by the cluster and master-replica runtimes: which Nodes may serve a read, and in what order. It
  * decides nothing about connection liveness; the class below walks the order it produces.
  */
private[client] object ReadRouting {

  // an ordinary (non-blocking) read; writes, blocking reads, cursor-bound scans (whose cursor is node-local), and `cached` reads (gated
  // separately) never reach here
  def replicaEligible(command: Command[?]): Boolean = command.isReadOnly && !command.isBlocking && !command.cursorBound

  // the ordered candidates for an eligible read, round-robin-rotated by `rr` across the replicas; an empty result means strict Replica fails
  def candidates(readFrom: ReadFrom, master: Node, replicas: Vector[Node], rr: Int): Vector[Node] = {
    val rotated =
      if (replicas.isEmpty) Vector.empty
      else {
        val k = ((rr % replicas.length) + replicas.length) % replicas.length
        replicas.drop(k) ++ replicas.take(k)
      }
    readFrom match {
      case ReadFrom.Master           => Vector(master)
      case ReadFrom.MasterPreferred  => master +: rotated
      case ReadFrom.Replica          => rotated
      case ReadFrom.ReplicaPreferred => rotated :+ master
    }
  }
}

/**
  * Walks the read policy's ordered candidates, shared by the cluster and master-replica runtimes: it picks each candidate's pool, skips
  * what cannot serve the read, establishes off the caller's thread when it must, and reports a fault back with the candidates left so the
  * runtime can apply its own disposition. Exhausting the order refreshes discovery and fails with [[NotConnected]], since nothing reached
  * the wire and so no fault event will fire.
  *
  * It also owns the per-master round-robin cursors, pruned through [[retain]] as the pools are.
  */
final private[client] class ReadRouting(
  masterPool: NodePool,
  replicaPool: NodePool,
  scheduler: Scheduler,
  readFrom: ReadFrom,
  triggerRefresh: () => Unit
) {

  private val cursors = new ConcurrentHashMap[Node, AtomicInteger]()

  /**
    * The candidates for `master`'s replicas under the policy, advancing that master's cursor once.
    */
  def candidatesFor(master: Node, replicas: Vector[Node]): Vector[Node] =
    candidatesFor(master, replicas, cursors.computeIfAbsent(master, _ => new AtomicInteger()).getAndIncrement())

  /**
    * As [[candidatesFor]], but rotated by a cursor the caller owns, for a read whose rotation belongs to no single master's shard. A
    * replica-preferred read that finds no replica schedules a refresh either way: it falls back to the master silently and would never look
    * again, where strict Replica refreshes by exhausting its candidates instead.
    */
  def candidatesFor(master: Node, replicas: Vector[Node], rr: Int): Vector[Node] = {
    if (readFrom == ReadFrom.ReplicaPreferred && replicas.isEmpty) triggerRefresh()
    ReadRouting.candidates(readFrom, master, replicas, rr)
  }

  /**
    * Drops the cursors of masters the topology no longer lists, alongside the pools' own `retain`.
    */
  def retain(keep: Node => Boolean): Unit = cursors.keySet.removeIf(node => !keep(node)): Unit

  /**
    * Submits `command` to the first candidate that can serve it, attributing the Node on success. `onFault` receives the failing Node, its
    * error, and the candidates left, and runs off the reply thread. An empty `candidates` completes on the caller's thread; anything else
    * completes on whichever thread answers the submit.
    */
  def walk[A](command: Command[A], candidates: Vector[Node], master: Node, complete: Try[A] => Unit)(
    onFault: (Node, Throwable, Vector[Node]) => Unit
  ): Unit =
    candidates match {
      case node +: rest =>
        val pool     = poolFor(node, master)
        val existing = pool.existing(node)
        if (existing != null)
          if (existing.isLive) submit(existing, node, command, rest, complete)(onFault)
          else walk(command, rest, master, complete)(onFault)
        else
          offload {
            val nc = establishOrNull(pool, node)
            if (nc == null || !nc.isLive) walk(command, rest, master, complete)(onFault)
            else submit(nc, node, command, rest, complete)(onFault)
          }
      case _            =>
        triggerRefresh()
        complete(Failure(NotConnected()))
    }

  /**
    * The one Node a batch of reads should land on with its connection, established if need be, or `None` when no candidate can serve the
    * read. `onPick` runs on the caller's thread while a live candidate is already established, and off it otherwise.
    */
  def pickOne(candidates: Vector[Node], master: Node)(onPick: Option[(Node, NodeClient)] => Unit): Unit = {
    val it = candidates.iterator
    while (it.hasNext) {
      val node = it.next()
      val nc   = poolFor(node, master).existing(node)
      if (nc == null) {
        offload(onPick(establishOne(candidates, master)))
        return
      }
      if (nc.isLive) {
        onPick(Some((node, nc)))
        return
      }
    }
    onPick(None)
  }

  private def submit[A](nc: NodeClient, node: Node, command: Command[A], rest: Vector[Node], complete: Try[A] => Unit)(
    onFault: (Node, Throwable, Vector[Node]) => Unit
  ): Unit =
    nc.submit[A](
      command,
      asking = false,
      {
        case success @ Success(_) =>
          Events.attributeNode(complete, node)
          complete(success)
        case Failure(error)       => offload(onFault(node, error, rest))
      }
    )

  private def establishOne(candidates: Vector[Node], master: Node): Option[(Node, NodeClient)] = {
    val it = candidates.iterator
    while (it.hasNext) {
      val node = it.next()
      val nc   = establishOrNull(poolFor(node, master), node)
      if (nc != null && nc.isLive) return Some((node, nc))
    }
    None
  }

  private def establishOrNull(pool: NodePool, node: Node): NodeClient =
    try pool.getOrEstablish(node)
    catch { case NonFatal(_) => null }

  private def poolFor(node: Node, master: Node): NodePool = if (node == master) masterPool else replicaPool

  private def offload(body: => Unit): Unit = scheduler.after(Duration.Zero)(body)
}

package sage.client.internal

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.util.{Failure, Success, Try}

import sage.SageException.NotConnected
import sage.client.ReadFrom
import sage.cluster.Node
import sage.commands.Command

/**
  * Selects the nodes that may serve a read, in order, for cluster and master-replica clients. [[candidates]] does not check connection
  * liveness. The [[ReadRouting]] class checks each selected node when it handles a read.
  */
private[client] object ReadRouting {

  final case class Picked(node: Node, client: NodeClient, remaining: Vector[Node])

  // Allow ordinary non-blocking reads. Exclude writes, blocking reads, and cursor-bound scans because their cursors are node-local. Cached
  // reads apply their own routing rule before calling this method.
  def replicaEligible(command: Command[?]): Boolean = command.isReadOnly && !command.isBlocking && !command.cursorBound

  // Order replicas in round-robin order using `rr`, then add the master when the policy permits it. An empty result means ReadFrom.Replica has
  // no available candidate.
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
  * Tries the read policy's candidates in order for cluster and master-replica clients. It skips candidates that cannot serve the read and
  * opens connections outside the caller's thread. After a failed attempt, the runtime receives the fault and the remaining candidates and
  * decides whether to continue. If every candidate is unavailable, this code refreshes discovery and fails with [[NotConnected]]. It must
  * refresh here because no command was sent and no fault event will be emitted.
  *
  * It keeps a separate round-robin position for each master and removes positions for masters that [[retain]] removes from the pools.
  */
final private[client] class ReadRouting(
  masterPool: NodePool,
  replicaPool: NodePool,
  scheduler: Scheduler,
  readFrom: ReadFrom,
  triggerRefresh: () => Unit
) {

  private val cursors = new ConcurrentHashMap[Node, AtomicInteger]()

  private enum CandidateState {
    case Unknown
    case Unavailable
    case Connected(client: NodeClient)
  }

  private enum Selection {
    case Found(picked: ReadRouting.Picked)
    case NeedsEstablish
    case Exhausted
  }

  /**
    * The candidates for `master`'s replicas under the policy, advancing that master's cursor once.
    */
  def candidatesFor(master: Node, replicas: Vector[Node]): Vector[Node] =
    candidatesFor(master, replicas, cursors.computeIfAbsent(master, _ => new AtomicInteger()).getAndIncrement())

  /**
    * Selects candidates using a caller-provided round-robin position. This supports keyless reads that are not associated with one master's
    * shard. When ReadFrom.ReplicaPreferred has no known replica, schedule a refresh even though the current read can use the master.
    * ReadFrom.Replica schedules the refresh after it exhausts its candidate list.
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
    * Submits `command` to the first candidate that can serve it and records that node on success. Connection establishment and `onFault` are
    * offloaded. A successful command completes on the reply thread. If the candidates are exhausted before a command is sent, completion
    * runs on the thread currently checking them.
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
          scheduler.offload {
            val nc = pool.getOrEstablishOrNull(node)
            if (nc == null || !nc.isLive) walk(command, rest, master, complete)(onFault)
            else submit(nc, node, command, rest, complete)(onFault)
          }
      case _            =>
        triggerRefresh()
        complete(Failure(NotConnected()))
    }

  /**
    * Selects one node and connection for a batch of reads, together with the remaining candidates. Returns `None` when no candidate can
    * serve the read. `onPick` runs on the caller's thread when a candidate is already connected or none is available. Connection
    * establishment is offloaded.
    */
  def pickOne(candidates: Vector[Node], master: Node)(onPick: Option[ReadRouting.Picked] => Unit): Unit =
    select(candidates, master, existingCandidate) match {
      case Selection.Found(picked)  => onPick(Some(picked))
      case Selection.Exhausted      => onPick(None)
      case Selection.NeedsEstablish =>
        scheduler.offload {
          // retry the full order, allowing a previously disconnected candidate to reconnect before checking the first unknown one
          select(candidates, master, establishCandidate) match {
            case Selection.Found(picked) => onPick(Some(picked))
            case _                       => onPick(None)
          }
        }
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
        case Failure(error)       => scheduler.offload(onFault(node, error, rest))
      }
    )

  private def select(candidates: Vector[Node], master: Node, lookup: (NodePool, Node) => CandidateState): Selection = {
    var remaining = candidates
    while (remaining.nonEmpty) {
      val node = remaining.head
      val pool = poolFor(node, master)
      lookup(pool, node) match {
        case CandidateState.Unknown       => return Selection.NeedsEstablish
        case CandidateState.Unavailable   => ()
        case CandidateState.Connected(nc) =>
          if (nc.isLive) return Selection.Found(ReadRouting.Picked(node, nc, remaining.tail))
      }
      remaining = remaining.tail
    }
    Selection.Exhausted
  }

  private def existingCandidate(pool: NodePool, node: Node): CandidateState = {
    val nc = pool.existing(node)
    if (nc == null) CandidateState.Unknown else CandidateState.Connected(nc)
  }

  private def establishCandidate(pool: NodePool, node: Node): CandidateState = {
    val nc = pool.getOrEstablishOrNull(node)
    if (nc == null) CandidateState.Unavailable else CandidateState.Connected(nc)
  }

  private def poolFor(node: Node, master: Node): NodePool = if (node == master) masterPool else replicaPool
}

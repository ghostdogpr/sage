package sage.client.internal

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.SageException.NotConnected
import sage.client.ReadFrom
import sage.cluster.{Node, Redirect}
import sage.commands.Command

/**
  * Executes the shared read policy after a topology has identified a master and its replicas. The topology owns its connection pools and
  * redirect/refresh behavior; this module owns eligibility, candidate rotation, liveness filtering, safe fall-through, and terminal Node
  * attribution so those semantics cannot drift between cluster and master-replica clients.
  */
final private[client] class ReadPolicy(
  readFrom: ReadFrom,
  nodes: ReadPolicy.NodeAccess,
  topology: ReadPolicy.TopologyPolicy,
  scheduler: Scheduler
) {

  import ReadPolicy.*

  private val cursors = new ConcurrentHashMap[Lane, AtomicInteger]

  def submit[A](source: Source, command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit = {
    val cursor     = cursors.computeIfAbsent(source.lane, _ => new AtomicInteger)
    val candidates = ReadPolicy.candidates(readFrom, source, cursor.getAndIncrement())
    attempt(candidates, Request(source.master, command, redirectsLeft, complete), Exhaustion(None, redispatch = false), establish = false)
  }

  def submitBatch(
    source: Source,
    commands: Vector[Command[?]],
    callbacks: Vector[Try[Any] => Unit],
    redirectsLeft: Int,
    onDeferredExhausted: () => Unit = () => ()
  ): Boolean = {
    val cursor     = cursors.computeIfAbsent(source.lane, _ => new AtomicInteger)
    val candidates = ReadPolicy.candidates(readFrom, source, cursor.getAndIncrement())
    val request    = BatchRequest(source.master, commands, callbacks, redirectsLeft, onDeferredExhausted)
    attemptBatch(candidates, request, Exhaustion(None, redispatch = false), establish = false) != BatchSubmission.Rejected
  }

  def retainSources(masters: Set[Node]): Unit = {
    val _ = cursors.keySet.removeIf {
      case Lane.Master(node) => !masters.contains(node)
      case Lane.Keyless      => false
    }
  }

  private def attempt[A](
    candidates: Vector[Node],
    request: Request[A],
    exhaustion: Exhaustion,
    establish: Boolean
  ): Unit =
    candidates.headOption match {
      case None       => topology.exhausted(exhaustion, request.command, request.redirectsLeft, request.complete)
      case Some(node) =>
        val role       = if (node == request.master) Role.Master else Role.Replica
        val connection =
          try Option(if (establish) nodes.establish(node, role) else nodes.existing(node, role))
          catch {
            case NonFatal(error) =>
              attempt(candidates.tail, request, exhaustion.withLast(node, error), establish)
              return
          }

        connection match {
          case Some(conn) if conn.isLive =>
            try conn.submit(request.command, result => onResult(node, candidates.tail, request, result))
            catch {
              case NonFatal(error) =>
                scheduler.after(Duration.Zero)(onFailure(node, candidates.tail, request, error))
            }
          case Some(_)                   =>
            attempt(candidates.tail, request, exhaustion.withLast(node, NotConnected()), establish)
          case None if establish         =>
            attempt(candidates.tail, request, exhaustion.withLast(node, NotConnected()), establish = true)
          case None                      =>
            scheduler.after(Duration.Zero)(attempt(candidates, request, exhaustion, establish = true))
        }
    }

  private def onResult[A](
    node: Node,
    remaining: Vector[Node],
    request: Request[A],
    result: Try[A]
  ): Unit =
    result match {
      case success: Success[A] => finishAt(node, request.complete, success)
      case Failure(error)      =>
        scheduler.after(Duration.Zero)(onFailure(node, remaining, request, error))
    }

  private def attemptBatch(
    candidates: Vector[Node],
    request: BatchRequest,
    exhaustion: Exhaustion,
    establish: Boolean
  ): BatchSubmission =
    candidates.headOption match {
      case None       => topology.batchExhausted(exhaustion); BatchSubmission.Rejected
      case Some(node) =>
        val role                           = if (node == request.master) Role.Master else Role.Replica
        val connection: Option[Connection] =
          try Option(if (establish) nodes.establish(node, role) else nodes.existing(node, role))
          catch {
            case NonFatal(error) =>
              return attemptBatch(candidates.tail, request, exhaustion.withLast(node, error), establish)
          }

        connection match {
          case Some(conn) if conn.isLive =>
            val routed = Vector.tabulate(request.commands.length) { index =>
              val command = request.commands(index).asInstanceOf[Command[Any]]
              val single  = Request(request.master, command, request.redirectsLeft, request.callbacks(index))
              (result: Try[Any]) => onResult(node, candidates.tail, single, result)
            }
            try
              if (conn.submitAll(request.commands, routed)) BatchSubmission.Submitted
              else attemptBatch(candidates.tail, request, exhaustion.withLast(node, NotConnected()), establish)
            catch {
              case NonFatal(error) =>
                Fault.categorize(error) match {
                  case Fault.Lost(false) =>
                    attemptBatch(
                      candidates.tail,
                      request,
                      Exhaustion(Some(node -> error), redispatch = true),
                      establish
                    )
                  case _                 => routed.foreach(_(Failure(error))); BatchSubmission.Submitted
                }
            }
          case Some(_)                   =>
            attemptBatch(candidates.tail, request, exhaustion.withLast(node, NotConnected()), establish)
          case None if establish         =>
            attemptBatch(candidates.tail, request, exhaustion.withLast(node, NotConnected()), establish = true)
          case None                      =>
            scheduler.after(Duration.Zero) {
              if (attemptBatch(candidates, request, exhaustion, establish = true) == BatchSubmission.Rejected)
                request.onDeferredExhausted()
            }
            BatchSubmission.Deferred
        }
    }

  private def onFailure[A](
    node: Node,
    remaining: Vector[Node],
    request: Request[A],
    error: Throwable
  ): Unit =
    Fault.categorize(error) match {
      case Fault.Redirected(redirect) =>
        topology.redirected(
          RedirectAttempt(
            node,
            redirect,
            error,
            request.command,
            request.redirectsLeft,
            request.complete,
            failure => finishAt(node, request.complete, Failure(failure))
          )
        )
      case Fault.Lost(false)          =>
        topology.onSafeLoss(node, error)
        attempt(remaining, request, Exhaustion(Some(node -> error), redispatch = true), establish = true)
      case Fault.Demoted              => terminal(node, error, request.complete)
      case Fault.Lost(true)           => terminal(node, error, request.complete)
      case Fault.Fatal                => finishAt(node, request.complete, Failure(error))
    }

  private def terminal[A](node: Node, error: Throwable, complete: Try[A] => Unit): Unit = {
    topology.terminal(node, error)
    finishAt(node, complete, Failure(error))
  }

  private def finishAt[A](node: Node, complete: Try[A] => Unit, result: Try[A]): Unit = {
    Events.attributeNode(complete.asInstanceOf[AnyRef], node)
    complete(result)
  }

  final private case class Request[A](master: Node, command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit)
  final private case class BatchRequest(
    master: Node,
    commands: Vector[Command[?]],
    callbacks: Vector[Try[Any] => Unit],
    redirectsLeft: Int,
    onDeferredExhausted: () => Unit
  )

  private enum BatchSubmission {
    case Submitted, Rejected, Deferred
  }
}

private[client] object ReadPolicy {

  enum Role {
    case Master, Replica
  }

  final class Source private (val master: Node, val replicas: Vector[Node], private[ReadPolicy] val lane: Lane)

  object Source {
    def forMaster(master: Node, replicas: Vector[Node]): Source = new Source(master, replicas, Lane.Master(master))
    def keyless(master: Node, replicas: Vector[Node]): Source   = new Source(master, replicas, Lane.Keyless)
  }

  private[ReadPolicy] enum Lane {
    case Master(node: Node)
    case Keyless
  }

  final case class Exhaustion(last: Option[(Node, Throwable)], redispatch: Boolean) {
    private[ReadPolicy] def withLast(node: Node, error: Throwable): Exhaustion = copy(last = Some(node -> error))
  }

  final case class RedirectAttempt[A](
    from: Node,
    redirect: Redirect,
    error: Throwable,
    command: Command[A],
    redirectsLeft: Int,
    complete: Try[A] => Unit,
    failAtSource: Throwable => Unit
  )

  trait Connection {
    def isLive: Boolean
    def submit[A](command: Command[A], callback: Try[A] => Unit): Unit
    def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Boolean
  }

  trait NodeAccess {
    def existing(node: Node, role: Role): Connection
    def establish(node: Node, role: Role): Connection
  }

  object NodeAccess {
    def pooled(master: NodePool, replica: NodePool): NodeAccess = new NodeAccess {
      def existing(node: Node, role: Role): Connection  = wrap(pool(role).existing(node))
      def establish(node: Node, role: Role): Connection = wrap(pool(role).getOrEstablish(node))

      private def pool(role: Role): NodePool = role match {
        case Role.Master  => master
        case Role.Replica => replica
      }

      private def wrap(client: NodeClient): Connection =
        if (client == null) null
        else
          new Connection {
            def isLive: Boolean                                                                       = client.isLive
            def submit[A](command: Command[A], callback: Try[A] => Unit): Unit                        = client.submit(command, asking = false, callback)
            def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Boolean = client.submitAll(commands, callbacks)
          }
    }
  }

  trait TopologyPolicy {
    def redirected[A](attempt: RedirectAttempt[A]): Unit = attempt.failAtSource(attempt.error)
    def onSafeLoss(node: Node, error: Throwable): Unit   = ()
    def exhausted[A](exhaustion: Exhaustion, command: Command[A], redirectsLeft: Int, complete: Try[A] => Unit): Unit
    def terminal(node: Node, error: Throwable): Unit
    def batchExhausted(exhaustion: Exhaustion): Unit
  }

  def replicaEligible(command: Command[?]): Boolean = command.isReadOnly && !command.isBlocking && !command.cursorBound

  def replicaEligible(commands: Vector[Command[?]]): Boolean = commands.forall(replicaEligible)

  private def candidates(readFrom: ReadFrom, source: Source, rr: Int): Vector[Node] = {
    val replicas = source.replicas
    val rotated  =
      if (replicas.isEmpty) Vector.empty
      else {
        val offset = Math.floorMod(rr, replicas.length)
        replicas.drop(offset) ++ replicas.take(offset)
      }

    readFrom match {
      case ReadFrom.Master           => Vector(source.master)
      case ReadFrom.MasterPreferred  => source.master +: rotated
      case ReadFrom.Replica          => rotated
      case ReadFrom.ReplicaPreferred => rotated :+ source.master
    }
  }
}

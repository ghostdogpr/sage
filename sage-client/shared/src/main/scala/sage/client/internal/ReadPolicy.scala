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
    attempt(candidates, Request(source.master, command, redirectsLeft, complete), Exhaustion.Unsubmitted(None), establish = false)
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
    attemptBatch(candidates, request, Exhaustion.Unsubmitted(None), establish = false) != BatchSubmission.Rejected
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
    resolve(candidates, request.master, exhaustion, establish) match {
      case Candidate.Exhausted(state)                   =>
        topology.exhausted(
          ExhaustedAttempt(
            state,
            request.command,
            request.redirectsLeft,
            request.complete,
            error => failFromExhaustion(state, request.complete, error)
          )
        )
      case Candidate.Establish(pending, state)          =>
        scheduler.after(Duration.Zero)(attempt(pending, request, state, establish = true))
      case Candidate.Live(node, conn, remaining, state) =>
        try conn.submit(request.command, result => onResult(node, remaining, request, result))
        catch {
          case NonFatal(error) =>
            scheduler.after(Duration.Zero)(onFailure(node, remaining, request, error))
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
    resolve(candidates, request.master, exhaustion, establish) match {
      case Candidate.Exhausted(state)                   => topology.batchExhausted(state); BatchSubmission.Rejected
      case Candidate.Establish(pending, state)          =>
        scheduler.after(Duration.Zero) {
          if (attemptBatch(pending, request, state, establish = true) == BatchSubmission.Rejected)
            request.onDeferredExhausted()
        }
        BatchSubmission.Deferred
      case Candidate.Live(node, conn, remaining, state) =>
        val routed = Vector.tabulate(request.commands.length) { index =>
          val command = request.commands(index).asInstanceOf[Command[Any]]
          val single  = Request(request.master, command, request.redirectsLeft, request.callbacks(index))
          (result: Try[Any]) => onResult(node, remaining, single, result)
        }
        try
          if (conn.submitAll(request.commands, routed)) BatchSubmission.Submitted
          else attemptBatch(remaining, request, state.withLast(node, NotConnected()), establish)
        catch {
          case NonFatal(error) =>
            Fault.categorize(error) match {
              case Fault.Lost(false) =>
                attemptBatch(remaining, request, Exhaustion.AfterSafeLoss(node, error), establish)
              case _                 => routed.foreach(_(Failure(error))); BatchSubmission.Submitted
            }
        }
    }

  private def resolve(
    candidates: Vector[Node],
    master: Node,
    exhaustion: Exhaustion,
    establish: Boolean
  ): Candidate =
    candidates.headOption match {
      case None       => Candidate.Exhausted(exhaustion)
      case Some(node) =>
        val role = if (node == master) Role.Master else Role.Replica
        Try(Option(if (establish) nodes.establish(node, role) else nodes.existing(node, role))) match {
          case Failure(error)                     => resolve(candidates.tail, master, exhaustion.withLast(node, error), establish)
          case Success(Some(conn)) if conn.isLive => Candidate.Live(node, conn, candidates.tail, exhaustion)
          case Success(Some(_))                   => resolve(candidates.tail, master, exhaustion.withLast(node, NotConnected()), establish)
          case Success(None) if establish         =>
            resolve(candidates.tail, master, exhaustion.withLast(node, NotConnected()), establish = true)
          case Success(None)                      => Candidate.Establish(candidates, exhaustion)
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
        attempt(remaining, request, Exhaustion.AfterSafeLoss(node, error), establish = true)
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

  private def failFromExhaustion[A](exhaustion: Exhaustion, complete: Try[A] => Unit, error: Throwable): Unit =
    exhaustion match {
      case Exhaustion.Unsubmitted(_)         => complete(Failure(error))
      case Exhaustion.AfterSafeLoss(node, _) => finishAt(node, complete, Failure(error))
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

  private enum Candidate {
    case Exhausted(exhaustion: Exhaustion)
    case Establish(candidates: Vector[Node], exhaustion: Exhaustion)
    case Live(node: Node, connection: Connection, remaining: Vector[Node], exhaustion: Exhaustion)
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

  enum Exhaustion {
    case Unsubmitted(lastFailure: Option[(Node, Throwable)])
    case AfterSafeLoss(node: Node, error: Throwable)

    private[ReadPolicy] def withLast(node: Node, error: Throwable): Exhaustion = this match {
      case Unsubmitted(_)             => Unsubmitted(Some(node -> error))
      case safe @ AfterSafeLoss(_, _) => safe
    }
  }

  final case class RedirectAttempt[A](
    from: Node,
    redirect: Redirect,
    error: Throwable,
    command: Command[A],
    redirectsLeft: Int,
    resume: Try[A] => Unit,
    failAtSource: Throwable => Unit
  )

  final case class ExhaustedAttempt[A](
    exhaustion: Exhaustion,
    command: Command[A],
    redirectsLeft: Int,
    resume: Try[A] => Unit,
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
    def exhausted[A](attempt: ExhaustedAttempt[A]): Unit
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

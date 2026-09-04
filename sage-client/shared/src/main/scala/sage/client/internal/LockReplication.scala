package sage.client.internal

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

import sage.SageException.{ConnectionLost, LockLost, TimedOut}
import sage.commands.{Command, Connection, Reply, Role, Server}

/**
  * Confirms a lock write on the connection that executed it. Each instance handles one write and its replication check.
  */
final private[client] class LockReplication(
  scheduler: Scheduler,
  replicas: Int,
  deadlineMillis: Long,
  onConfirmationFailure: () => Unit,
  replicaAcknowledgement: Boolean
) {
  private val confirming = new AtomicBoolean(false)

  def cancelled(): Unit = if (confirming.get()) onConfirmationFailure()

  def submit[A](conn: DedicatedConnection, command: Command[A], asking: Boolean, complete: Try[A] => Unit): Unit = {
    // Report that the write may have executed. Lock acquisition retries are safe because they reuse the same ownership token.
    def confirmationFailed(error: Throwable): Unit = {
      onConfirmationFailure()
      val failure = Fault.categorize(error) match {
        case Fault.Lost(_) | Fault.Redirected(_) | Fault.TryAgain | Fault.Unavailable(_) =>
          val lost = ConnectionLost(mayHaveExecuted = true)
          lost.initCause(error)
          lost
        case _                                                                           => error
      }
      complete(Failure(failure))
    }

    def confirm(value: A): Unit = {
      confirming.set(true)
      conn.submit(
        Server.role,
        {
          case Success(Role.Master(_, connected)) =>
            if (replicaAcknowledgement) waitForReplicas(conn, value, connected.size, complete, confirmationFailed)
            else complete(Success(value))
          case Success(_)                         => confirmationFailed(LockLost("the granting node is no longer a master"))
          case Failure(error)                     => confirmationFailed(error)
        }
      )
    }

    val onReply: Try[A] => Unit = {
      case Success(value) if value == true => confirm(value)
      case result                          => complete(result)
    }
    if (asking)
      conn.submitRaw(Vector(Connection.asking, command.rawFrame), result => onReply(result.flatMap(frames => Reply.decode(command, frames.last))))
    else conn.submit(command, onReply)
  }

  private def waitForReplicas[A](
    conn: DedicatedConnection,
    value: A,
    connected: Int,
    complete: Try[A] => Unit,
    confirmationFailed: Throwable => Unit
  ): Unit = {
    val required = math.max(replicas, connected)
    if (required == 0) complete(Success(value))
    else {
      // Reserve half the remaining budget for the server's timeout processing and the reply's transit.
      val waitMillis = (deadlineMillis - scheduler.nowMillis) / 2L
      if (waitMillis <= 0L) confirmationFailed(TimedOut("distributed lock replication deadline reached before WAIT"))
      else
        conn.submit(
          Server.waitReplicas(required.toLong, waitMillis.millis),
          {
            case Success(count) if count >= required => complete(Success(value))
            case Success(count)                      => confirmationFailed(TimedOut(s"replication confirmed by $count of $required required replicas"))
            case Failure(error)                      => confirmationFailed(error)
          }
        )
    }
  }
}

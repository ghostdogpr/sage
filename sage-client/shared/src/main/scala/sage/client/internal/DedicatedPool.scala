package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.Try
import scala.util.control.NonFatal

import sage.SageException.{ConnectionLost, NotConnected, TimedOut}
import sage.client.DedicatedPoolConfig
import sage.commands.Command

/**
  * The on-demand pool of Dedicated Connections that carry blocking commands. A connection is created lazily, used by exactly one blocking
  * command at a time, and returned to an idle set on release — or discarded outright on any loss. The pool never reconnects a connection
  * (that lives in the Multiplexed Connection); it simply establishes a fresh one when none is idle.
  *
  * Acquisition is gated on the client being live (`isLive`): a blocking command issued while disconnected fails fast `NotConnected`, the
  * same contract ordinary commands get (ADR-0006). When all slots are in use the caller waits up to `acquireTimeout` for one to free, then
  * fails `TimedOut`. `close` force-closes every connection at once — an in-flight blocking command then fails `ConnectionLost(true)` rather
  * than stalling shutdown for a reply that may never come.
  */
final private[client] class DedicatedPool(
  factory: MultiplexedConnection.TransportFactory,
  bootstrap: Vector[Command[?]],
  scheduler: Scheduler,
  isLive: () => Boolean,
  generation: () => Long,
  config: DedicatedPoolConfig,
  connectTimeoutMillis: Long
) {

  private val lock      = new ReentrantLock()
  private val available = lock.newCondition()

  private val idle                              = mutable.ArrayDeque.empty[DedicatedPool.Idle]
  private val live                              = mutable.Set.empty[DedicatedConnection]
  private var reserved                          = 0
  private var closing                           = false
  private val sweepHandle: Scheduler.Cancelable =
    config.idleTimeout match {
      case interval: FiniteDuration => scheduler.every(interval)(sweepExpired())
      case _                        => null
    }

  /**
    * Runs a blocking command on a borrowed Dedicated Connection, releasing it when the reply (or a failure) arrives. Returns immediately;
    * the acquire — which may wait for a slot or open a socket — is offloaded so the calling fiber is never blocked.
    */
  def use[A](command: Command[A], callback: Try[A] => Unit): Unit =
    if (!isLive()) callback(scala.util.Failure(NotConnected()))
    else
      scheduler.after(Duration.Zero) {
        val acquired =
          try Right(acquire())
          catch {
            case e: NotConnected => Left(e)
            case e: TimedOut     => Left(e)
            case NonFatal(_)     => Left(ConnectionLost(mayHaveExecuted = false)) // never reached the wire
          }
        acquired match {
          case Left(error) => callback(scala.util.Failure(error))
          case Right(conn) =>
            conn.submit(
              command,
              result => {
                release(conn)
                callback(result)
              }
            )
        }
      }

  /**
    * Borrows a connection to lease across a transaction's many commands (rather than the single command [[use]] runs). Blocks the calling
    * fiber while it waits for a slot or opens a socket, so callers must offload it; gated on liveness like [[use]].
    */
  def acquireForTransaction(): DedicatedConnection = {
    if (!isLive()) throw NotConnected()
    acquire()
  }

  /**
    * Returns a leased connection. `reusable` recycles it to the idle set; otherwise (a transaction left with watches armed, or interrupted
    * mid-command) it is discarded outright rather than handed to the next borrower with residual `WATCH`/`MULTI` state.
    */
  def releaseTransaction(connection: DedicatedConnection, reusable: Boolean): Unit =
    if (reusable) release(connection)
    else
      locked {
        live -= connection
        scheduleClose(connection)
        available.signalAll()
      }

  def close(): Unit = {
    val toClose = locked {
      closing = true
      if (sweepHandle != null) sweepHandle.cancel()
      available.signalAll()
      val snapshot = live.toVector
      live.clear()
      idle.clear()
      snapshot
    }
    toClose.foreach(_.close())
  }

  private def acquire(): DedicatedConnection = {
    val deadlineNanos = System.nanoTime() + config.acquireTimeout.toNanos
    locked {
      while (true) {
        if (closing) throw NotConnected()
        val reused = takeIdleLocked()
        if (reused != null) return reused
        if (live.size + reserved < config.maxConnections) {
          reserved += 1
          val established = establishOutsideLock()
          if (established != null) return established
          // discarded as stale; retry, but bounded by the deadline so generation churn can't spin forever
          if (System.nanoTime() >= deadlineNanos) throw acquireTimedOut
        } else {
          val remaining = deadlineNanos - System.nanoTime()
          if (remaining <= 0L) throw acquireTimedOut
          val _         = available.awaitNanos(remaining)
        }
      }
      throw new IllegalStateException("unreachable")
    }
  }

  // entered holding the lock with `reserved` already incremented; drops the lock for the blocking establish, then re-accounts under it.
  // Returns null when the connection must be discarded and the caller should retry.
  private def establishOutsideLock(): DedicatedConnection = {
    lock.unlock()
    val connection =
      try DedicatedConnection.establish(factory, bootstrap, connectTimeoutMillis, generation())
      catch {
        case e: Throwable =>
          locked { reserved -= 1; available.signalAll() }
          lock.lock() // re-take so acquire()'s `locked` block unlocks exactly once on exit
          throw e
      }
    lock.lock()
    reserved -= 1
    // a close or reconnect during establish makes this freshly built socket stale before first use — never hand it out
    if (closing || !isLive()) {
      available.signalAll()
      scheduleClose(connection)
      throw NotConnected()
    }
    if (connection.generation != generation()) {
      available.signalAll()
      scheduleClose(connection)
      null
    } else {
      live += connection
      connection
    }
  }

  private def acquireTimedOut: TimedOut =
    TimedOut(s"dedicated pool acquire timed out after ${config.acquireTimeout.toMillis}ms")

  private def takeIdleLocked(): DedicatedConnection = {
    val gen                         = generation()
    var result: DedicatedConnection = null
    while (result == null && idle.nonEmpty) {
      val candidate = idle.removeLast()
      if (reusable(candidate, gen)) result = candidate.connection
      else {
        live -= candidate.connection
        scheduleClose(candidate.connection)
      }
    }
    result
  }

  private def release(connection: DedicatedConnection): Unit =
    locked {
      if (closing || !connection.isHealthy || connection.generation != generation()) {
        live -= connection
        scheduleClose(connection)
      } else idle.append(DedicatedPool.Idle(connection, scheduler.nowMillis))
      available.signalAll()
    }

  private def sweepExpired(): Unit = {
    val toClose = locked {
      val gen     = generation()
      val due     = Vector.newBuilder[DedicatedConnection]
      idle.filterInPlace { entry =>
        val keep = reusable(entry, gen)
        if (!keep) { val _ = due += entry.connection }
        keep
      }
      val expired = due.result()
      expired.foreach(live -= _)
      expired
    }
    toClose.foreach(scheduleClose) // never close on the timer thread: close() joins I/O threads
  }

  // a connection from an older generation outlived a reconnect (e.g. DNS failover) and now points at the old server
  private def reusable(entry: DedicatedPool.Idle, gen: Long): Boolean =
    entry.connection.isHealthy && entry.connection.generation == gen && !expired(entry)

  private def expired(entry: DedicatedPool.Idle): Boolean =
    config.idleTimeout.isFinite && scheduler.nowMillis - entry.idleSinceMillis >= config.idleTimeout.toMillis

  // closing a connection joins its I/O threads, so it must never run under the lock or on the scheduler's timer thread
  private def scheduleClose(connection: DedicatedConnection): Unit =
    scheduler.after(Duration.Zero)(connection.close())

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }
}

private[client] object DedicatedPool {

  final case class Idle(connection: DedicatedConnection, idleSinceMillis: Long)
}

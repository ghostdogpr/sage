package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.Try
import scala.util.control.NonFatal

import sage.SageException.{ConnectionLost, NotConnected, TimedOut}
import sage.client.DedicatedPoolConfig
import sage.commands.{Command, Connection}

/**
  * The on-demand pool of Dedicated Connections that carry blocking commands. A connection is created lazily, used by exactly one blocking
  * command at a time, and returned to an idle set on release — or discarded outright on any loss. The pool never reconnects a connection
  * (that lives in the Multiplexed Connection); it simply establishes a fresh one when none is idle.
  *
  * Acquisition is gated on the client being live (`isLive`): a blocking command issued while disconnected fails fast `NotConnected`, the
  * same contract ordinary commands get. When all slots are in use the caller waits up to `acquireTimeout` for one to free, then
  * fails `TimedOut`. `close` force-closes every connection at once — an in-flight blocking command then fails `ConnectionLost(true)` rather
  * than stalling shutdown for a reply that may never come.
  */
final private[client] class DedicatedPool(
  factory: MultiplexedConnection.TransportFactory,
  bootstrap: Vector[Command[?]],
  scheduler: Scheduler,
  isLive: () => Boolean,
  liveGeneration: () => Option[MultiplexedConnection.Generation],
  isCurrent: MultiplexedConnection.Generation => Boolean,
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
  def use[A](command: Command[A], callback: Try[A] => Unit): Unit = lease(command, asking = false, callback)

  /**
    * Runs a blocking command redirected by `ASK`: `ASKING` then the command, back-to-back on one exclusively-leased connection, so their
    * wire adjacency is automatic. The `ASKING` reply is discarded; the command's reply releases the connection.
    */
  def useAsking[A](command: Command[A], callback: Try[A] => Unit): Unit = lease(command, asking = true, callback)

  private def lease[A](command: Command[A], asking: Boolean, callback: Try[A] => Unit): Unit =
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
            if (asking) conn.submit[Unit](Connection.asking, _ => ())
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
        discardLocked(connection)
        available.signalAll()
      }

  private[internal] def wakeWaiters(): Unit = locked(available.signalAll())

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
        // fail fast while not live: never reuse, establish, or park during a reconnect window, re-checked on every wakeup
        if (!isLive()) throw NotConnected()
        val reused    = takeIdleLocked()
        if (reused != null) return reused
        if (live.size + reserved < config.maxConnections) {
          reserved += 1
          return establishOutsideLock()
        }
        val remaining = deadlineNanos - System.nanoTime()
        if (remaining <= 0L) throw acquireTimedOut
        val _         = available.awaitNanos(remaining)
      }
      throw new IllegalStateException("unreachable")
    }
  }

  // entered holding the lock with `reserved` already incremented; drops the lock for the blocking establish, then re-accounts under it.
  private def establishOutsideLock(): DedicatedConnection = {
    lock.unlock()
    val connection =
      try DedicatedConnection.establish(factory, bootstrap, connectTimeoutMillis)
      catch {
        case e: Throwable =>
          locked { reserved -= 1; available.signalAll() }
          lock.lock() // re-take so acquire()'s `locked` block unlocks exactly once on exit
          throw e
      }
    lock.lock()
    reserved -= 1
    // stamp with the epoch live the moment it joins the pool, so a connection built across a reconnect is born current, never stale
    if (closing) {
      available.signalAll()
      scheduleClose(connection)
      throw NotConnected()
    }
    liveGeneration() match {
      case None      =>
        available.signalAll()
        scheduleClose(connection)
        throw NotConnected()
      case Some(gen) =>
        connection.stampEpoch(gen)
        live += connection
        connection
    }
  }

  private def acquireTimedOut: TimedOut =
    TimedOut(s"dedicated pool acquire timed out after ${config.acquireTimeout.toMillis}ms")

  private def takeIdleLocked(): DedicatedConnection = {
    var result: DedicatedConnection = null
    while (result == null && idle.nonEmpty) {
      val candidate = idle.removeLast()
      if (reusable(candidate)) result = candidate.connection
      else discardLocked(candidate.connection)
    }
    result
  }

  private def release(connection: DedicatedConnection): Unit =
    locked {
      if (closing || !healthyAndCurrent(connection)) discardLocked(connection)
      else idle.append(DedicatedPool.Idle(connection, scheduler.nowMillis))
      available.signalAll()
    }

  private def sweepExpired(): Unit = {
    val toClose = locked {
      val due     = Vector.newBuilder[DedicatedConnection]
      idle.filterInPlace { entry =>
        val keep = reusable(entry)
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
  private def healthyAndCurrent(connection: DedicatedConnection): Boolean =
    connection.isHealthy && isCurrent(connection.epoch)

  private def reusable(entry: DedicatedPool.Idle): Boolean =
    healthyAndCurrent(entry.connection) && !expired(entry)

  private def expired(entry: DedicatedPool.Idle): Boolean =
    config.idleTimeout.isFinite && scheduler.nowMillis - entry.idleSinceMillis >= config.idleTimeout.toMillis

  // must hold the lock; callers that free an occupied slot signal waiters themselves
  private def discardLocked(connection: DedicatedConnection): Unit = {
    live -= connection
    scheduleClose(connection)
  }

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

  def forConnection(
    factory: MultiplexedConnection.TransportFactory,
    bootstrap: Vector[Command[?]],
    scheduler: Scheduler,
    connection: MultiplexedConnection,
    config: DedicatedPoolConfig,
    connectTimeoutMillis: Long
  ): DedicatedPool = {
    val pool = new DedicatedPool(
      factory,
      bootstrap,
      scheduler,
      () => connection.isLive,
      () => connection.liveGeneration(),
      connection.isCurrent,
      config,
      connectTimeoutMillis
    )
    connection.setOnLivenessLost(() => pool.wakeWaiters())
    pool
  }

  final case class Idle(connection: DedicatedConnection, idleSinceMillis: Long)
}

package sage.client.internal

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.Try
import scala.util.control.NonFatal

import sage.SageException
import sage.SageException.{ConnectionLost, NotConnected, TimedOut}
import sage.client.DedicatedPoolConfig
import sage.commands.{Command, Connection}

/**
  * A pool of dedicated connections for blocking commands. Connections are created when needed, used by one command at a time, and returned
  * to the pool while healthy. A lost connection is discarded. When no idle connection is available, the pool opens a new one.
  *
  * A request made while the client is disconnected fails immediately with `NotConnected`. When all connections are busy, acquisition
  * waits up to `acquireTimeout` and then fails with `TimedOut`. Closing the pool also closes connections in use, causing their commands to
  * fail with `ConnectionLost(true)`.
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
  // connections whose socket is being opened outside the lock, so close() can abort one still connecting
  private val establishing                      = mutable.Set.empty[DedicatedConnection]
  private var reserved                          = 0
  private var closing                           = false
  private val sweepHandle: Scheduler.Cancelable =
    config.idleTimeout match {
      case interval: FiniteDuration => scheduler.every(interval)(sweepExpired())
      case _                        => null
    }

  /**
    * Runs a blocking command on a borrowed connection and releases it after the reply or failure. Acquisition runs on another thread
    * because it may wait for a pool slot or open a socket.
    */
  def use[A](command: Command[A], callback: Try[A] => Unit, lease: DedicatedPool.Lease = new DedicatedPool.Lease): Unit =
    leaseAndSubmit(command, asking = false, callback, lease)

  /**
    * Runs a blocking command after an `ASK` redirect. `ASKING` and the command are written consecutively on the same leased connection.
    * The `ASKING` reply is discarded, and the command's reply releases the connection.
    */
  def useAsking[A](command: Command[A], callback: Try[A] => Unit, lease: DedicatedPool.Lease): Unit =
    leaseAndSubmit(command, asking = true, callback, lease)

  private def leaseAndSubmit[A](command: Command[A], asking: Boolean, callback: Try[A] => Unit, lease: DedicatedPool.Lease): Unit =
    if (!isLive()) callback(scala.util.Failure(NotConnected()))
    else
      scheduler.after(Duration.Zero) {
        val acquired =
          try Right(acquire())
          catch {
            case e: SageException => Left(e)
            case NonFatal(_)      => Left(ConnectionLost(mayHaveExecuted = false)) // never reached the wire
          }
        acquired match {
          case Left(error) => callback(scala.util.Failure(error))
          case Right(conn) =>
            // attach fails only when already cancelled (interrupt before/between attaches); settle the callback here since cancel found no Held to settle
            val onInterrupt = () => callback(scala.util.Failure(ConnectionLost(mayHaveExecuted = true)))
            if (lease.attach(this, conn, onInterrupt)) {
              if (asking) conn.submit[Unit](Connection.asking, _ => ())
              conn.submit(
                command,
                result =>
                  if (lease.finish(conn)) {
                    release(conn)
                    callback(result)
                  }
              )
            } else onInterrupt()
        }
      }

  /**
    * Borrows a connection for the full duration of a transaction. This method can wait for a pool slot or open a socket, so callers must
    * run it on a blocking thread. It fails with `NotConnected` when the client is not live.
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
        available.signal()
      }

  private[internal] def wakeWaiters(): Unit = locked(available.signalAll())

  def close(): Unit = {
    val toClose = locked {
      closing = true
      if (sweepHandle != null) sweepHandle.cancel()
      available.signalAll()
      val snapshot = (live ++ establishing).toVector
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
        // reject acquisition while the shared connection is reconnecting; repeat the liveness check after each wake-up
        if (!isLive()) throw NotConnected()
        val reused    = takeIdleLocked()
        if (reused != null) return reused
        if (live.size + reserved < config.maxConnections) {
          reserved += 1
          return establishOutsideLock()
        }
        val remaining = deadlineNanos - System.nanoTime()
        if (remaining <= 0L) throw acquireTimedOut
        available.awaitNanos(remaining): Unit
      }
      throw new IllegalStateException("unreachable")
    }
  }

  // Entered holding the lock with `reserved` already incremented; registers the connection, drops the lock for the blocking establish, then
  // re-accounts under it.
  private def establishOutsideLock(): DedicatedConnection = {
    val connection = DedicatedConnection.create(factory, connectTimeoutMillis)
    establishing += connection
    lock.unlock()
    try connection.establish(bootstrap)
    catch {
      case e: Throwable =>
        locked {
          establishing -= connection
          reserved -= 1
          available.signal()
        }
        lock.lock() // re-take so acquire()'s `locked` block unlocks exactly once on exit
        throw e
    }
    lock.lock()
    establishing -= connection
    reserved -= 1
    // record the current generation only after the connection is ready to join the pool. This handles reconnects during establishment.
    if (closing) {
      available.signal()
      scheduleClose(connection)
      throw NotConnected()
    }
    liveGeneration() match {
      case None      =>
        available.signal()
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
      available.signal()
    }

  private def sweepExpired(): Unit = {
    val toClose = locked {
      val due     = Vector.newBuilder[DedicatedConnection]
      idle.filterInPlace { entry =>
        val keep = reusable(entry)
        if (!keep) due += entry.connection
        keep
      }
      val expired = due.result()
      expired.foreach(live -= _)
      expired
    }
    toClose.foreach(scheduleClose) // never close on the timer thread: close() joins I/O threads
  }

  // a connection from an older generation may still point to the previous server after a reconnect or DNS failover.
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

  // closing joins the connection's I/O threads. Schedule it outside the pool lock and timer thread.
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

  /**
    * Tracks the connection held by one blocking command, including after redirects. `attach` records a leased connection and its interruption
    * callback. `finish` clears the lease after a reply. `cancel` discards the current connection and invokes the callback, which completes
    * tracing and events for the interrupted command. Once cancelled, any later attachment is discarded immediately.
    */
  final class Lease {
    private val state = new AtomicReference[AnyRef]() // null idle, a Held while leased, Cancelled terminal

    private[internal] def attach(pool: DedicatedPool, conn: DedicatedConnection, onInterrupt: () => Unit): Boolean =
      if (state.compareAndSet(null, Held(pool, conn, onInterrupt))) true
      else {
        pool.releaseTransaction(conn, reusable = false)
        false
      }

    private[internal] def finish(conn: DedicatedConnection): Boolean =
      state.get() match {
        case h: Held if h.conn eq conn => state.compareAndSet(h, null)
        case _                         => false
      }

    def cancel(): Unit = state.getAndSet(Cancelled) match {
      case h: Held =>
        h.pool.releaseTransaction(h.conn, reusable = false)
        h.onInterrupt()
      case _       => ()
    }
  }

  final private case class Held(pool: DedicatedPool, conn: DedicatedConnection, onInterrupt: () => Unit)
  private case object Cancelled
}

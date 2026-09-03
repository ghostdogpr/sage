package sage.client.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.SageException
import sage.SageException.{ConnectionLost, LockLost, NotConnected, TimedOut}
import sage.client.DedicatedPoolConfig
import sage.commands.{Command, Connection, Reply, Role, Server}

/**
  * A pool of dedicated connections for blocking commands, transactions, and lock replication checks. Connections are created when needed,
  * used by one caller, and returned to the pool while healthy. A lost connection is discarded. When no idle connection is available, the
  * pool opens a new one.
  *
  * A request made while the client is disconnected fails immediately with `NotConnected`. When all connections are busy, acquisition
  * waits up to `acquireTimeout`, shortened by a lock write's deadline, and then fails with `TimedOut`. Closing the pool also closes connections
  * in use, causing their commands to fail with `ConnectionLost(true)`.
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
    useConnection(callback, lease) { (conn, complete) =>
      if (asking) conn.submit[Unit](Connection.asking, _ => ())
      conn.submit(command, complete)
    }

  // WAIT blocks its socket. Keep the write and confirmation on one leased connection so other commands can proceed independently.
  def useLockWrite[A](
    command: Command[A],
    asking: Boolean,
    replicas: Int,
    deadlineMillis: Long,
    callback: Try[A] => Unit,
    lease: DedicatedPool.Lease,
    onConfirmationFailure: () => Unit
  ): Unit = {
    val confirming = new AtomicBoolean(false)
    useConnection(callback, lease, () => if (confirming.get()) onConfirmationFailure(), Some(deadlineMillis)) { (conn, complete) =>
      // Once the write succeeds, a failed confirmation cannot make it safe to replay the write.
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
              val required = math.max(replicas, connected.size)
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
  }

  private def useConnection[A](
    callback: Try[A] => Unit,
    lease: DedicatedPool.Lease,
    onCancel: () => Unit = () => (),
    deadlineMillis: Option[Long] = None
  )(submit: (DedicatedConnection, Try[A] => Unit) => Unit): Unit =
    if (!isLive()) callback(Failure(NotConnected()))
    else if (!lease.beginAcquire(this)) callback(Failure(ConnectionLost(mayHaveExecuted = true)))
    else
      scheduler.after(Duration.Zero) {
        val acquired =
          try Right(acquire(Some(lease), deadlineMillis))
          catch {
            case e: SageException => Left(e)
            case NonFatal(_)      => Left(ConnectionLost(mayHaveExecuted = false))
          }
        acquired match {
          case Left(error) =>
            lease.endAcquire()
            callback(Failure(error))
          case Right(conn) =>
            // Cancellation can precede attachment while acquisition waits for a socket or pool slot.
            val onInterrupt = () => {
              onCancel()
              callback(Failure(ConnectionLost(mayHaveExecuted = true)))
            }
            if (lease.attach(this, conn, onInterrupt)) {
              submit(
                conn,
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

  private def acquire(lease: Option[DedicatedPool.Lease] = None, deadlineMillis: Option[Long] = None): DedicatedConnection = {
    val budgetNanos   = deadlineMillis.fold(config.acquireTimeout.toNanos) { deadline =>
      math.min(config.acquireTimeout.toNanos, (deadline - scheduler.nowMillis).millis.toNanos)
    }
    // Condition waits use real nanoseconds, so only the remaining duration crosses from the scheduler's monotonic clock.
    val deadlineNanos = System.nanoTime() + budgetNanos
    locked {
      while (true) {
        if (lease.exists(_.isCancelled)) throw ConnectionLost(mayHaveExecuted = true)
        if (closing) throw NotConnected()
        // reject acquisition while the shared connection is reconnecting; repeat the liveness check after each wake-up
        if (!isLive()) throw NotConnected()
        val remaining = deadlineNanos - System.nanoTime()
        // A lock deadline limits the write itself. An expired write must not take a slot even when one is immediately available.
        if (deadlineMillis.isDefined && remaining <= 0L) throw acquireTimedOut(budgetNanos.nanos)
        val reused    = takeIdleLocked()
        if (reused != null) return reused
        if (live.size + reserved < config.maxConnections) {
          reserved += 1
          return establishOutsideLock()
        }
        if (remaining <= 0L) throw acquireTimedOut(budgetNanos.nanos)
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

  private def acquireTimedOut(budget: FiniteDuration): TimedOut =
    TimedOut(s"dedicated pool acquire timed out after ${math.max(0L, budget.toMillis)}ms")

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
    * Tracks the connection held by one operation, including after redirects. `attach` records a leased connection and its interruption
    * callback. `finish` clears the lease after a reply. `cancel` discards the current connection and invokes the callback, which completes
    * tracing and events for the interrupted command. Cancellation wakes pool waiters. A connection acquired after cancellation is returned
    * to the pool because it has received no command from this operation.
    */
  final class Lease {
    private val state = new AtomicReference[AnyRef]() // null idle, Waiting during acquisition, Held while leased, Cancelled terminal

    private[internal] def beginAcquire(pool: DedicatedPool): Boolean = state.compareAndSet(null, Waiting(pool))

    private[internal] def endAcquire(): Unit = state.get() match {
      case w: Waiting => state.compareAndSet(w, null): Unit
      case _          => ()
    }

    private[internal] def isCancelled: Boolean = state.get() == Cancelled

    private[internal] def attach(pool: DedicatedPool, conn: DedicatedConnection, onInterrupt: () => Unit): Boolean = {
      val attached = state.get() match {
        case w: Waiting => state.compareAndSet(w, Held(pool, conn, onInterrupt))
        case _          => false
      }
      if (!attached) pool.releaseTransaction(conn, reusable = true)
      attached
    }

    private[internal] def finish(conn: DedicatedConnection): Boolean =
      state.get() match {
        case h: Held if h.conn eq conn => state.compareAndSet(h, null)
        case _                         => false
      }

    def cancel(): Unit = state.getAndSet(Cancelled) match {
      case w: Waiting => w.pool.wakeWaiters()
      case h: Held    =>
        h.pool.releaseTransaction(h.conn, reusable = false)
        h.onInterrupt()
      case _          => ()
    }
  }

  final private case class Waiting(pool: DedicatedPool)
  final private case class Held(pool: DedicatedPool, conn: DedicatedConnection, onInterrupt: () => Unit)
  private case object Cancelled
}

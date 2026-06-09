package sage.client.internal

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.Bytes
import sage.SageException
import sage.SageException.{ConnectionLost, NotConnected}
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.commands.{Command, Connection, Reply}
import sage.protocol.Frame

/**
  * The Multiplexed Connection: one auto-pipelined connection carrying every ordinary command, replies matched FIFO. It owns the
  * connection lifecycle — auto-reconnect with jittered backoff, the HELLO bootstrap run on each fresh connection, the idle-PING watchdog,
  * and graceful drain on close. Per-attempt reconnection re-invokes the [[TransportFactory]], which re-resolves the hostname so a DNS
  * repoint after failover lands on the new master.
  *
  * Each connection generation is a self-contained [[Conn]] owning its own `pending` queue, so a dead generation's late frames can never
  * misalign the live one. The reconnect loop runs off the reader thread (via the [[Scheduler]]) so backoff never blocks I/O.
  */
final private[client] class MultiplexedConnection private (
  factory: MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  bootstrap: Vector[Command[?]],
  backoff: BackoffConfig,
  watchdog: WatchdogConfig,
  connectTimeout: FiniteDuration,
  closeTimeout: FiniteDuration
) {
  import MultiplexedConnection.State

  // ReentrantLock, not `synchronized`: a parking lock never pins a virtual thread's carrier (a monitor does on JDK < 24).
  private val lock                                 = new ReentrantLock()
  private var state: State                         = State.Reconnecting
  private var current: Conn                        = null
  private var establishing: Conn                   = null
  private var watchdogHandle: Scheduler.Cancelable = null
  // bumped when a fresh socket becomes live; the dedicated pool stamps borrowed connections with it to detect ones outliving a reconnect
  private var generation: Long                     = 0L

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  def submit[A](command: Command[A], callback: Try[A] => Unit): Unit = {
    val conn = locked(if (state == State.Live) current else null)
    if (conn == null) callback(Failure(NotConnected()))
    else conn.submit(command, callback)
  }

  // ASKING must immediately precede its command on the wire (it arms the target node for the next command on the connection). Writing the
  // pair as one batch keeps them adjacent and FIFO-matched even though every fiber shares this connection; the ASKING reply is discarded.
  def submitAsking[A](command: Command[A], callback: Try[A] => Unit): Unit = {
    val conn = locked(if (state == State.Live) current else null)
    if (conn == null) callback(Failure(NotConnected()))
    else conn.submitAll(Vector(Connection.asking, command), Vector(_ => (), callback.asInstanceOf[Try[Any] => Unit]))
  }

  // Enqueues a whole pipeline onto a single generation, captured once, so a reconnect mid-batch can never split it across connections.
  // Returns false when not connected — nothing was submitted, so the caller fails fast rather than fabricating per-position errors.
  def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Boolean = {
    val conn = locked(if (state == State.Live) current else null)
    if (conn == null) false
    else {
      conn.submitAll(commands, callbacks)
      true
    }
  }

  def close(): Unit = {
    // `aborting`: a reconnect attempt's in-flight connection, closed so its socket is released before close() returns.
    val (draining, aborting) = locked {
      state match {
        case State.Closed | State.Draining => (null, null)
        case State.Reconnecting            =>
          state = State.Closed
          stopWatchdog()
          (null, establishing)
        case State.Live                    =>
          state = State.Draining
          (current, null)
      }
    }
    if (aborting != null) aborting.close()
    if (draining != null) {
      val _ = draining.beginDrain().await(closeTimeout.toMillis, TimeUnit.MILLISECONDS)
      locked(stopWatchdog())
      draining.close()
    }
  }

  private[internal] def currentState: State = locked(state)

  private[internal] def currentGeneration: Long = locked(generation)

  private def connectInitial(): Unit = {
    val conn = establish() // the first connect propagates a handshake failure; only reconnects retry
    locked {
      current = conn
      state = State.Live
      generation += 1
      startWatchdog()
    }
  }

  private def establish(): Conn = {
    val conn = new Conn
    locked { establishing = conn }
    try {
      conn.start()
      bootstrap.foreach { command =>
        val latch   = new CountDownLatch(1)
        val outcome = new AtomicReference[Try[Any]]()
        conn.submit[Any](
          command.asInstanceOf[Command[Any]],
          result => {
            outcome.set(result)
            latch.countDown()
          }
        )
        // a half-open peer can accept the socket yet never answer HELLO; bound the wait so the reconnect loop can't stall on it forever
        val replied = latch.await(connectTimeout.toMillis, TimeUnit.MILLISECONDS)
        if (!replied) {
          conn.close()
          throw ConnectionLost(mayHaveExecuted = false)
        }
        outcome.get() match {
          case Failure(error) =>
            conn.close()
            throw error
          case Success(_)     => ()
        }
      }
      conn
    } finally locked { if (establishing eq conn) establishing = null }
  }

  private def scheduleReconnect(attempt: Int): Unit = {
    val base    = backoffBaseMillis(attempt)
    val delayMs = scheduler.jitterMillis(base + 1)
    scheduler.after(delayMs.millis)(attemptReconnect(attempt))
  }

  private def attemptReconnect(attempt: Int): Unit = {
    val proceed = locked(state == State.Reconnecting)
    if (proceed)
      try {
        val conn = establish()
        val live = locked {
          if (state == State.Reconnecting) {
            current = conn
            state = State.Live
            generation += 1
            true
          } else false
        }
        if (!live) conn.close()
      } catch {
        case NonFatal(_) => locked(if (state == State.Reconnecting) scheduleReconnect(attempt + 1))
      }
  }

  private def backoffBaseMillis(attempt: Int): Long = {
    val capped = backoff.maxDelay.toMillis
    val raw    = backoff.initialDelay.toMillis.toDouble * math.pow(backoff.multiplier, attempt.toDouble)
    if (raw.isInfinite || raw >= capped.toDouble) capped else math.max(0L, raw.toLong)
  }

  // Connections that never became `current` (a failed establish) are ignored here; their caller handles them.
  private def onConnTerminated(conn: Conn): Unit = locked {
    if (conn eq current)
      state match {
        case State.Live | State.Reconnecting => state = State.Reconnecting; scheduleReconnect(0)
        case State.Draining                  => state = State.Closed; stopWatchdog()
        case State.Closed                    => ()
      }
  }

  private def startWatchdog(): Unit =
    if (watchdog.enabled && watchdogHandle == null)
      watchdogHandle = scheduler.every(watchdog.pingInterval)(watchdogTick())

  private def stopWatchdog(): Unit =
    if (watchdogHandle != null) {
      watchdogHandle.cancel()
      watchdogHandle = null
    }

  private def watchdogTick(): Unit = {
    val conn = locked(if (state == State.Live) current else null)
    if (conn != null) conn.checkLiveness(scheduler.nowMillis, watchdog.pingInterval.toMillis, watchdog.pingTimeout.toMillis)
  }

  final private class Conn {

    private val pending                              = new ConcurrentLinkedQueue[Entry[?]]()
    private val transportRef                         = new AtomicReference[Transport]()
    @volatile private var lastReplyAtMillis: Long    = scheduler.nowMillis
    @volatile private var drainLatch: CountDownLatch = null
    @volatile private var aborted: Boolean           = false

    // transportRef is published before start()'s blocking connect, so a concurrent close() can abort it; `aborted` covers the
    // narrow gap where close() lands before the transport is published.
    def start(): Unit = {
      val transport = factory(onFrame, onClosed)
      transportRef.set(transport)
      if (aborted) transport.close()
      else transport.start()
    }

    def submit[A](command: Command[A], callback: Try[A] => Unit): Unit =
      transportRef.get().send(new Entry(command, callback))

    // One Transport.Item, not N: the writer treats a queue element atomically, so concatenating the batch into a single payload is what
    // guarantees the pipeline is one socket write (one round-trip) rather than racing the writer between sends.
    def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Unit = {
      val entries = Vector.tabulate(commands.length)(i => new Entry(commands(i).asInstanceOf[Command[Any]], callbacks(i)))
      transportRef.get().send(new Batch(entries))
    }

    def close(): Unit = {
      aborted = true
      val transport = transportRef.get()
      if (transport != null) transport.close()
    }

    def beginDrain(): CountDownLatch = {
      val latch = new CountDownLatch(1)
      drainLatch = latch
      if (pending.isEmpty) latch.countDown()
      latch
    }

    def checkLiveness(now: Long, intervalMillis: Long, timeoutMillis: Long): Unit = {
      val head = pending.peek()
      if (head != null) {
        // offload: close() blocks joining I/O threads, and the watchdog tick runs on the shared timer thread, which must not block
        if (now - head.sentAtMillis >= timeoutMillis) scheduler.after(Duration.Zero)(close())
      } else if (now - lastReplyAtMillis >= intervalMillis) submit(Connection.ping(None), _ => ())
    }

    // Out-of-band frames never consume a pending entry. A READONLY reply fails its command, then poisons the connection: an in-place
    // failover demotes the old master without dropping the socket, so it looks healthy but rejects writes.
    private def onFrame(frame: Frame): Unit = {
      lastReplyAtMillis = scheduler.nowMillis
      frame match {
        case _: Frame.Push | _: Frame.Attribute => ()
        case reply                              =>
          val entry = pending.poll()
          if (entry == null) close()
          else {
            entry.complete(reply)
            if (drainLatch != null && pending.isEmpty) drainLatch.countDown()
          }
          if (Poison.isReadonly(reply)) close()
      }
    }

    private def onClosed(): Unit = {
      var entry = pending.poll()
      while (entry != null) {
        entry.fail(ConnectionLost(mayHaveExecuted = true))
        entry = pending.poll()
      }
      if (drainLatch != null) drainLatch.countDown()
      onConnTerminated(this)
    }

    final private class Entry[A](command: Command[A], callback: Try[A] => Unit) extends Transport.Item {

      @volatile var sentAtMillis: Long = 0L

      val payload: Bytes = command.encode

      def writeAttempted(): Unit = {
        sentAtMillis = scheduler.nowMillis
        val _ = pending.add(this)
      }

      def dropped(): Unit = callback(Failure(ConnectionLost(mayHaveExecuted = false)))

      // the catch guards against throwing user decoders: an escaped exception here would lose the callback and hang the awaiting fiber
      def complete(frame: Frame): Unit = {
        val result =
          try
            Reply.run(command, frame) match {
              case Right(value) => Success(value)
              case Left(error)  => Failure(error)
            }
          catch {
            case NonFatal(error) => Failure(error)
          }
        callback(result)
      }

      def fail(error: SageException): Unit = callback(Failure(error))
    }

    // A pipeline as one write unit: its payload is the entries concatenated, and its write/drop hooks fan out to every entry so each is
    // matched and failed individually. A whole batch is therefore written — or dropped — atomically, never split across socket writes.
    final private class Batch(entries: Vector[Entry[Any]]) extends Transport.Item {

      val payload: Bytes = Bytes.concat(entries.map(_.payload))

      def writeAttempted(): Unit = entries.foreach(_.writeAttempted())

      def dropped(): Unit = entries.foreach(_.dropped())
    }
  }
}

private[client] object MultiplexedConnection {

  type TransportFactory = (Frame => Unit, () => Unit) => Transport

  enum State {
    case Live, Reconnecting, Draining, Closed
  }

  /**
    * Connects and runs the bootstrap synchronously; throws (no retry) if the first handshake fails.
    */
  def connect(
    factory: TransportFactory,
    scheduler: Scheduler,
    bootstrap: Vector[Command[?]],
    backoff: BackoffConfig,
    watchdog: WatchdogConfig,
    connectTimeout: FiniteDuration,
    closeTimeout: FiniteDuration
  ): MultiplexedConnection = {
    val connection = new MultiplexedConnection(factory, scheduler, bootstrap, backoff, watchdog, connectTimeout, closeTimeout)
    connection.connectInitial()
    connection
  }
}

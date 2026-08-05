package sage.client.internal

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.ReentrantLock

import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.{Bytes, CommandSpan, Outcome, SageEvent, SageException}
import sage.SageException.{ConnectionLost, NotConnected}
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.{Command, Connection, Invalidation, Reply}
import sage.protocol.Frame

/**
  * Maintains one auto-pipelined connection for ordinary commands and matches replies in order. It reconnects with jittered backoff, runs
  * `HELLO` on each new connection, sends idle `PING` checks, and waits for accepted commands while closing. Each reconnect invokes the
  * [[TransportFactory]] again and resolves the hostname again, allowing DNS changes after failover to select the new master.
  *
  * A new connection gets a new `pending` queue, which prevents late frames from a closed connection from affecting the current one. The
  * [[Scheduler]] runs reconnect delays outside the reader thread.
  */
final private[client] class MultiplexedConnection private (
  factory: MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  bootstrap: Vector[Command[?]],
  backoff: BackoffConfig,
  watchdog: WatchdogConfig,
  connectTimeout: FiniteDuration,
  closeTimeout: FiniteDuration,
  cacheMaxBytes: Long,
  node: Option[Node],
  events: Events
) {
  import MultiplexedConnection.{Generation, State}

  // use ReentrantLock because a waiting virtual thread can unmount. A synchronized monitor can pin its carrier thread on JDK versions before 24.
  private val lock                                 = new ReentrantLock()
  private var state: State                         = State.Reconnecting
  private var current: Conn                        = null
  private var establishing: Conn                   = null
  private var watchdogHandle: Scheduler.Cancelable = null
  // increment when a new socket becomes live; the dedicated pool records this value and rejects connections from an earlier generation
  private var generation: Generation               = Generation.initial
  // Store the current live generation, or notLive, in an atomic reference. The dedicated pool can read it while holding the pool lock without
  // also acquiring this connection's lifecycle lock.
  private val liveEpoch                            = new AtomicReference[Generation](Generation.notLive)
  @volatile private var onLivenessLost: () => Unit = () => ()
  // keep the reconnect attempt count across short-lived connections, increasing their backoff until a connection remains stable
  private var reconnectAttempt: Int                = 0
  private var liveSinceMillis: Long                = -1L
  // whether the live generation accepts CLIENT TRACKING; false makes cached reads run uncached
  @volatile private var trackingActive             = true

  private[internal] def setOnLivenessLost(hook: () => Unit): Unit = onLivenessLost = hook

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  // the sole mutator of `state`, keeping `liveEpoch` in step. Must hold `lock`; at a Live edge the caller bumps `generation` first.
  private def transition(to: State): Unit = {
    state = to
    liveEpoch.set(if (to == State.Live) generation else Generation.notLive)
    if (to == State.Live) liveSinceMillis = scheduler.nowMillis
  }

  // return the current connection only in the Live state; disconnected or reconnecting callers receive null and fail immediately
  private inline def liveConn(): Conn = locked(if (state == State.Live) current else null)

  // Add n accepted entries to inFlight while holding the lock that admits the command. This records them before close() reads inFlight, including
  // commands that have not reached Conn.submit yet (#95). A null result means the connection is not Live.
  private def reserved(n: Int): Conn = locked(if (state == State.Live) {
    current.reserve(n)
    current
  } else null)

  def submit[A](command: Command[A], callback: Try[A] => Unit): Unit = {
    val conn = reserved(1)
    if (conn == null) callback(Failure(NotConnected()))
    else conn.submit(command, callback)
  }

  // Use one connection for the cache lookup and server fetch; reconnecting during the fetch reports a connection loss. In master-replica mode,
  // deferred contains tracing context captured before offloading. A null value starts tracing on this thread. Local cache hits are not traced.
  def cachedSubmit[A](command: Command[A], ttlMillis: Long, callback: Try[A] => Unit, deferred: () => CommandSpan = null): Unit = {
    // a Fetch sends [CLIENT CACHING YES, read]; a cache hit/wait sends nothing and releases the reservation
    val conn = reserved(2)
    if (conn == null) {
      val error = NotConnected()
      Events.settleSpan(Events.startOrDefer(events, command, deferred), Outcome.Failed(error))
      callback(Failure(error))
    } else conn.cachedSubmit(command, ttlMillis, callback, deferred)
  }

  // ASKING must immediately precede its command on the wire (it arms the target node for the next command on the connection). Writing the
  // pair as one batch keeps them adjacent and FIFO-matched even though every fiber shares this connection; the ASKING reply is discarded.
  def submitAsking[A](command: Command[A], callback: Try[A] => Unit): Unit = {
    val conn = reserved(2)
    if (conn == null) callback(Failure(NotConnected()))
    else conn.submitAll(Vector(Connection.asking, command), Vector(_ => (), callback.asInstanceOf[Try[Any] => Unit]))
  }

  // Enqueues a whole pipeline onto one captured generation. A reconnect cannot split the batch across connections.
  // Return false when disconnected. The caller handles the unsent batch by rerouting or failing it as appropriate.
  def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Boolean = {
    val conn = reserved(commands.length)
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
          transition(State.Closed)
          stopWatchdog()
          (null, establishing)
        case State.Live                    =>
          transition(State.Draining)
          (current, null)
      }
    }
    if (aborting != null) aborting.close()
    if (draining != null) {
      draining.beginDrain().await(closeTimeout.toMillis, TimeUnit.MILLISECONDS)
      locked(stopWatchdog())
      draining.close()
    }
  }

  private[internal] def currentState: State = locked(state)

  private[internal] def isLive: Boolean = liveEpoch.get() != Generation.notLive

  // return the current generation for a new DedicatedConnection while this connection is Live; None reports that it is unavailable
  private[internal] def liveGeneration(): Option[Generation] = {
    val g = liveEpoch.get()
    if (g == Generation.notLive) None else Some(g)
  }

  // Compare a DedicatedConnection's generation with the current live generation. The `notLive` value rejects every generation while
  // reconnecting, including the previous number before the next connection increments it.
  private[internal] def isCurrent(g: Generation): Boolean = liveEpoch.get() == g

  private def connectInitial(): Unit = {
    val conn     = establish() // the first connect propagates a handshake failure; only reconnects retry
    // Emit while holding the lock to keep the event ordered with the state transition. If the socket drops immediately afterward,
    // Disconnected is enqueued after Connected because the same lock serializes both events.
    val teardown = locked {
      // if close runs during establishment, close the new connection instead of changing the state to Live
      if (state == State.Closed) conn
      else if (conn.isTerminated) {
        scheduleReconnect(0)
        null
      } else {
        current = conn
        generation = generation.next
        transition(State.Live)
        startWatchdog()
        events.emit(SageEvent.Connection.Connected(node))
        null
      }
    }
    if (teardown != null) teardown.close()
  }

  private def establish(): Conn = {
    val conn = new Conn
    locked {
      establishing = conn
      if (state == State.Closed) conn.close()
    }
    try {
      conn.start()
      var tracking = true
      // Call reserve(1) for each command because submit does not increment the count. The reply later calls retire() to balance it. A
      // half-open peer can accept the socket without answering HELLO, so the runner limits each wait and lets the reconnect loop continue.
      Bootstrap.run(
        bootstrap,
        connectTimeout.toMillis,
        (c, cb) => {
          conn.reserve(1)
          conn.submit(c, cb)
        },
        () => conn.close(),
        onTolerated = c => if (Connection.isClientTracking(c)) tracking = false
      )
      trackingActive = tracking
      conn
    } finally locked { if (establishing eq conn) establishing = null }
  }

  private[internal] def flushCache(): Unit = {
    val c = locked(current)
    if (c != null) c.flushCache()
  }

  private def scheduleReconnect(attempt: Int): Unit = {
    reconnectAttempt = attempt
    scheduler.after(Backoff.jitteredMillis(backoff, attempt, scheduler).millis)(attemptReconnect(attempt))
  }

  // reset the attempt count after a connection remains live for the maximum backoff period; shorter connections keep increasing the delay
  private def nextReconnectAttempt(): Int = {
    val stable = liveSinceMillis >= 0L && scheduler.nowMillis - liveSinceMillis >= backoff.maxDelay.toMillis
    reconnectAttempt = if (stable) 0 else reconnectAttempt + 1
    reconnectAttempt
  }

  private def attemptReconnect(attempt: Int): Unit = {
    val proceed = locked(state == State.Reconnecting)
    if (proceed)
      try {
        val conn = establish()
        val live = locked {
          if (state == State.Reconnecting && !conn.isTerminated) {
            current = conn
            generation = generation.next
            transition(State.Live)
            startWatchdog()
            events.emit(SageEvent.Connection.Connected(node))
            true
          } else false
        }
        if (!live) {
          conn.close()
          locked(if (state == State.Reconnecting) scheduleReconnect(attempt + 1))
        }
      } catch {
        case NonFatal(error) =>
          locked(if (state == State.Reconnecting) {
            events.emit(SageEvent.Connection.ReconnectFailed(node, error))
            scheduleReconnect(attempt + 1)
          })
      }
  }

  // ignore connections that fail before becoming `current`; the establishment caller handles those failures
  private def onConnTerminated(conn: Conn): Unit = {
    // emit Disconnected only when the current Live connection ends. Holding the lock orders it after that connection's Connected event.
    val lostLiveness = locked {
      if (conn eq current)
        state match {
          case State.Live                        =>
            transition(State.Reconnecting)
            scheduleReconnect(nextReconnectAttempt())
            events.emit(SageEvent.Connection.Disconnected(node))
            true
          case State.Draining                    =>
            transition(State.Closed)
            stopWatchdog()
            false
          case State.Reconnecting | State.Closed => false
        }
      else false
    }
    // notify the pool after releasing the lifecycle lock
    if (lostLiveness) onLivenessLost()
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
    val conn = liveConn()
    if (conn != null) conn.checkLiveness(scheduler.nowMillis, watchdog.pingInterval.toMillis, watchdog.pingTimeout.toMillis)
  }

  private def decodeFrame[A](command: Command[A], frame: Frame): Try[A] = Reply.decode(command, frame)

  final private class Conn {

    private val pending                              = new ConcurrentLinkedQueue[Entry[?]]()
    // count entries when they are accepted by submit, including commands still queued for writing, so close waits for all accepted work (#95)
    private val inFlight                             = new AtomicInteger(0)
    private val transportRef                         = new AtomicReference[Transport]()
    // each connection has its own cache. Reconnecting creates a new Conn and discards the previous cached values.
    private val cache                                = new ClientCache(cacheMaxBytes)
    @volatile private var lastReplyAtMillis: Long    = scheduler.nowMillis
    @volatile private var drainLatch: CountDownLatch = null
    @volatile private var aborted: Boolean           = false
    @volatile private var terminated: Boolean        = false

    def isTerminated: Boolean = terminated

    // Publish transportRef before the blocking connect starts so close can abort it. aborted records a close that happens before transportRef
    // is published.
    def start(): Unit = {
      val transport = factory(onFrame, onClosed)
      transportRef.set(transport)
      if (aborted) transport.close()
      else transport.start()
    }

    // inFlight is reserved by the caller (reserve below) before the send, so neither submit nor submitAll touches the counter here.
    def submit[A](command: Command[A], callback: Try[A] => Unit): Unit =
      transportRef.get().send(new Entry(command, callback))

    // Concatenate the pipeline into one Transport.Item. The writer processes an item atomically, which keeps the pipeline in one socket write
    // and prevents other sends from being inserted between its commands.
    def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Unit = {
      val entries = Vector.tabulate(commands.length)(i => new Entry(commands(i), callbacks(i)))
      transportRef.get().send(new Batch(entries))
    }

    // increment inFlight for accepted entries. retire() decrements it after completion, and release() decrements it for unsent entries.
    def reserve(n: Int): Unit =
      inFlight.addAndGet(n): Unit

    // OPTIN tracking applies CLIENT CACHING YES only to the next command. Submit it together with the cached read to keep them adjacent. An
    // identity decoder passes the raw reply Frame to the cache, and each waiter then uses its own command decoder.
    def cachedSubmit[A](command: Command[A], ttlMillis: Long, callback: Try[A] => Unit, deferred: () => CommandSpan): Unit = {
      // tracking off: run uncached, releasing one of the two slots reserved for the (now unsent) caching prefix
      if (!trackingActive) {
        release(1)
        uncached(command, callback, deferred)
        return
      }
      val commandBytes                = command.encode
      val keys                        = command.keys
      def deliver(frame: Frame): Unit = callback(decodeFrame(command, frame))
      val waiter: Try[Frame] => Unit  = {
        case Success(frame) => deliver(frame)
        case Failure(error) => callback(Failure(error))
      }
      @tailrec def attempt(): Unit    =
        cache.acquire(commandBytes, keys, scheduler.nowMillis, waiter) match {
          // A Hit serves locally; a Wait coalesces onto an in-flight fetch — both avoid a server round trip, so both are reported as a hit
          // and release the two slots reserved for the (now unsent) fetch
          case ClientCache.Acquire.Hit(frame, epoch) =>
            if (cache.isCurrent(epoch)) {
              release(2)
              if (events.emitsEvents) events.emit(SageEvent.Cache.Hit(command.name))
              deliver(frame)
            }
            // reroute a hit retired by a topology change or a dead connection; refetch here one retired by a server flush (ownership unchanged)
            else if (terminated || cache.rerouteRetired(epoch)) {
              release(2)
              callback(Failure(ConnectionLost(mayHaveExecuted = false)))
            } else attempt()
          case ClientCache.Acquire.Wait              =>
            release(2)
            if (events.emitsEvents) events.emit(SageEvent.Cache.Hit(command.name))
          case ClientCache.Acquire.Fetch             =>
            if (events.emitsEvents) events.emit(SageEvent.Cache.Miss(command.name))
            traced(command, deferred) { settle =>
              val raw                         = Command[Frame](command.name, command.keyIndices, command.args, frame => Right(frame))
              val onReply: Try[Frame] => Unit = { result =>
                result match {
                  case Success(frame) => cache.store(commandBytes, keys, frame, scheduler.nowMillis, ttlMillis)
                  case Failure(error) => cache.fail(commandBytes, error)
                }
                // the outcome reflects the decoded reply, not the raw identity-decoded frame
                settle(Outcome.of(result.flatMap(decodeFrame(command, _))))
              }
              try submitAll(Vector(Connection.clientCachingYes, raw), Vector(_ => (), onReply.asInstanceOf[Try[Any] => Unit]))
              catch {
                // nothing was sent: release the slots the entries won't retire, and fail the fetch as a reply failure would
                case NonFatal(error) =>
                  release(2)
                  onReply(Failure(error))
              }
            }
        }
      attempt()
    }

    private def uncached[A](command: Command[A], callback: Try[A] => Unit, deferred: () => CommandSpan): Unit =
      traced(command, deferred)(settle =>
        submit(
          command,
          (result: Try[A]) => {
            settle(Outcome.of(result))
            callback(result)
          }
        )
      )

    // record tracing and CommandCompleted events around a command sent to the server. send calls the supplied function with the outcome.
    private def traced(command: Command[?], deferred: () => CommandSpan)(send: ((=> Outcome) => Unit) => Unit): Unit = {
      val span    = Events.startOrDefer(events, command, deferred)
      node.foreach(Events.routeSpan(span, _))
      val started = System.nanoTime()
      send { outcome =>
        if (events.enabled) {
          val settled = outcome
          Events.settleSpan(span, settled)
          if (events.emitsEvents)
            events.emit(SageEvent.CommandCompleted(command.name, node, FiniteDuration(System.nanoTime() - started, NANOSECONDS), settled))
        }
      }
    }

    def flushCache(): Unit = cache.flushForReroute()

    def close(): Unit = {
      aborted = true
      val transport = transportRef.get()
      if (transport != null) transport.close()
    }

    def beginDrain(): CountDownLatch = {
      val latch = new CountDownLatch(1)
      drainLatch = latch
      if (inFlight.get() == 0) latch.countDown()
      latch
    }

    private def retire(): Unit = release(1)

    private def release(n: Int): Unit =
      if (inFlight.addAndGet(-n) == 0) {
        val latch = drainLatch
        if (latch != null) latch.countDown()
      }

    def checkLiveness(now: Long, intervalMillis: Long, timeoutMillis: Long): Unit = {
      val head = pending.peek()
      if (head != null) {
        // offload: close() blocks joining I/O threads, and the watchdog tick runs on the shared timer thread, which must not block
        if (now - head.sentAtMillis >= timeoutMillis) scheduler.after(Duration.Zero)(close())
      } else if (now - lastReplyAtMillis >= intervalMillis) {
        reserve(1)
        submit(Connection.ping(None), _ => ())
      }
    }

    // Out-of-band frames do not consume a pending entry. A READONLY reply fails its command and closes the connection because an in-place
    // failover can leave the old master connected but unable to accept writes
    private def onFrame(frame: Frame): Unit =
      frame match {
        case Frame.Push(elements) =>
          // a push confirms only that reads are working. Leave lastReplyAtMillis unchanged so push-only traffic still receives idle PING checks.
          Invalidation.decode(elements) match {
            case Some(Invalidation.Evict(keys)) => keys.foreach(cache.invalidate)
            case Some(Invalidation.FlushAll)    => cache.flush()
            case None                           => ()
          }
        case reply                =>
          lastReplyAtMillis = scheduler.nowMillis
          val entry = pending.poll()
          if (entry == null) close()
          else entry.complete(reply)
          if (Poison.isReadonly(reply)) close()
      }

    private def onClosed(): Unit = {
      terminated = true
      // a dropped connection loses all further invalidations, so its cache can no longer be trusted for a hit
      cache.flush()
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

      var payload: Bytes = command.encode

      override def clearPayload(): Unit = payload = Bytes.empty

      def writeAttempted(): Unit = {
        sentAtMillis = scheduler.nowMillis
        pending.add(this): Unit
      }

      def dropped(): Unit = {
        callback(Failure(ConnectionLost(mayHaveExecuted = false)))
        retire()
      }

      // decodeFrame guards against throwing user decoders: an escaped exception would otherwise lose the callback and hang the awaiting fiber
      def complete(frame: Frame): Unit = {
        callback(decodeFrame(command, frame))
        retire()
      }

      def fail(error: SageException): Unit = {
        callback(Failure(error))
        retire()
      }
    }

    // Concatenate a pipeline into one transport write and notify each entry individually when the write succeeds or fails; the transport
    // writes or drops the complete batch without splitting it across socket writes
    final private class Batch(entries: Vector[Entry[Any]]) extends Transport.Item {

      val payload: Bytes = Bytes.concatBy(entries)(_.payload)

      override def clearPayload(): Unit = entries.foreach(_.clearPayload())

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

  // a monotonic generation for the current socket; the pool records it on each DedicatedConnection and rejects the connection after it changes
  opaque type Generation = Long
  object Generation {
    val initial: Generation                        = 0L
    // distinct from every real generation, making all recorded generations invalid while disconnected
    val notLive: Generation                        = -1L
    extension (g: Generation) def next: Generation = g + 1L
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
    closeTimeout: FiniteDuration,
    cacheMaxBytes: Long = 0L,
    node: Option[Node] = None,
    events: Events = Events.disabled,
    onConstructed: MultiplexedConnection => Unit = _ => ()
  ): MultiplexedConnection = {
    val connection =
      new MultiplexedConnection(factory, scheduler, bootstrap, backoff, watchdog, connectTimeout, closeTimeout, cacheMaxBytes, node, events)
    // expose the instance before the blocking connect so an owner can abort it from close()
    onConstructed(connection)
    connection.connectInitial()
    connection
  }
}

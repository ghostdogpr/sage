package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

import sage.{Bytes, SageEvent}
import sage.SageException.NotConnected
import sage.client.SageConfig
import sage.cluster.Node
import sage.commands.{Command, Connection, Invalidation, Reply}
import sage.protocol.Frame

/**
  * Maintains one auto-pipelined connection for ordinary commands and matches replies in order. It reconnects with jittered backoff, runs
  * `HELLO` on each new connection, sends idle `PING` checks, and waits for accepted commands while closing. Each reconnect invokes the
  * [[TransportFactory]] again and resolves the hostname again, allowing DNS changes after failover to select the new master.
  *
  * A new connection gets a new `pending` queue, which prevents late frames from a closed connection from affecting the current one. The
  * [[Scheduler]] runs reconnect delays outside the reader thread. The connection owns its node's [[DedicatedPool]] and sends blocking
  * commands there.
  */
final private[client] class MultiplexedConnection(
  factory: MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  config: SageConfig,
  role: MultiplexedConnection.NodeRole,
  node: Option[Node] = None,
  events: Events = Events.disabled
) {
  import MultiplexedConnection.State

  // use ReentrantLock because a waiting virtual thread can unmount. A synchronized monitor can pin its carrier thread on JDK versions before 24.
  private val lock                   = new ReentrantLock()
  // readable by the dedicated pool without this connection's lifecycle lock
  @volatile private var state: State = State.Reconnecting
  private var current: Conn          = null
  private var establishing: Conn     = null
  private val reconnects             = new Reconnects(scheduler, config.reconnect, lock)
  private val bootstrap              = Bootstrap.commands(config) ++ role.setup
  private[internal] val pool         =
    new DedicatedPool(factory, bootstrap, scheduler, () => isLive, config.dedicatedPool, config.connectTimeout.toMillis)

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  private def goLive(conn: Conn): Unit = {
    current = conn
    state = State.Live
    reconnects.live()
    conn.watch()
    events.emit(SageEvent.Connection.Connected(node))
  }

  // Add n accepted entries to inFlight while holding the lock that admits the command. This records them before close() reads inFlight, including
  // commands that have not reached Conn.submit yet (#95). A null result means the connection is not Live.
  private def reserved(n: Int): Conn = locked(if (state == State.Live) {
    current.reserve(n)
    current
  } else null)

  // A supplied lease lets an interrupted caller release the slot; blocking commands without one use a private lease. ASKING must immediately
  // precede its command on the wire (it arms the target node for the next command on the connection). Writing the pair as one batch keeps them
  // adjacent and FIFO-matched even though every fiber shares this connection; the ASKING reply is discarded.
  def submit[A](command: Command[A], callback: Try[A] => Unit, asking: Boolean = false, lease: DedicatedPool.Lease = null): Unit =
    if (command.isBlocking) pool.use(command, callback, if (lease != null) lease else new DedicatedPool.Lease, asking)
    else {
      val conn = reserved(if (asking) 2 else 1)
      if (conn == null) callback(Failure(NotConnected()))
      else if (asking) conn.submitAfter(Connection.asking, command, callback)
      else conn.write(command, callback)
    }

  // Use one connection for the cache lookup and server fetch; reconnecting during the fetch reports a connection loss. trace tracks the read
  // once it is sent or fails.
  def cachedSubmit[A](command: Command[A], ttlMillis: Long, callback: Try[A] => Unit, trace: Events.Fetching): Unit = {
    // a Fetch sends [CLIENT CACHING YES, read]; a cache hit/wait sends nothing and releases the reservation
    val conn = reserved(2)
    if (conn == null) trace.fetching(command, callback)(Failure(NotConnected()))
    else conn.cachedSubmit(command, ttlMillis, callback, trace)
  }

  // Enqueues a whole pipeline onto one captured generation. A reconnect cannot split the batch across connections.
  // Return false when disconnected. The caller handles the unsent batch by rerouting or failing it as appropriate.
  // Blocking commands are rejected before this method is called.
  def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Boolean = {
    val conn = reserved(commands.length)
    if (conn == null) false
    else {
      conn.submitAll(commands, callbacks)
      true
    }
  }

  def close(): Unit = {
    pool.close()
    // `aborting`: a reconnect attempt's in-flight connection, closed so its socket is released before close() returns.
    val (draining, aborting) = locked {
      state match {
        case State.Closed | State.Draining => (null, null)
        case State.Reconnecting            =>
          state = State.Closed
          (null, establishing)
        case State.Live                    =>
          state = State.Draining
          (current, null)
      }
    }
    if (aborting != null) aborting.close()
    if (draining != null) {
      try draining.beginDrain().await(config.closeTimeout.toMillis, TimeUnit.MILLISECONDS): Unit
      catch { case _: InterruptedException => Thread.currentThread().interrupt() }
      draining.close()
    }
  }

  private[internal] def currentState: State = state

  private[internal] def isLive: Boolean = state == State.Live

  /**
    * Connects to the node and runs bootstrap synchronously. Like a standalone client, this method throws without retrying when the first
    * handshake fails, and closes the connection and its pool. The caller moves this blocking connection attempt off its thread and treats a
    * failure as an unreachable node.
    */
  def start(): MultiplexedConnection = {
    // the first connect propagates a handshake failure; only reconnects retry
    onThrow(install(establish()))(_ => close())
    this
  }

  // Go live unless close ran during establishment or the connection died in setup; a dead one is replaced by a reconnect. Emit while holding
  // the lock to keep the event ordered with the state transition. If the socket drops immediately afterward, Disconnected is enqueued after
  // Connected because the same lock serializes both events.
  private def install(conn: Conn): Unit = {
    val live = locked {
      if (state == State.Reconnecting && !conn.isDead) {
        goLive(conn)
        true
      } else false
    }
    if (!live) {
      conn.close()
      locked(if (state == State.Reconnecting) reconnect())
    }
  }

  private def establish(): Conn = {
    val conn = new Conn
    locked {
      establishing = conn
      if (state == State.Closed) conn.close()
    }
    try {
      // A half-open peer can accept the socket without answering HELLO, so the handshake limits each wait and lets the reconnect loop continue.
      conn.handshake(bootstrap, config.connectTimeout.toMillis)
      // a server that denies tracking (an ACL restriction, a proxy) still connects and serves cached reads uncached (ADR-0045)
      conn.cache = Option
        .when(role.caches && config.clientCache.enabled)(config.clientCache.maxBytes)
        .filter(_ => conn.step(Connection.clientTrackingOnOptin, config.connectTimeout.toMillis).isEmpty)
        .map(new ClientCache(_))
      conn
    } finally locked { if (establishing eq conn) establishing = null }
  }

  private[internal] def flushCache(): Unit = {
    val c = locked(current)
    if (c != null) c.flushCache()
  }

  // must hold lock
  private def reconnect(): Unit =
    reconnects.schedule(state == State.Reconnecting, error => events.emit(SageEvent.Connection.ReconnectFailed(node, error)))(install(establish()))

  // ignore connections that fail before becoming `current`; the establishment caller handles those failures
  private def onConnTerminated(conn: Conn): Unit =
    // Emit Disconnected only when the current Live connection ends. Holding the lock orders it after that connection's Connected event, and
    // the pool learns of the loss before any reconnect can go live.
    locked {
      if (conn eq current)
        state match {
          case State.Live                        =>
            state = State.Reconnecting
            pool.onLivenessLost()
            reconnect()
            events.emit(SageEvent.Connection.Disconnected(node))
          case State.Draining                    =>
            state = State.Closed
          case State.Reconnecting | State.Closed => ()
        }
    }

  final private class Conn extends WatchedPipe(factory, scheduler, config.watchdog) {

    // each connection has its own cache. Reconnecting creates a new Conn and discards the previous cached values. None runs cached reads
    // uncached, either because caching is off or because the server refused CLIENT TRACKING.
    @volatile var cache: Option[ClientCache]         = None
    @volatile private var drainLatch: CountDownLatch = null

    // Concatenate the pipeline into one Transport.Item. The writer processes an item atomically, which keeps the pipeline in one socket write
    // and prevents other sends from being inserted between its commands.
    def submitAll(commands: Vector[Command[?]], callbacks: Vector[Try[Any] => Unit]): Unit =
      sendAll(Vector.tabulate(commands.length)(i => new Entry(commands(i), callbacks(i))))

    // writes `prefix`, whose reply is discarded, and `command` as one item so no other command is written between them
    def submitAfter[A](prefix: Command[Unit], command: Command[A], callback: Try[A] => Unit): Unit =
      sendAll(Vector(new Entry(prefix, _ => ()), new Entry(command, callback)))

    // OPTIN tracking applies CLIENT CACHING YES only to the next command. Submit it together with the cached read to keep them adjacent. An
    // identity decoder passes the raw reply Frame to the cache, and each waiter then uses its own command decoder.
    def cachedSubmit[A](command: Command[A], ttlMillis: Long, callback: Try[A] => Unit, trace: Events.Fetching): Unit =
      cache match {
        // tracking off: run uncached, releasing one of the two slots reserved for the (now unsent) caching prefix
        case None        =>
          release(1)
          write(command, trace.fetching(command, callback))
        case Some(cache) =>
          val commandBytes            = command.encode
          val keys                    = command.keys
          val reply                   = new MultiplexedConnection.CachedReply(command, callback)
          @tailrec def lookup(): Unit = cache.acquire(commandBytes, keys, scheduler.nowMillis, reply) match {
            // A flush since the lookup (MOVED, a dropped connection, FLUSHALL) retires the hit. Looking it up again fetches from the server,
            // which redirects the read, or fails it with ConnectionLost when the connection is gone.
            case hit: ClientCache.Acquire.Hit if !cache.isCurrent(hit) => lookup()
            // Hit returns a local value. Wait joins an in-flight fetch. Neither case sends another server request, so report a cache hit and
            // release the two slots reserved for the unsent fetch.
            case ClientCache.Acquire.Hit(frame, _)                     =>
              release(2)
              if (events.emitsEvents) events.emit(SageEvent.Cache.Hit(command.name))
              reply.deliver(frame)
            case ClientCache.Acquire.Wait                              =>
              release(2)
              if (events.emitsEvents) events.emit(SageEvent.Cache.Hit(command.name))
            case ClientCache.Acquire.Fetch(fetching)                   =>
              if (events.emitsEvents) events.emit(SageEvent.Cache.Miss(command.name))
              reply.callback = trace.fetching(command, reply.callback)
              val onReply: Try[Frame] => Unit = {
                case Success(frame) => cache.store(fetching, frame, scheduler.nowMillis, ttlMillis)
                case Failure(error) => cache.fail(fetching, error)
              }
              submitAfter(Connection.clientCachingYes, command.rawFrame, onReply)
          }
          trace.lookingUp()
          lookup()
      }

    def flushCache(): Unit = cache.foreach(_.flush())

    def beginDrain(): CountDownLatch = {
      unwatch()
      val latch = new CountDownLatch(1)
      drainLatch = latch
      if (isIdle) latch.countDown()
      latch
    }

    override protected def onDrained(): Unit = {
      val latch = drainLatch
      if (latch != null) latch.countDown()
    }

    protected def onPush(elements: Vector[Frame]): Unit =
      Invalidation.decode(elements) match {
        case Some(Invalidation.Evict(keys)) => cache.foreach(c => keys.foreach(c.invalidate))
        case Some(Invalidation.FlushAll)    => cache.foreach(_.flush())
        case None                           => ()
      }

    override protected def onClosed(): Unit = {
      // a dropped connection loses all further invalidations, so its cache can no longer be trusted for a hit
      cache.foreach(_.flush())
      super.onClosed()
      if (drainLatch != null) drainLatch.countDown()
      onConnTerminated(this)
    }
  }
}

private[client] object MultiplexedConnection {

  type TransportFactory = (Frame => Unit, () => Unit) => Transport

  // Completes a cached read from the raw reply frame. A read sent to the server switches to its tracked callback before the send, which
  // orders the switch before any reply.
  final private class CachedReply[A](command: Command[A], var callback: Try[A] => Unit) extends (Try[Frame] => Unit) {
    def deliver(frame: Frame): Unit = callback(Reply.decode(command, frame))

    def apply(result: Try[Frame]): Unit =
      result match {
        case Success(frame) => deliver(frame)
        case Failure(error) => callback(Failure(error))
      }
  }

  enum State {
    case Live, Reconnecting, Draining, Closed
  }

  // Only master connections cache, because cached reads run on the master. Cluster replicas send READONLY during setup to serve reads for
  // their master's slots.
  enum NodeRole(val setup: Vector[Command[?]], val caches: Boolean) {
    case Master         extends NodeRole(Vector.empty, caches = true)
    case Replica        extends NodeRole(Vector.empty, caches = false)
    case ClusterReplica extends NodeRole(Vector(Connection.readonly), caches = false)
  }
}

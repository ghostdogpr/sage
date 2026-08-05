package sage.client.internal

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.{Bytes, SageEvent, SageException}
import sage.SageException.{ConnectionFailed, ConnectionLost, NotConnected}
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.cluster.Node
import sage.commands.{Command, Connection, Pubsub, Reply}
import sage.protocol.Frame

/**
  * A connection dedicated to pub/sub push frames. It supports channels, glob patterns, and shard channels (`SSUBSCRIBE`). Each subscription
  * has a bounded buffer. When a buffer fills, the reader waits and TCP applies backpressure to the publisher. Other subscriptions on this
  * connection also wait, but command connections are unaffected. The watchdog does not close the connection while its reader is waiting
  * on this backpressure.
  *
  * In standalone and master-replica mode, the connection owns its subscribers and restores them after reconnecting. `onReconnect` lets a
  * master-replica client discover the current master before each attempt. In cluster mode, [[ClusterSubscriptions]] owns the subscribers
  * and assigns them to nodes. When a cluster connection closes, the manager assigns its subscribers again using the latest topology.
  */
final private[client] class SubscriptionConnection(
  factory: MultiplexedConnection.TransportFactory,
  bootstrap: Vector[Command[?]],
  scheduler: Scheduler,
  backoff: BackoffConfig,
  watchdog: WatchdogConfig,
  connectTimeoutMillis: Long,
  bufferSize: Int,
  isLive: () => Boolean,
  cluster: Boolean = false,
  onTerminated: () => Unit = () => (),
  onReconnect: () => Unit = () => (),
  events: Events = Events.disabled,
  node: Option[Node] = None
) extends Placement.ShardConn {
  import SubscriptionConnection.*

  private val lock          = new ReentrantLock()
  private val established   = lock.newCondition()
  private val confirmed     = lock.newCondition()
  private var state: State  = State.Idle
  private var current: Conn = null
  // connections still being opened; a set, since a reconnect and a fresh attach can be establishing at once
  private val establishing  = mutable.Set.empty[Conn]
  private val sinksByKind   = Array.fill(Kind.values.length)(mutable.HashMap.empty[String, mutable.LinkedHashSet[Sink]])

  // The server confirms each subscribed name with one push frame in send order. Standalone subscriptions return after confirmation. Cluster
  // attachment waits up to the connection timeout and may return before confirmation, which can arrive later. These counters are guarded by `lock`.
  private var subscribeSent: Long      = 0L
  private var subscribeConfirmed: Long = 0L
  // Set this generation's resubscribe acknowledgement count in goLive before waking waiters. Later subscriptions do not change what an
  // existing waiter expects.
  private var liveTarget: Long         = -1L

  private var watchdogHandle: Scheduler.Cancelable   = null
  @volatile private var readerBlocked: Boolean       = false
  @volatile private var lastReplyAtMillis: Long      = scheduler.nowMillis
  @volatile private var lastBackpressureMillis: Long = 0L
  @volatile private var pingSentAtMillis: Long       = 0L

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  private def sinksFor(kind: Kind): mutable.HashMap[String, mutable.LinkedHashSet[Sink]] = sinksByKind(kind.ordinal)

  // --- standalone conveniences: the connection owns the sink -------------------------------------------------------------------------------

  def subscribeChannels(channels: Vector[String]): RawSubscription = ownedSubscription(channels, Kind.Channel)

  def subscribePatterns(patterns: Vector[String]): RawSubscription = ownedSubscription(patterns, Kind.Pattern)

  def subscribeShard(channels: Vector[String]): RawSubscription = ownedSubscription(channels, Kind.Shard)

  private def ownedSubscription(names: Vector[String], kind: Kind): RawSubscription = {
    val sink = new Sink(names, kind, bufferSize)
    // closeOwned removes a sink that attachInternal registered before awaitActive failed, preventing it from being restored after reconnecting
    try attachInternal(sink, names, kind, failIfUnconfirmed = true)
    catch {
      case e: Throwable =>
        closeOwned(sink)
        throw e
    }
    new RawSubscription(sink, () => closeOwned(sink))
  }

  // --- manager-driven attach/detach: the caller owns the sink (cluster) --------------------------------------------------------------------

  /**
    * Registers `sink` under `names` and subscribes names that are not already active. It waits up to the connection timeout for confirmation;
    * if the timeout expires, the method returns and the connection can confirm later. For shard subscriptions, the caller must pass
    * names from one slot so a single `SSUBSCRIBE` does not cross slots.
    */
  def attach(sink: Sink, names: Vector[String], kind: Kind): Unit = attachInternal(sink, names, kind, failIfUnconfirmed = false)

  private def attachInternal(sink: Sink, names: Vector[String], kind: Kind, failIfUnconfirmed: Boolean): Unit = {
    var doEstablish   = false
    // the acknowledgement count this attachment waits for; -1 means goLive will send the subscription and set the target
    var confirmTarget = -1L
    lock.lock()
    try {
      var settled = false
      while (!settled)
        state match {
          case State.Closed       => throw NotConnected()
          case State.Establishing => established.await()
          case State.Live         =>
            val fresh = register(sink, names, kind)
            // when every name is already subscribed, wait only for acknowledgements already confirmed and ignore other pending subscriptions
            if (fresh.nonEmpty) {
              sendSubscribe(current, kind, fresh)
              confirmTarget = subscribeSent
            } else confirmTarget = subscribeConfirmed
            settled = true
          case State.Reconnecting =>
            register(sink, names, kind) // the next successful reconnect resubscribes everything currently registered
            settled = true
          case State.Idle         =>
            if (!isLive()) throw NotConnected()
            register(sink, names, kind)
            state = State.Establishing
            doEstablish = true
            settled = true
        }
    } finally lock.unlock()

    if (doEstablish)
      try goLive(establish())
      catch {
        case e: Throwable =>
          // If establishment fails after registering the sink, remove it while the connection remains in the Establishing state; a concurrent
          // close or goLive call may have already changed the state and completed the cleanup
          locked(if (state == State.Establishing) {
            deregister(sink, names, kind)
            state = State.Idle
            established.signalAll()
          })
          throw e
      }
    awaitActive(failIfUnconfirmed, confirmTarget)
  }

  /**
    * Deregisters `sink` from `names` and unsubscribes the names left with no subscriber. Returns true when the connection now holds no sinks
    * at all, so the manager can evict and close it.
    */
  def detach(sink: Sink, names: Vector[String], kind: Kind): Boolean =
    locked {
      val emptied = deregister(sink, names, kind)
      // best-effort: swallow a failed (or interrupted) unsubscribe write so the caller still learns emptiness and terminates the sink
      if (emptied.nonEmpty && state == State.Live)
        try current.send(kind.unsubscribeWire(emptied))
        catch { case NonFatal(_) | _: InterruptedException => () }
      isEmptyUnlocked
    }

  def isEmpty: Boolean = locked(isEmptyUnlocked)

  private def isEmptyUnlocked: Boolean = sinksByKind.forall(_.isEmpty)

  // --- shared establish/dispatch machinery -------------------------------------------------------------------------------------------------

  // Mark a new socket Live and subscribe it to every registered name. Reset confirmation counters for the new connection. In cluster mode,
  // each connection has shard channels for at most one slot, which keeps its SSUBSCRIBE within that slot.
  private def goLive(conn: Conn): Unit = {
    var reconnect          = false
    var notify             = false
    // conn.close waits for the reader, and onConnClosed needs lock. Close conn after releasing lock.
    var teardown: Conn     = null
    var failure: Throwable = null
    locked {
      if (state != State.Establishing && state != State.Reconnecting) teardown = conn
      else if (conn.isTerminated) {
        if (cluster) {
          stopWatchdog()
          current = null
          state = State.Closed
          notify = true
        } else {
          state = State.Reconnecting
          reconnect = true
        }
        established.signalAll()
        confirmed.signalAll()
      } else {
        current = conn
        subscribeSent = 0L
        subscribeConfirmed = 0L
        pingSentAtMillis = 0L
        lastReplyAtMillis = scheduler.nowMillis
        val pending = Kind.values.map(kind => kind -> sinksFor(kind).keys.toVector)
        try
          pending.foreach { case (kind, names) =>
            if (names.nonEmpty) sendSubscribe(conn, kind, names)
          }
        catch {
          // If writing the subscriptions fails, clear current and close this connection before reporting the failure; this prevents an older
          // connection from dispatching after its replacement becomes active
          case e: Throwable =>
            current = null
            teardown = conn
            failure = e
        }
        if (failure == null)
          if (pending.forall(_._2.isEmpty)) {
            // if all subscribers close during establishment, close the new connection and return to Idle
            teardown = conn
            current = null
            state = State.Idle
          } else {
            state = State.Live
            startWatchdog()
            liveTarget = subscribeSent
          }
        established.signalAll()
        confirmed.signalAll()
      }
    }
    if (teardown != null) teardown.close()
    if (reconnect) scheduleReconnect(0)
    if (notify) onTerminated()
    if (failure != null) throw failure
  }

  private def sendSubscribe(conn: Conn, kind: Kind, names: Vector[String]): Unit = {
    conn.send(kind.subscribeWire(names))
    subscribeSent += names.size
  }

  // Wait up to the connection timeout for subscribeConfirmed to reach the target. A target of -1 is replaced with liveTarget after the
  // connection becomes live, covering subscriptions sent by goLive after reconnecting. Owned subscriptions fail with NotConnected when
  // confirmation does not arrive before the deadline.
  private def awaitActive(failIfUnconfirmed: Boolean, target0: Long): Unit = {
    var active = false
    lock.lock()
    try {
      val deadline = scheduler.nowMillis + connectTimeoutMillis
      var target   = target0
      var settled  = false
      while (!settled)
        state match {
          case State.Closed => settled = true
          case State.Live   =>
            if (target < 0) target = liveTarget
            if (subscribeConfirmed >= target) {
              active = true
              settled = true
            } else if (awaitOrTimeout(deadline)) settled = true
          case _            =>
            target = -1L // reconnecting resets the counters; use the next liveTarget when the connection becomes live
            if (awaitOrTimeout(deadline)) settled = true
        }
    } finally lock.unlock()
    if (failIfUnconfirmed && !active) throw NotConnected()
  }

  // true (stop) on timeout; must hold `lock`
  private def awaitOrTimeout(deadline: Long): Boolean = {
    val remaining = deadline - scheduler.nowMillis
    if (remaining <= 0) true
    else {
      confirmed.await(remaining, TimeUnit.MILLISECONDS)
      false
    }
  }

  private def establish(): Conn = {
    val conn = new Conn
    locked {
      establishing += conn
      if (state == State.Closed) conn.close()
    }
    try {
      try conn.start()
      catch {
        case e: SageException => throw e
        case NonFatal(e)      =>
          val failed = ConnectionFailed(s"could not open the subscription connection: $e")
          failed.initCause(e)
          throw failed
      }
      runBootstrap(conn)
      conn
    } finally locked(establishing -= conn): Unit
  }

  // keep bootstrap completion on its connection because two connections may bootstrap concurrently; clear it afterward to ignore later PONG replies
  private def runBootstrap(conn: Conn): Unit =
    try
      Bootstrap.run(
        bootstrap,
        connectTimeoutMillis,
        (command, cb) => {
          conn.armBootstrap(result => cb(result.flatMap(frame => Reply.decode(command, frame))))
          if (conn.isTerminated) { conn.completeBootstrap(Failure(ConnectionLost(mayHaveExecuted = false))): Unit }
          else conn.send(command.encode)
        },
        () => conn.close()
      )
    finally conn.clearBootstrap()

  private def scheduleReconnect(attempt: Int): Unit =
    scheduler.after(Backoff.jitteredMillis(backoff, attempt, scheduler).millis)(attemptReconnect(attempt))

  private def attemptReconnect(attempt: Int): Unit = {
    val proceed = locked(state == State.Reconnecting)
    if (proceed) {
      onReconnect()
      try goLive(establish())
      catch {
        case NonFatal(error) =>
          locked(if (state == State.Reconnecting) {
            events.emit(SageEvent.Connection.ReconnectFailed(node, error))
            scheduleReconnect(attempt + 1)
          })
      }
    }
  }

  private def onConnClosed(conn: Conn): Unit =
    if (cluster) {
      // Cluster connections do not reconnect themselves. The manager uses the latest topology to reassign their subscribers. During slot
      // migration, the server sends `sunsubscribe` and disconnects, making closure the reliable signal to do this.
      val notify = locked {
        if (conn ne current) false
        else
          state match {
            case State.Live | State.Establishing =>
              stopWatchdog()
              current = null
              state = State.Closed
              established.signalAll()
              confirmed.signalAll()
              true
            case _                               => false
          }
      }
      if (notify) onTerminated()
    } else {
      val reconnect = locked {
        if (conn ne current) false
        else
          state match {
            case State.Live | State.Reconnecting =>
              state = State.Reconnecting
              confirmed.signalAll()
              true
            case _                               => false
          }
      }
      if (reconnect) scheduleReconnect(0)
    }

  private def onFrame(conn: Conn, frame: Frame): Unit =
    frame match {
      case Frame.Push(elements) =>
        // a push confirms only that reads are working. Leave lastReplyAtMillis unchanged so push-only traffic still receives idle PING checks.
        Pubsub.decode(elements) match {
          case Some(Pubsub.Event.Message(channel, payload))            =>
            dispatch(sinksFor(Kind.Channel), channel, Delivery.Channel(channel, payload))
          case Some(Pubsub.Event.ShardMessage(channel, payload))       =>
            dispatch(sinksFor(Kind.Shard), channel, Delivery.Channel(channel, payload))
          case Some(Pubsub.Event.PatternMessage(pattern, ch, payload)) =>
            dispatch(sinksFor(Kind.Pattern), pattern, Delivery.Pattern(pattern, ch, payload))
          case Some(_: Pubsub.Event.Subscribed)                        =>
            // conn eq current: a late ack from a superseded generation must not advance this generation's count
            locked(if (conn eq current) {
              subscribeConfirmed += 1
              confirmed.signalAll()
            })
          case _                                                       => () // an Unsubscribed ack is informational; re-homing is disconnect-driven
        }
      case reply                => // non-push reply: bootstrap HELLO, watchdog PONG, or an unexpected error
        lastReplyAtMillis = scheduler.nowMillis
        if (!conn.completeBootstrap(Success(reply)))
          reply match {
            // an error such as MOVED is not a PONG. Close the connection so subscription placement is recalculated.
            case _: Frame.SimpleError | _: Frame.BulkError => scheduler.after(Duration.Zero)(conn.close()) // off the reader thread: close() joins it
            case _                                         => pingSentAtMillis = 0L
          }
    }

  // snapshot the sinks under the lock, then deliver outside it: a blocking put (backpressure) must never hold the registry lock
  private def dispatch(map: mutable.HashMap[String, mutable.LinkedHashSet[Sink]], key: String, delivery: Delivery): Unit = {
    val targets = locked(map.get(key).map(_.toVector).getOrElse(Vector.empty))
    if (targets.nonEmpty) {
      readerBlocked = true
      try {
        var blocked = false
        targets.foreach(sink => if (sink.offer(delivery)) blocked = true)
        if (blocked) lastBackpressureMillis = scheduler.nowMillis
      } finally readerBlocked = false
    }
  }

  // in standalone mode, close the sink after unsubscribing it. Close the socket when the last sink is removed.
  private def closeOwned(sink: Sink): Unit = {
    var teardown: Conn     = null
    var failure: Throwable = null
    locked {
      val emptied = deregister(sink, sink.names, sink.kind)
      try if (emptied.nonEmpty && state == State.Live) current.send(sink.kind.unsubscribeWire(emptied))
      catch { case e: Throwable => failure = e }
      if (isEmptyUnlocked && (state == State.Live || state == State.Reconnecting)) {
        stopWatchdog()
        teardown = current
        current = null
        state = State.Idle
      }
    }
    sink.terminate()
    if (teardown != null) teardown.close()
    if (failure != null) throw failure
  }

  // must hold lock. Change the state to Closed and return the current and establishing connections for the caller to close.
  private def markClosed(): Vector[Conn] = {
    val conns = (Option(current) ++ establishing).toVector
    establishing.clear()
    state = State.Closed
    stopWatchdog()
    current = null
    established.signalAll()
    confirmed.signalAll()
    conns
  }

  // check for subscribers and set Closed under one lock, preventing attach from registering a subscriber between those operations
  def closeIfEmpty(): Boolean = {
    var toClose: Vector[Conn] = Vector.empty
    val closing               = locked {
      if (!isEmptyUnlocked) false
      else {
        toClose = markClosed()
        true
      }
    }
    toClose.foreach(_.close())
    closing
  }

  // close the socket and watchdog but keep subscribers available for reassignment.
  def shutdown(): Unit = tearDown(terminateSinks = false)

  def close(): Unit = tearDown(terminateSinks = true)

  private def tearDown(terminateSinks: Boolean): Unit = {
    var sinks: Set[Sink] = Set.empty
    val toClose          = locked {
      if (terminateSinks) sinks = sinksByKind.iterator.flatMap(_.values.flatten).toSet
      val conns = markClosed()
      sinksByKind.foreach(_.clear())
      conns
    }
    // Terminate sinks before closing connections. Connection close waits for the reader, and the reader may be waiting in Sink.offer until
    // its sink is closed. Closing the connection first would deadlock. In cluster mode, the manager has already terminated the sinks.
    sinks.foreach(_.terminate())
    toClose.foreach(_.close())
  }

  private def register(sink: Sink, names: Vector[String], kind: Kind): Vector[String] = {
    val map   = sinksFor(kind)
    val fresh = Vector.newBuilder[String]
    names.foreach { name =>
      val set = map.getOrElseUpdate(name, mutable.LinkedHashSet.empty)
      if (set.isEmpty) fresh += name
      set += sink
    }
    fresh.result()
  }

  private def deregister(sink: Sink, names: Vector[String], kind: Kind): Vector[String] = {
    val map     = sinksFor(kind)
    val emptied = Vector.newBuilder[String]
    names.foreach { name =>
      map.get(name).foreach { set =>
        set -= sink
        if (set.isEmpty) {
          map -= name
          emptied += name
        }
      }
    }
    emptied.result()
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
    if (readerBlocked) return // deliberate backpressure on a slow consumer; the connection is alive, not stuck
    val conn = locked(if (state == State.Live) current else null)
    if (conn != null) {
      val now = scheduler.nowMillis
      if (pingSentAtMillis != 0L) {
        // Recent backpressure may have kept the reader from reaching the queued PONG. When the sink has room, an unanswered PING still closes
        // the connection after the timeout.
        val backpressured = now - lastBackpressureMillis < watchdog.pingTimeout.toMillis
        if (!backpressured && now - pingSentAtMillis >= watchdog.pingTimeout.toMillis) scheduler.after(Duration.Zero)(conn.close())
      } else if (now - lastReplyAtMillis >= watchdog.pingInterval.toMillis) {
        pingSentAtMillis = now
        conn.send(Connection.ping(None).encode)
      }
    }
  }

  final private class Conn {

    private val transportRef         = new AtomicReference[Transport]()
    @volatile private var terminated = false
    @volatile private var aborted    = false
    private val bootstrapWaiter      = new AtomicReference[Try[Frame] => Unit]()

    def isTerminated: Boolean = terminated

    def start(): Unit = {
      val transport = factory(frame => onFrame(this, frame), () => onTerminated())
      transportRef.set(transport)
      if (aborted) transport.close()
      else transport.start()
    }

    private def onTerminated(): Unit = {
      terminated = true
      completeBootstrap(Failure(ConnectionLost(mayHaveExecuted = false)))
      onConnClosed(this)
    }

    def armBootstrap(waiter: Try[Frame] => Unit): Unit = bootstrapWaiter.set(waiter)
    def clearBootstrap(): Unit                         = bootstrapWaiter.set(null)

    def completeBootstrap(result: Try[Frame]): Boolean = {
      val waiter = bootstrapWaiter.getAndSet(null)
      if (waiter != null) {
        waiter(result)
        true
      } else false
    }

    def send(payload: Bytes): Unit = {
      val transport = transportRef.get()
      if (transport != null) transport.send(new RawItem(payload))
    }

    def close(): Unit = {
      aborted = true
      val transport = transportRef.get()
      if (transport != null) transport.close()
    }
  }
}

private[client] object SubscriptionConnection {

  private enum State {
    case Idle, Establishing, Live, Reconnecting, Closed
  }

  /**
    * The three subscription kinds, each with its wire encoders: classic channels (`SUBSCRIBE`), glob patterns (`PSUBSCRIBE`), and shard
    * channels (`SSUBSCRIBE`).
    */
  private[internal] enum Kind {
    case Channel, Pattern, Shard

    def subscribeWire(names: Vector[String]): Bytes =
      this match {
        case Channel => Pubsub.subscribe(names)
        case Pattern => Pubsub.psubscribe(names)
        case Shard   => Pubsub.ssubscribe(names)
      }

    def unsubscribeWire(names: Vector[String]): Bytes =
      this match {
        case Channel => Pubsub.unsubscribe(names)
        case Pattern => Pubsub.punsubscribe(names)
        case Shard   => Pubsub.sunsubscribe(names)
      }
  }

  /**
    * A raw delivery sent to a subscription buffer. Shard channel messages use [[Channel]] because they contain the same channel and payload.
    */
  enum Delivery {
    case Channel(channel: String, payload: Bytes)
    case Pattern(pattern: String, channel: String, payload: Bytes)
  }

  // pub/sub writes (SUBSCRIBE/UNSUBSCRIBE) are confirmed by push frames, not a per-write reply, so the write hooks are no-ops
  final private class RawItem(val payload: Bytes) extends Transport.Item {
    def writeAttempted(): Unit = ()
    def dropped(): Unit        = ()
  }

  /**
    * Buffers deliveries for one subscription. `next` registers a one-shot callback and returns control to the effect runtime until a delivery
    * arrives. `offer` completes a waiting callback directly or adds the delivery to the bounded buffer. When the buffer is full and no
    * callback is waiting, `offer` blocks the reader and applies TCP backpressure. A subscription supports one sequential consumer and at most
    * one waiting callback.
    */
  final private[internal] class Sink(val names: Vector[String], val kind: Kind, capacity: Int) {

    private val cap                              = math.max(1, capacity)
    private val lock                             = new ReentrantLock()
    private val notFull                          = lock.newCondition()
    private val backlog                          = new java.util.ArrayDeque[Delivery](cap)
    private var waiter: Option[Delivery] => Unit = null
    private var closed                           = false

    def next(callback: Option[Delivery] => Unit): Unit = {
      var ready: Option[Delivery] = null // null means the callback was stored; a non-null value is delivered immediately
      lock.lock()
      try {
        // an existing callback means another consumer called next concurrently; keep that callback registered and reject this call
        if (waiter != null) throw new IllegalStateException("a subscription is single-consumer; concurrent next is not supported")
        val head = backlog.poll()
        if (head != null) {
          notFull.signal()
          ready = Some(head)
        } else if (closed) ready = None
        else waiter = callback
      } finally lock.unlock()
      if (ready != null) callback(ready)
    }

    def cancelNext(callback: Option[Delivery] => Unit): Unit = {
      lock.lock()
      try if (waiter eq callback) waiter = null
      finally lock.unlock()
    }

    def offer(delivery: Delivery): Boolean = {
      var hungry: Option[Delivery] => Unit = null
      var blocked                          = false
      lock.lock()
      try {
        var settled = false
        while (!settled)
          if (closed) settled = true
          else if (waiter != null) {
            hungry = waiter
            waiter = null
            settled = true
          } else if (backlog.size < cap) {
            backlog.add(delivery)
            settled = true
          } else {
            // Wait until the consumer makes room when the backlog is full.
            blocked = true
            notFull.await()
          }
      } finally lock.unlock()
      if (hungry != null) hungry(Some(delivery))
      blocked
    }

    def terminate(): Unit = {
      var pending: Option[Delivery] => Unit = null
      lock.lock()
      try {
        closed = true
        backlog.clear()
        pending = waiter
        waiter = null
        notFull.signalAll() // release a reader blocked on backpressure
      } finally lock.unlock()
      if (pending != null) pending(None)
    }
  }

  final class RawSubscription private[internal] (sink: Sink, onClose: () => Unit) {

    def next(callback: Option[Delivery] => Unit): Unit = sink.next(callback)

    def cancelNext(callback: Option[Delivery] => Unit): Unit = sink.cancelNext(callback)

    def close(): Unit = onClose()
  }
}

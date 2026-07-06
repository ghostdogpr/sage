package sage.client.internal

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.control.NonFatal

import sage.Bytes
import sage.SageException.NotConnected
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.commands.{Command, Connection, Pubsub, Reply}
import sage.protocol.Frame

/**
  * A Subscription Connection: a connection whose product is push frames, dispatched to per-subscription buffers; the only non-push replies
  * are the HELLO bootstrap and the watchdog's PONG. It carries three kinds of subscription — channels, glob patterns, and Shard Channels
  * (`SSUBSCRIBE`) — and a slow consumer blocks the reader, so TCP backpressures the publisher (lossless), stalling peer subscriptions on this
  * connection but never commands; the watchdog therefore skips its liveness kill while the reader is so blocked.
  *
  * Two lifecycles share this one machinery. Standalone (`cluster = false`): the connection owns its sinks, runs its own reconnect loop, and
  * re-issues every active subscription on reconnect; each attempt first calls `onReconnect`, which a master-replica subscription uses to
  * re-discover roles and re-home onto a promoted master. Cluster (`cluster = true`): the [[ClusterSubscriptions]] manager owns the sinks and
  * attaches them per Node; a socket loss is terminal for the connection — it fires `onTerminated` and the manager re-homes the surviving
  * sinks onto the current owner rather than blindly reconnecting to a node that may no longer own the slot.
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
  onReconnect: () => Unit = () => ()
) extends Placement.ShardConn {
  import SubscriptionConnection.*

  private val lock          = new ReentrantLock()
  private val established   = lock.newCondition()
  private val confirmed     = lock.newCondition()
  private var state: State  = State.Idle
  private var current: Conn = null
  private val channelSinks  = mutable.HashMap.empty[String, mutable.LinkedHashSet[Sink]]
  private val patternSinks  = mutable.HashMap.empty[String, mutable.LinkedHashSet[Sink]]
  private val shardSinks    = mutable.HashMap.empty[String, mutable.LinkedHashSet[Sink]]

  // SUBSCRIBE-ack accounting (guarded by `lock`): the server confirms each subscribed name with one push frame in send order, so a subscribe
  // waits until `subscribeConfirmed` catches `subscribeSent` before returning — otherwise `subscribe` then `publish` races across the sockets.
  private var subscribeSent: Long      = 0L
  private var subscribeConfirmed: Long = 0L
  // this generation's resubscribe ack count, set in goLive before waiters wake so a re-arming waiter never adopts a later subscribe's count
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

  private def sinksFor(kind: Kind): mutable.HashMap[String, mutable.LinkedHashSet[Sink]] =
    kind match {
      case Kind.Channel => channelSinks
      case Kind.Pattern => patternSinks
      case Kind.Shard   => shardSinks
    }

  // --- standalone conveniences: the connection owns the sink -------------------------------------------------------------------------------

  def subscribeChannels(channels: Vector[String]): RawSubscription = ownedSubscription(channels, Kind.Channel)

  def subscribePatterns(patterns: Vector[String]): RawSubscription = ownedSubscription(patterns, Kind.Pattern)

  def subscribeShard(channels: Vector[String]): RawSubscription = ownedSubscription(channels, Kind.Shard)

  private def ownedSubscription(names: Vector[String], kind: Kind): RawSubscription = {
    val sink = new Sink(names, kind, bufferSize)
    // closeOwned, not bare terminate: if attach registered the sink but awaitActive then threw, fully unwind it so no phantom channel is
    // resubscribed on the next reconnect
    try attachInternal(sink, names, kind, failIfUnconfirmed = true)
    catch { case e: Throwable => closeOwned(sink); throw e }
    new RawSubscription(sink, () => closeOwned(sink))
  }

  // --- manager-driven attach/detach: the caller owns the sink (cluster) --------------------------------------------------------------------

  /**
    * Registers `sink` under `names` of the given kind and subscribes the names not already subscribed, blocking (bounded by the connect
    * timeout) until the server confirms. The caller must pass names that hash to a single Slot for the Shard kind in cluster mode, so the
    * one `SSUBSCRIBE` this emits never spans slots (`CROSSSLOT`).
    */
  def attach(sink: Sink, names: Vector[String], kind: Kind): Unit = attachInternal(sink, names, kind, failIfUnconfirmed = false)

  private def attachInternal(sink: Sink, names: Vector[String], kind: Kind, failIfUnconfirmed: Boolean): Unit = {
    var doEstablish   = false
    // the ack count this attach waits for, captured under its send lock; -1 means it sends later (in goLive) and adopts liveTarget
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
            // nothing sent (names already subscribed): target the confirmed count, not subscribeSent, so we don't wait on an unrelated pending ack
            if (fresh.nonEmpty) { sendSubscribe(current, kind, fresh); confirmTarget = subscribeSent }
            else confirmTarget = subscribeConfirmed
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
          // drop the phantom sink, but only if a concurrent close/goLive hasn't already moved us off Establishing
          locked(if (state == State.Establishing) { deregister(sink, names, kind); state = State.Idle; established.signalAll() })
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
      if (emptied.nonEmpty && state == State.Live) current.send(kind.unsubscribeWire(emptied))
      isEmptyUnlocked
    }

  def isEmpty: Boolean = locked(isEmptyUnlocked)

  private def isEmptyUnlocked: Boolean = channelSinks.isEmpty && patternSinks.isEmpty && shardSinks.isEmpty

  // --- shared establish/dispatch machinery -------------------------------------------------------------------------------------------------

  // bring a freshly-established socket Live, resubscribing everything registered (counters reset to this generation). In cluster mode it runs
  // once per connection with at most one Slot's worth of Shard channels registered, so the single SSUBSCRIBE never spans slots (CROSSSLOT).
  private def goLive(conn: Conn): Unit = {
    var reconnect          = false
    var notify             = false
    // conn.close() joins the reader whose onConnClosed needs `lock`, so tear down after releasing it (as closeOwned/close do)
    var teardown: Conn     = null
    var failure: Throwable = null
    locked {
      if (state != State.Establishing && state != State.Reconnecting) teardown = conn
      else if (conn.isTerminated) {
        if (cluster) { stopWatchdog(); current = null; state = State.Closed; notify = true }
        else { state = State.Reconnecting; reconnect = true }
        established.signalAll()
        confirmed.signalAll()
      } else {
        current = conn
        subscribeSent = 0L
        subscribeConfirmed = 0L
        pingSentAtMillis = 0L
        lastReplyAtMillis = scheduler.nowMillis
        val channels = channelSinks.keys.toVector
        val patterns = patternSinks.keys.toVector
        val shards   = shardSinks.keys.toVector
        try {
          if (channels.nonEmpty) sendSubscribe(conn, Kind.Channel, channels)
          if (patterns.nonEmpty) sendSubscribe(conn, Kind.Pattern, patterns)
          if (shards.nonEmpty) sendSubscribe(conn, Kind.Shard, shards)
        } catch {
          // resubscribe write failed: drop this socket and clear current so a superseded generation can't keep dispatching, then rethrow
          case e: Throwable => current = null; teardown = conn; failure = e
        }
        if (failure == null)
          if (channels.isEmpty && patterns.isEmpty && shards.isEmpty) {
            // everything closed during the establish; tear back down rather than hold an idle socket open
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

  // Waits (bounded by the connect timeout) for subscribeConfirmed to reach `target`; -1 (re)arms to liveTarget, e.g. after a reconnect. When
  // `failIfUnconfirmed` (owned standalone/master-replica), an unconfirmed deadline throws NotConnected instead of returning an inactive stream.
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
            if (subscribeConfirmed >= target) { active = true; settled = true }
            else if (awaitOrTimeout(deadline)) settled = true
          case _            =>
            target = -1L // a reconnect reset the counters: re-arm to liveTarget
            if (awaitOrTimeout(deadline)) settled = true
        }
    } finally lock.unlock()
    if (failIfUnconfirmed && !active) throw NotConnected()
  }

  // true (stop) on timeout; must hold `lock`
  private def awaitOrTimeout(deadline: Long): Boolean = {
    val remaining = deadline - scheduler.nowMillis
    if (remaining <= 0) true
    else { val _ = confirmed.await(remaining, TimeUnit.MILLISECONDS); false }
  }

  private def establish(): Conn = {
    val conn = new Conn
    conn.start()
    runBootstrap(conn)
    conn
  }

  // per-conn waiter (two establishes can bootstrap concurrently and must not cross-complete); the finally clears it so a later PONG isn't misrouted
  private def runBootstrap(conn: Conn): Unit =
    try
      Bootstrap.run(
        bootstrap,
        connectTimeoutMillis,
        (command, cb) => {
          conn.bootstrapWaiter = frame => cb(Reply.decode(command, frame))
          conn.send(command.encode)
        },
        () => conn.close()
      )
    finally conn.bootstrapWaiter = null

  private def scheduleReconnect(attempt: Int): Unit =
    scheduler.after(Backoff.jitteredMillis(backoff, attempt, scheduler).millis)(attemptReconnect(attempt))

  private def attemptReconnect(attempt: Int): Unit = {
    val proceed = locked(state == State.Reconnecting)
    if (proceed) {
      onReconnect()
      try goLive(establish())
      catch { case NonFatal(_) => locked(if (state == State.Reconnecting) scheduleReconnect(attempt + 1)) }
    }
  }

  private def onConnClosed(conn: Conn): Unit =
    if (cluster) {
      // a cluster connection never self-reconnects: a socket loss is terminal, and the manager re-homes the surviving sinks onto the current
      // owner (the server pushes `sunsubscribe` then disconnects on a slot migration, so this drop is the reliable signal)
      val notify = locked {
        if (conn ne current) false
        else
          state match {
            case State.Live | State.Establishing =>
              stopWatchdog(); current = null; state = State.Closed; established.signalAll(); confirmed.signalAll(); true
            case _                               => false
          }
      }
      if (notify) onTerminated()
    } else {
      val reconnect = locked {
        if (conn ne current) false
        else
          state match {
            case State.Live | State.Reconnecting => state = State.Reconnecting; confirmed.signalAll(); true
            case _                               => false
          }
      }
      if (reconnect) scheduleReconnect(0)
    }

  private def onFrame(conn: Conn, frame: Frame): Unit =
    frame match {
      case Frame.Push(elements) =>
        // not touching lastReplyAtMillis: a push proves only the read path, so push-only traffic must still get the idle keepalive PING
        Pubsub.decode(elements) match {
          case Some(Pubsub.Event.Message(channel, payload))            => dispatch(channelSinks, channel, Delivery.Channel(channel, payload))
          case Some(Pubsub.Event.ShardMessage(channel, payload))       => dispatch(shardSinks, channel, Delivery.Channel(channel, payload))
          case Some(Pubsub.Event.PatternMessage(pattern, ch, payload)) => dispatch(patternSinks, pattern, Delivery.Pattern(pattern, ch, payload))
          case Some(_: Pubsub.Event.Subscribed)                        =>
            // conn eq current: a late ack from a superseded generation must not advance this generation's count
            locked(if (conn eq current) { subscribeConfirmed += 1; confirmed.signalAll() })
          case _                                                       => () // an Unsubscribed ack is informational; re-homing is disconnect-driven
        }
      case reply                => // non-push reply: bootstrap HELLO, watchdog PONG, or an unexpected error
        lastReplyAtMillis = scheduler.nowMillis
        val waiter = conn.bootstrapWaiter
        if (waiter != null) waiter(reply)
        else
          reply match {
            // an error (e.g. MOVED on SSUBSCRIBE) isn't a PONG: drop the connection so re-home/reconnect re-plans
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

  // standalone: the connection owns the sink, so it both unsubscribes and terminates it; tears the socket down when its last sink closes
  private def closeOwned(sink: Sink): Unit = {
    var teardown: Conn = null
    locked {
      val emptied = deregister(sink, sink.names, sink.kind)
      if (emptied.nonEmpty && state == State.Live) current.send(sink.kind.unsubscribeWire(emptied))
      if (isEmptyUnlocked && (state == State.Live || state == State.Reconnecting)) {
        stopWatchdog()
        teardown = current
        current = null
        state = State.Idle
      }
    }
    sink.terminate()
    if (teardown != null) teardown.close()
  }

  // atomic empty-check + Closed transition, so a racing attach can't bind a sink to a connection about to close
  def closeIfEmpty(): Boolean = {
    var conn: Conn = null
    val closing    = locked {
      if (!isEmptyUnlocked) false
      else {
        conn = current
        state = State.Closed
        stopWatchdog()
        current = null
        established.signalAll()
        confirmed.signalAll()
        true
      }
    }
    if (conn != null) conn.close()
    closing
  }

  // close socket/watchdog but keep the sinks (unlike close), so a re-home re-attaches them
  def shutdown(): Unit = {
    var conn: Conn = null
    locked {
      conn = current
      state = State.Closed
      stopWatchdog()
      current = null
      channelSinks.clear()
      patternSinks.clear()
      shardSinks.clear()
      established.signalAll()
      confirmed.signalAll()
    }
    if (conn != null) conn.close()
  }

  def close(): Unit = {
    var conn: Conn       = null
    var sinks: Set[Sink] = Set.empty
    locked {
      sinks = (channelSinks.values.flatten ++ patternSinks.values.flatten ++ shardSinks.values.flatten).toSet
      conn = current
      state = State.Closed
      stopWatchdog()
      current = null
      channelSinks.clear()
      patternSinks.clear()
      shardSinks.clear()
      established.signalAll()
      confirmed.signalAll()
    }
    // terminate before close: conn.close() joins the reader, which only exits Sink.offer once its sink is closed — closing first deadlocks.
    // In cluster mode the manager owns the sinks and terminates them before calling close(), so this set is already empty.
    sinks.foreach(_.terminate())
    if (conn != null) conn.close()
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
        if (set.isEmpty) { map -= name; emptied += name }
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
        // recent backpressure means the reader hasn't reached the queued PONG yet; a sink with room never blocks, so an unanswered PING there still kills
        val backpressured = now - lastBackpressureMillis < watchdog.pingTimeout.toMillis
        if (!backpressured && now - pingSentAtMillis >= watchdog.pingTimeout.toMillis) scheduler.after(Duration.Zero)(conn.close())
      } else if (now - lastReplyAtMillis >= watchdog.pingInterval.toMillis) {
        pingSentAtMillis = now
        conn.send(Connection.ping(None).encode)
      }
    }
  }

  final private class Conn {

    private val transportRef                     = new AtomicReference[Transport]()
    @volatile private var terminated             = false
    @volatile var bootstrapWaiter: Frame => Unit = null

    def isTerminated: Boolean = terminated

    def start(): Unit = {
      val transport = factory(frame => onFrame(this, frame), () => { terminated = true; onConnClosed(this) })
      transportRef.set(transport)
      transport.start()
    }

    def send(payload: Bytes): Unit = {
      val transport = transportRef.get()
      if (transport != null) transport.send(new RawItem(payload))
    }

    def close(): Unit = {
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
    * The three subscription kinds, each with its wire encoders: classic channels (`SUBSCRIBE`), glob patterns (`PSUBSCRIBE`), and Shard
    * Channels (`SSUBSCRIBE`).
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
    * A raw delivery routed to a subscription's buffer. A Shard Channel delivery reuses [[Channel]] — it carries the same channel and payload.
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
    * One subscription's async mailbox. `next` registers a one-shot callback (never blocks the consumer, so a fiber runtime parks rather than
    * pinning a worker); `offer` hands a waiting consumer its delivery directly, else buffers, else blocks the reader for TCP backpressure
    * once the bounded backlog is full and no consumer is pending. A single consumer pulls sequentially, so at most one waiter exists.
    */
  final private[internal] class Sink(val names: Vector[String], val kind: Kind, capacity: Int) {

    private val cap                              = math.max(1, capacity)
    private val lock                             = new ReentrantLock()
    private val notFull                          = lock.newCondition()
    private val backlog                          = new java.util.ArrayDeque[Delivery](cap)
    private var waiter: Option[Delivery] => Unit = null
    private var closed                           = false

    def next(callback: Option[Delivery] => Unit): Unit = {
      var ready: Option[Delivery] = null // null = parked on the waiter; non-null = deliver synchronously
      lock.lock()
      try {
        // an existing waiter means a concurrent consumer; fail loud rather than silently evict it
        if (waiter != null) throw new IllegalStateException("a subscription is single-consumer; concurrent next is not supported")
        val head = backlog.poll()
        if (head != null) { notFull.signal(); ready = Some(head) }
        else if (closed) ready = None
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
          else if (waiter != null) { hungry = waiter; waiter = null; settled = true }
          else if (backlog.size < cap) { backlog.add(delivery); settled = true }
          else { blocked = true; notFull.await() } // backlog full, no consumer: backpressure the reader
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

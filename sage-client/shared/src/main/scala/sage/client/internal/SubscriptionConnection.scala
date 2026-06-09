package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.util.control.NonFatal

import sage.Bytes
import sage.SageException.{ConnectionLost, NotConnected}
import sage.client.{BackoffConfig, WatchdogConfig}
import sage.commands.{Command, Connection, Pubsub, Reply}
import sage.protocol.Frame

/**
  * The Subscription Connection: one lazily-created connection per client, re-issuing every active subscription on reconnect. Unlike the
  * Multiplexed Connection, push frames are the product (dispatched to per-subscription buffers) and the only non-push replies are the HELLO
  * bootstrap and the watchdog's PONG. A slow consumer blocks the reader, so TCP backpressures the publisher — lossless, but it stalls peer
  * subscriptions on this connection (never commands); the watchdog therefore skips its liveness kill while the reader is so blocked.
  */
final private[client] class SubscriptionConnection(
  factory: MultiplexedConnection.TransportFactory,
  bootstrap: Vector[Command[?]],
  scheduler: Scheduler,
  backoff: BackoffConfig,
  watchdog: WatchdogConfig,
  connectTimeoutMillis: Long,
  bufferSize: Int,
  isLive: () => Boolean
) {
  import SubscriptionConnection.*

  private val lock          = new ReentrantLock()
  private val established   = lock.newCondition()
  private val confirmed     = lock.newCondition()
  private var state: State  = State.Idle
  private var current: Conn = null
  private val channelSinks  = mutable.HashMap.empty[String, mutable.LinkedHashSet[Sink]]
  private val patternSinks  = mutable.HashMap.empty[String, mutable.LinkedHashSet[Sink]]

  // SUBSCRIBE-ack accounting (guarded by `lock`): the server confirms each subscribed name with one push frame in send order, so a subscribe
  // waits until `subscribeConfirmed` catches `subscribeSent` before returning — otherwise `subscribe` then `publish` races across the sockets.
  private var subscribeSent: Long      = 0L
  private var subscribeConfirmed: Long = 0L

  private var watchdogHandle: Scheduler.Cancelable     = null
  @volatile private var readerBlocked: Boolean         = false
  @volatile private var lastReplyAtMillis: Long        = scheduler.nowMillis
  @volatile private var pingSentAtMillis: Long         = 0L
  @volatile private var bootstrapWaiter: Frame => Unit = null

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  def subscribeChannels(channels: Vector[String]): RawSubscription = add(channels, isPattern = false)

  def subscribePatterns(patterns: Vector[String]): RawSubscription = add(patterns, isPattern = true)

  private def add(names: Vector[String], isPattern: Boolean): RawSubscription = {
    val sink        = new Sink(names, isPattern, bufferSize)
    var doEstablish = false
    lock.lock()
    try {
      var settled = false
      while (!settled)
        state match {
          case State.Closed       => throw NotConnected()
          case State.Establishing => established.await()
          case State.Live         =>
            val fresh = register(sink, names, isPattern)
            if (fresh.nonEmpty) sendSubscribe(current, isPattern, fresh)
            settled = true
          case State.Reconnecting =>
            register(sink, names, isPattern) // the next successful reconnect resubscribes everything currently registered
            settled = true
          case State.Idle         =>
            if (!isLive()) throw NotConnected()
            register(sink, names, isPattern)
            state = State.Establishing
            doEstablish = true
            settled = true
        }
    } finally lock.unlock()

    if (doEstablish)
      try {
        val conn = establish() // create + start + HELLO, blocking, no lock held
        goLive(conn)
      } catch {
        case e: Throwable =>
          // establish failed after the sink was registered; drop it so a later reconnect doesn't resubscribe a phantom channel
          locked { deregister(sink); state = State.Idle; established.signalAll() }
          sink.terminate()
          throw e
      }
    try awaitActive()
    catch { case e: Throwable => closeSubscription(sink); throw e }
    new RawSubscription(this, sink)
  }

  // bring a freshly-established socket Live under the lock, resubscribing everything registered (counters reset to this generation)
  private def goLive(conn: Conn): Unit = locked {
    if (state == State.Establishing || state == State.Reconnecting) {
      current = conn
      subscribeSent = 0L
      subscribeConfirmed = 0L
      val channels = channelSinks.keys.toVector
      val patterns = patternSinks.keys.toVector
      if (channels.nonEmpty) { conn.send(Pubsub.subscribe(channels)); subscribeSent += channels.size }
      if (patterns.nonEmpty) { conn.send(Pubsub.psubscribe(patterns)); subscribeSent += patterns.size }
      if (channels.isEmpty && patterns.isEmpty) {
        // everything closed during the establish; tear back down rather than hold an idle socket open
        conn.close()
        current = null
        state = State.Idle
      } else {
        state = State.Live
        startWatchdog()
      }
      established.signalAll()
      confirmed.signalAll()
    } else conn.close()
  }

  private def sendSubscribe(conn: Conn, isPattern: Boolean, names: Vector[String]): Unit = {
    conn.send(wire(isPattern, subscribe = true, names))
    subscribeSent += names.size
  }

  // Best-effort, bounded by the connect timeout: a down or backpressured connection degrades to fire-and-forget rather than blocking forever.
  // The target is (re)captured whenever the connection is Live, so a reconnect mid-wait re-arms against the new generation's resubscribe.
  private def awaitActive(): Unit = {
    lock.lock()
    try {
      val deadline = scheduler.nowMillis + connectTimeoutMillis
      var target   = -1L
      var settled  = false
      while (!settled)
        state match {
          case State.Closed => settled = true
          case State.Live   =>
            if (target < 0) target = subscribeSent
            if (subscribeConfirmed >= target) settled = true
            else settled = awaitOrTimeout(deadline)
          case _            =>
            target = -1L // re-arm against the next live generation
            settled = awaitOrTimeout(deadline)
        }
    } finally lock.unlock()
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

  private def runBootstrap(conn: Conn): Unit =
    bootstrap.foreach { command =>
      val latch   = new CountDownLatch(1)
      val box     = new AtomicReference[Frame]()
      bootstrapWaiter = frame => { box.set(frame); latch.countDown() }
      conn.send(command.encode)
      val replied = latch.await(connectTimeoutMillis, TimeUnit.MILLISECONDS)
      bootstrapWaiter = null
      if (!replied) {
        conn.close()
        throw ConnectionLost(mayHaveExecuted = false)
      }
      Reply.run(command.asInstanceOf[Command[Any]], box.get()) match {
        case Left(error) => conn.close(); throw error
        case Right(_)    => ()
      }
    }

  private def scheduleReconnect(attempt: Int): Unit =
    scheduler.after(Backoff.jitteredMillis(backoff, attempt, scheduler).millis)(attemptReconnect(attempt))

  private def attemptReconnect(attempt: Int): Unit = {
    val proceed = locked(state == State.Reconnecting)
    if (proceed)
      try goLive(establish())
      catch { case NonFatal(_) => locked(if (state == State.Reconnecting) scheduleReconnect(attempt + 1)) }
  }

  private def onConnClosed(conn: Conn): Unit = {
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

  private def onFrame(frame: Frame): Unit = {
    lastReplyAtMillis = scheduler.nowMillis
    frame match {
      case Frame.Push(elements) =>
        Pubsub.decode(elements) match {
          case Some(Pubsub.Event.Message(channel, payload))            => dispatch(channelSinks, channel, Delivery.Channel(channel, payload))
          case Some(Pubsub.Event.PatternMessage(pattern, ch, payload)) => dispatch(patternSinks, pattern, Delivery.Pattern(pattern, ch, payload))
          case Some(_: Pubsub.Event.Subscribed)                        => locked { subscribeConfirmed += 1; confirmed.signalAll() }
          case _                                                       => ()
        }
      case reply                => // a non-push reply is the bootstrap HELLO or the watchdog's PONG
        val waiter = bootstrapWaiter
        if (waiter != null) waiter(reply) else pingSentAtMillis = 0L
    }
  }

  // snapshot the sinks under the lock, then deliver outside it: a blocking put (backpressure) must never hold the registry lock
  private def dispatch(map: mutable.HashMap[String, mutable.LinkedHashSet[Sink]], key: String, delivery: Delivery): Unit = {
    val targets = locked(map.get(key).map(_.toVector).getOrElse(Vector.empty))
    if (targets.nonEmpty) {
      readerBlocked = true
      try targets.foreach(_.offer(delivery))
      finally readerBlocked = false
    }
  }

  private[internal] def closeSubscription(sink: Sink): Unit = {
    var teardown: Conn = null
    locked {
      val emptied = deregister(sink)
      if (emptied.nonEmpty && state == State.Live) current.send(wire(sink.isPattern, subscribe = false, emptied))
      if (channelSinks.isEmpty && patternSinks.isEmpty && (state == State.Live || state == State.Reconnecting)) {
        stopWatchdog()
        teardown = current
        current = null
        state = State.Idle
      }
    }
    sink.terminate()
    if (teardown != null) teardown.close()
  }

  def close(): Unit = {
    var conn: Conn       = null
    var sinks: Set[Sink] = Set.empty
    locked {
      sinks = (channelSinks.values.flatten ++ patternSinks.values.flatten).toSet
      conn = current
      state = State.Closed
      stopWatchdog()
      current = null
      channelSinks.clear()
      patternSinks.clear()
      established.signalAll()
      confirmed.signalAll()
    }
    // terminate before close: conn.close() joins the reader, which only exits Sink.offer once its sink is closed — closing first deadlocks
    sinks.foreach(_.terminate())
    if (conn != null) conn.close()
  }

  private def register(sink: Sink, names: Vector[String], isPattern: Boolean): Vector[String] = {
    val map   = if (isPattern) patternSinks else channelSinks
    val fresh = Vector.newBuilder[String]
    names.foreach { name =>
      val set = map.getOrElseUpdate(name, mutable.LinkedHashSet.empty)
      if (set.isEmpty) fresh += name
      set += sink
    }
    fresh.result()
  }

  private def deregister(sink: Sink): Vector[String] = {
    val map     = if (sink.isPattern) patternSinks else channelSinks
    val emptied = Vector.newBuilder[String]
    sink.names.foreach { name =>
      map.get(name).foreach { set =>
        set -= sink
        if (set.isEmpty) { map -= name; emptied += name }
      }
    }
    emptied.result()
  }

  private def wire(isPattern: Boolean, subscribe: Boolean, names: Vector[String]): Bytes =
    (isPattern, subscribe) match {
      case (false, true)  => Pubsub.subscribe(names)
      case (false, false) => Pubsub.unsubscribe(names)
      case (true, true)   => Pubsub.psubscribe(names)
      case (true, false)  => Pubsub.punsubscribe(names)
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
        if (now - pingSentAtMillis >= watchdog.pingTimeout.toMillis) scheduler.after(Duration.Zero)(conn.close())
      } else if (now - lastReplyAtMillis >= watchdog.pingInterval.toMillis) {
        pingSentAtMillis = now
        conn.send(Connection.ping(None).encode)
      }
    }
  }

  final private class Conn {

    private val transportRef = new AtomicReference[Transport]()

    def start(): Unit = {
      val transport = factory(onFrame, () => onConnClosed(this))
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
    * A raw delivery routed to a subscription's buffer.
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
  final private[internal] class Sink(val names: Vector[String], val isPattern: Boolean, capacity: Int) {

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
        val head = backlog.poll()
        if (head != null) { notFull.signal(); ready = Some(head) }
        else if (closed) ready = None
        else waiter = callback
      } finally lock.unlock()
      if (ready != null) callback(ready)
    }

    def offer(delivery: Delivery): Unit = {
      var hungry: Option[Delivery] => Unit = null
      lock.lock()
      try {
        var settled = false
        while (!settled)
          if (closed) settled = true
          else if (waiter != null) { hungry = waiter; waiter = null; settled = true }
          else if (backlog.size < cap) { backlog.add(delivery); settled = true }
          else notFull.await() // backlog full, no consumer: backpressure the reader
      } finally lock.unlock()
      if (hungry != null) hungry(Some(delivery))
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

  final class RawSubscription private[SubscriptionConnection] (owner: SubscriptionConnection, sink: Sink) {

    def next(callback: Option[Delivery] => Unit): Unit = sink.next(callback)

    def close(): Unit = owner.closeSubscription(sink)
  }
}

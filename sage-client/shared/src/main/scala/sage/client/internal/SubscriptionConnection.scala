package sage.client.internal

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.SageEvent
import sage.SageException.{NotConnected, ServerError}
import sage.client.SageConfig
import sage.cluster.Node
import sage.commands.Pubsub
import sage.protocol.Frame

/**
  * A connection dedicated to pub/sub push frames. It supports channels, glob patterns, and shard channels (`SSUBSCRIBE`). Each subscription
  * has a bounded buffer. When a buffer fills, the reader waits and TCP applies backpressure to the publisher. Other subscriptions on this
  * connection also wait, but command connections are unaffected. The watchdog does not close the connection while its reader is waiting
  * on this backpressure.
  *
  * With [[OnLoss.Reconnect]] (standalone, master-replica, and cluster classic subscriptions), the connection owns its subscribers and restores
  * them after reconnecting; `beforeAttempt` lets the client discover the current master before each attempt. For shard channels in a
  * cluster, [[ClusterSubscriptions]] owns the subscribers and assigns them to nodes. When such a connection closes, the manager assigns its
  * subscribers again using the latest topology.
  */
final private[client] class SubscriptionConnection(
  factory: MultiplexedConnection.TransportFactory,
  scheduler: Scheduler,
  config: SageConfig,
  isLive: () => Boolean,
  onLoss: SubscriptionConnection.OnLoss = SubscriptionConnection.OnLoss.Reconnect(() => (), Events.disabled)
) extends ClusterSubscriptions.ShardConn
  with SubscriptionConnection.PubSub {
  import SubscriptionConnection.*

  private enum State {
    case Idle, Establishing, Reconnecting, Closed
    case Live(conn: Conn)
  }

  private val lock         = new ReentrantLock()
  private val changed      = lock.newCondition()
  private var state: State = State.Idle
  // connections still being opened; a set, since a reconnect and a fresh attach can be establishing at once
  private val establishing = mutable.Set.empty[Conn]
  private val sinksByKind  = Array.fill(Kind.values.length)(mutable.HashMap.empty[String, Name])
  private val reconnects   = new Reconnects(scheduler, config.reconnect, lock)

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  private def sinksFor(kind: Kind): mutable.HashMap[String, Name] = sinksByKind(kind.ordinal)

  // must hold lock. A refused, dropped or emptied name is replaced by a new Name when subscribed again.
  private def registered(name: Name): Boolean = sinksFor(name.kind).get(name.name).exists(_ eq name)

  // --- standalone conveniences: the connection owns the sink -------------------------------------------------------------------------------

  def subscribeChannels(channels: Vector[String]): RawSubscription = owned(channels, Kind.Channel, failIfUnconfirmed = true)

  def subscribePatterns(patterns: Vector[String]): RawSubscription = owned(patterns, Kind.Pattern, failIfUnconfirmed = true)

  def subscribeShard(channels: Vector[String]): RawSubscription = owned(channels, Kind.Shard, failIfUnconfirmed = true)

  // cluster classic subscriptions pass failIfUnconfirmed = false, since confirmation in a cluster is best-effort
  def owned(names: Vector[String], kind: Kind, failIfUnconfirmed: Boolean): RawSubscription = {
    val sink = new Sink(names, kind, config.pubsub.bufferSize)
    // closeOwned removes a sink that attachInternal registered before awaitActive failed, preventing it from being restored after reconnecting
    onThrow { attachInternal(sink, names, failIfUnconfirmed); sink.failure.foreach(throw _) }(_ => closeOwned(sink))
    new RawSubscription(sink, () => closeOwned(sink))
  }

  // --- manager-driven attach/detach: the caller owns the sink (cluster) --------------------------------------------------------------------

  /**
    * Registers `sink` under `names` and subscribes names that are not already active. It waits up to the connection timeout for confirmation;
    * if the timeout expires, the method returns and the connection can confirm later. A name the server rejects leaves the registry.
    */
  def attach(sink: Sink, names: Vector[String]): Unit = attachInternal(sink, names, failIfUnconfirmed = false)

  private def attachInternal(sink: Sink, names: Vector[String], failIfUnconfirmed: Boolean): Unit = {
    var doEstablish = false
    var waitFor     = Vector.empty[Name]
    lock.lock()
    try {
      var settled = false
      while (!settled)
        state match {
          case State.Closed       => throw NotConnected()
          case State.Establishing => changed.await()
          case State.Live(conn)   =>
            val (all, created) = register(sink, names)
            conn.subscribe(created)
            waitFor = all
            settled = true
          case State.Reconnecting =>
            waitFor = register(sink, names)._1 // the next successful reconnect resubscribes everything currently registered
            settled = true
          case State.Idle         =>
            if (!isLive()) throw NotConnected()
            waitFor = register(sink, names)._1
            state = State.Establishing
            doEstablish = true
            settled = true
        }
    } finally lock.unlock()

    if (doEstablish)
      onThrow(goLive(establish())) { _ =>
        // If establishment fails after registering the sink, remove it while the connection remains in the Establishing state; a concurrent
        // close or goLive call may have already changed the state and completed the cleanup
        locked(if (state == State.Establishing) {
          deregister(sink, names)
          state = State.Idle
          changed.signalAll()
        })
      }
    awaitActive(waitFor, failIfUnconfirmed)
  }

  /**
    * Deregisters `sink` from `names` and unsubscribes the names left with no subscriber.
    */
  def detach(sink: Sink, names: Vector[String]): Unit =
    locked {
      val emptied = deregister(sink, names)
      liveConn.foreach(_.unsubscribe(sink.kind, emptied))
    }

  def namesOf(sink: Sink): Vector[String] =
    locked(sink.names.distinct.filter(name => sinksFor(sink.kind).get(name).exists(_.sinks.contains(sink))))

  def isEmpty: Boolean = locked(isEmptyUnlocked)

  private def isEmptyUnlocked: Boolean = sinksByKind.forall(_.isEmpty)

  // must hold lock
  private def liveConn: Option[Conn] =
    state match {
      case State.Live(conn) => Some(conn)
      case _                => None
    }

  // --- shared establish/dispatch machinery -------------------------------------------------------------------------------------------------

  // Mark a new socket Live and subscribe it to every registered name.
  private def goLive(conn: Conn): Unit =
    // conn.close waits for the reader, and onConnClosed needs lock. Close conn after releasing lock.
    locked(state match {
      case State.Establishing | State.Reconnecting =>
        if (conn.isDead) lost()
        else {
          val pending = sinksByKind.toVector.flatMap(_.values)
          conn.subscribe(pending)
          changed.signalAll()
          if (pending.isEmpty) {
            // if all subscribers close during establishment, close the new connection and return to Idle
            state = State.Idle
            () => conn.close()
          } else {
            state = State.Live(conn)
            reconnects.live()
            conn.watch()
            () => ()
          }
        }
      case _                                       => () => conn.close()
    })()

  // Runs once per subscribed name with its confirmation, its error reply, or ConnectionLost when the connection ends first. A refusal (NOPERM,
  // ERR) ends the name's subscriptions with the server's error. After any other error, such as BUSY, LOADING or MOVED, a cluster shard
  // channel is placed again, and any other connection closes so that its reconnect subscribes the name again.
  private def confirm(conn: Conn, name: Name)(result: Try[Unit]): Unit =
    locked {
      changed.signalAll()
      result match {
        case Success(_)                                      =>
          name.confirmedOn = Some(conn)
          () => ()
        case Failure(error: ServerError) if registered(name) =>
          onLoss match {
            case _ if refuses(error)          =>
              sinksFor(name.kind) -= name.name
              val ends = name.sinks.toVector.map(_.end(Some(error)))
              () => ends.foreach(_())
            case OnLoss.Report(_, _, onMoved) =>
              sinksFor(name.kind) -= name.name
              onMoved
            case _: OnLoss.Reconnect          => () => scheduler.offload(conn.close())
          }
        case _                                               => () => () // a lost reply is sent again by the next connection
      }
    }()

  // Wait up to the connection timeout until the live connection confirmed every name that is still registered. Owned subscriptions fail
  // with NotConnected when confirmation does not arrive before the deadline.
  private def awaitActive(names: Vector[Name], failIfUnconfirmed: Boolean): Unit = {
    var active = false
    lock.lock()
    try {
      val deadline  = scheduler.nowMillis + config.connectTimeout.toMillis
      // names before `done` are settled on `checkedOn`; a new live connection must confirm them again
      var checkedOn = Option.empty[Conn]
      var done      = 0
      var settled   = false
      while (!settled)
        if (state == State.Closed) settled = true
        else {
          val live = liveConn
          if (live != checkedOn) {
            checkedOn = live
            done = 0
          }
          while (done < names.size && (!registered(names(done)) || live.exists(conn => names(done).confirmedOn.exists(_ eq conn)))) done += 1
          if (done == names.size) {
            active = true
            settled = true
          } else if (awaitOrTimeout(deadline)) settled = true
        }
    } finally lock.unlock()
    if (failIfUnconfirmed && !active) throw NotConnected()
  }

  // true (stop) on timeout; must hold `lock`
  private def awaitOrTimeout(deadline: Long): Boolean = {
    val remaining = deadline - scheduler.nowMillis
    if (remaining <= 0) true
    else {
      changed.await(remaining, TimeUnit.MILLISECONDS)
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
      try conn.handshake(Bootstrap.commands(config), config.connectTimeout.toMillis)
      catch { case NonFatal(e) => throw Client.translateHandshake(e) }
      conn
    } finally locked(establishing -= conn): Unit
  }

  private def onConnClosed(conn: Conn): Unit =
    locked(state match {
      case State.Live(c) if c eq conn => lost()
      case _                          => () => ()
    })()

  // Must hold lock; returns the action to run after releasing it. A cluster shard connection does not reconnect itself: the manager reassigns its
  // subscribers using the latest topology.
  private def lost(): () => Unit = {
    changed.signalAll()
    onLoss match {
      case OnLoss.Report(onTerminated, _, _) =>
        state = State.Closed
        () => onTerminated(this)
      case mode: OnLoss.Reconnect            =>
        state = State.Reconnecting
        reconnects.schedule(
          state == State.Reconnecting,
          error => mode.events.emit(SageEvent.Connection.ReconnectFailed(mode.node(), error)),
          mode.immediately
        ) {
          mode.beforeAttempt()
          goLive(establish())
        }
        () => ()
    }
  }

  // snapshot the sinks under the lock, then deliver outside it: a blocking put (backpressure) must never hold the registry lock
  private def dispatch(conn: Conn, map: mutable.HashMap[String, Name], key: String, delivery: Delivery): Unit = {
    val targets = locked(map.get(key).map(_.sinks.toVector).getOrElse(Vector.empty))
    if (targets.nonEmpty) {
      conn.readerBlocked = true
      try {
        var blocked = false
        targets.foreach(sink => if (sink.offer(delivery)) blocked = true)
        if (blocked) conn.lastBackpressureMillis = scheduler.nowMillis
      } finally conn.readerBlocked = false
    }
  }

  // Close the socket without unsubscribing when the last sink is removed. Terminate the sink first: closing the socket waits for the reader,
  // which may be blocked offering to this sink.
  private def closeOwned(sink: Sink): Unit = {
    sink.terminate()
    locked {
      val emptied = deregister(sink, sink.names)
      if (isEmptyUnlocked && (liveConn.nonEmpty || state == State.Reconnecting)) {
        val teardown = liveConn
        state = State.Idle
        teardown
      } else {
        liveConn.foreach(_.unsubscribe(sink.kind, emptied))
        None
      }
    }.foreach(_.close())
  }

  // must hold lock. Change the state to Closed and return the current and establishing connections for the caller to close.
  private def markClosed(): Vector[Conn] = {
    val conns = (liveConn ++ establishing).toVector
    establishing.clear()
    state = State.Closed
    changed.signalAll()
    conns
  }

  // check for subscribers and set Closed under one lock, preventing attach from registering a subscriber between those operations
  def closeIfEmpty(): Boolean = {
    val toClose = locked(Option.when(isEmptyUnlocked)(markClosed()))
    toClose.foreach(_.foreach(_.close()))
    toClose.nonEmpty
  }

  def close(): Unit = {
    val (sinks, toClose) = locked {
      val sinks = sinksByKind.iterator.flatMap(_.values.flatMap(_.sinks)).toSet
      val conns = markClosed()
      sinksByKind.foreach(_.clear())
      (sinks, conns)
    }
    // Terminate sinks before closing connections. Connection close waits for the reader, and the reader may be waiting in Sink.offer until
    // its sink is closed. Closing the connection first would deadlock. For shard connections, the cluster manager has already terminated the sinks.
    sinks.foreach(_.terminate())
    toClose.foreach(_.close())
  }

  // returns the Names of `names` and the ones this call created, which still need a subscribe
  private def register(sink: Sink, names: Vector[String]): (Vector[Name], Vector[Name]) = {
    val created = Vector.newBuilder[Name]
    val all     = names.map(name => sinksFor(sink.kind).getOrElseUpdate(name, { val n = new Name(sink.kind, name); created += n; n }))
    all.foreach(_.sinks += sink)
    (all, created.result())
  }

  private def deregister(sink: Sink, names: Vector[String]): Vector[String] = {
    val map     = sinksFor(sink.kind)
    val emptied = Vector.newBuilder[String]
    names.foreach { name =>
      map.get(name).foreach { n =>
        n.sinks -= sink
        if (n.sinks.isEmpty) {
          map -= name
          emptied += name
        }
      }
    }
    emptied.result()
  }

  // One registered name and its sinks, guarded by `lock`. `confirmedOn` is the last connection that confirmed it, so after a reconnect the
  // name counts as active only once the new connection confirms it.
  final private class Name(val kind: Kind, val name: String) {
    val sinks                     = mutable.LinkedHashSet.empty[Sink]
    var confirmedOn: Option[Conn] = None
  }

  // Replies (bootstrap, subscribed and unsubscribed names, and watchdog PING) match their entries in write order; a reply with nothing pending
  // closes the connection.
  final private class Conn extends WatchedPipe(factory, scheduler, config.watchdog) {

    // Each name is its own entry, answered in write order by its confirmation push or by an error reply such as NOPERM or MOVED.
    def subscribe(names: Vector[Name]): Unit = sendEach(names.map(name => new Entry(name.kind.subscribe(name.name), confirm(this, name))))

    // An UNSUBSCRIBE or PUNSUBSCRIBE is answered by its own push. The push of an SUNSUBSCRIBE is ambiguous: a cluster node sends the same
    // push when it drops a channel whose slot moved, and then answers the SUNSUBSCRIBE with another push (Redis) or MOVED (Valkey). One HELLO
    // written after the SUNSUBSCRIBEs ends their replies: an error before the HELLO's reply is one of theirs, and their pushes go to onPush.
    // Each name has its own SUNSUBSCRIBE because Valkey answers CROSSSLOT to one naming channels in several slots.
    def unsubscribe(kind: Kind, names: Vector[String]): Unit =
      if (kind != Kind.Shard) sendEach(names.map(name => new Entry(kind.unsubscribe(name), _ => ())))
      else if (names.nonEmpty) sendEach(names.map(name => new Entry(kind.unsubscribe(name), untilHello)) :+ new Entry(Pubsub.helloInfo, afterHello))

    // used on the reader thread only, while the HELLO's reply is passed on to its own entry
    private var passing = false
    private var reached = false

    // The first SUNSUBSCRIBE still pending receives the HELLO's reply and passes it on in a loop, not recursively, so a long batch cannot
    // overflow the stack.
    private val untilHello: Try[Frame] => Unit = {
      case Success(reply) if !passing =>
        passing = true
        reached = false
        try while (!reached && !isDead) answer(reply)
        finally passing = false
      case _                          => ()
    }

    // ACL does not refuse an argument-less HELLO, so an error here means that replies no longer match their entries
    private val afterHello: Try[Frame] => Unit = { result =>
      reached = true
      result match {
        case Failure(_: ServerError) => this.close()
        case _                       => ()
      }
    }

    private def sendEach(entries: Vector[Entry[?]]): Unit =
      if (entries.nonEmpty) {
        reserve(entries.size)
        sendAll(entries)
      }

    @volatile var readerBlocked: Boolean       = false
    @volatile var lastBackpressureMillis: Long = 0L

    // Recent backpressure may have kept the reader from reaching the probe's reply. When the sink has room, an unanswered probe still closes
    // the connection after the timeout.
    override protected def tick(): Unit =
      if (!readerBlocked) // deliberate backpressure on a slow consumer; the connection is alive, not stuck
        checkLiveness(lastBackpressureMillis + config.watchdog.pingTimeout.toMillis)

    protected def onPush(elements: Vector[Frame]): Unit =
      Pubsub.decode(elements).foreach {
        case Pubsub.Event.Confirmed                               => answer(Frame.Push(elements))
        case Pubsub.Event.Delivered(kind, subscription, delivery) => dispatch(this, sinksFor(kind), subscription, delivery)
        // The server dropped the channel only if it is still registered and confirmed here. An unsubscribed channel has left the registry.
        case Pubsub.Event.ShardUnsubscribed(channel)              =>
          onLoss match {
            case OnLoss.Report(_, onDropped, _)
                if locked(
                  sinksFor(Kind.Shard).get(channel).exists(_.confirmedOn.exists(_ eq this)) && sinksFor(Kind.Shard).remove(channel).isDefined
                ) =>
              onDropped()
            case _ => ()
          }
      }

    override protected def onClosed(): Unit = {
      super.onClosed()
      onConnClosed(this)
    }
  }
}

private[client] object SubscriptionConnection {

  // What a lost connection does: reconnect and restore its subscribers, or report the loss (cluster shard channels).
  enum OnLoss {
    // `node` names the node of the last attempt in reported failures. With `immediately`, the first attempt after a stable period does not wait.
    case Reconnect(beforeAttempt: () => Unit, events: Events, node: () => Option[Node] = () => None, immediately: Boolean = false)
    // onDropped runs when the server drops a shard channel without closing the connection, as it does when the channel's slot moves, and
    // onMoved when the server refuses a shard channel with a redirect or another retryable error
    case Report(onTerminated: SubscriptionConnection => Unit, onDropped: () => Unit, onMoved: () => Unit)
  }

  export Pubsub.{Delivery, Kind}

  // The errors a subscribe gets every time it is sent: an ACL denial, or a command or argument the server does not support.
  private def refuses(error: ServerError): Boolean = error.code == "NOPERM" || error.code == "ERR"

  // Connects each attempt to the node `pick` names at that time. `retain` closes that connection once its node leaves the deployment, and
  // the connection then reconnects to a current node.
  final class Following(nodeFactory: Node => MultiplexedConnection.TransportFactory, pick: () => Option[Node]) {

    @volatile private var on: Option[(Node, Transport)] = None
    // the node of the latest attempt, recorded before connecting so that a failed attempt reports it
    @volatile var node: Option[Node]                    = None

    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      node = pick()
      node match {
        case Some(target) =>
          val transport = nodeFactory(target)(onFrame, onClosed)
          on = Some(target -> transport)
          transport
        case None         => throw NotConnected()
      }
    }

    def retain(listed: Node => Boolean): Unit = on.foreach { case (node, transport) => if (!listed(node)) transport.close() }
  }

  trait PubSub {
    def subscribeChannels(channels: Vector[String]): RawSubscription
    def subscribePatterns(patterns: Vector[String]): RawSubscription
    def subscribeShard(channels: Vector[String]): RawSubscription
    def close(): Unit
  }

  /**
    * Buffers deliveries for one subscription. `next` registers a one-shot callback and returns control to the effect runtime until a delivery
    * arrives. `offer` completes a waiting callback directly or adds the delivery to the bounded buffer. When the buffer is full and no
    * callback is waiting, `offer` blocks the reader and applies TCP backpressure. A subscription supports one sequential consumer and at most
    * one waiting callback.
    */
  final private[internal] class Sink(val names: Vector[String], val kind: Kind, capacity: Int) {

    private val lock                             = new ReentrantLock()
    private val notFull                          = lock.newCondition()
    private val backlog                          = new java.util.ArrayDeque[Delivery](capacity)
    private var waiter: Option[Delivery] => Unit = null
    // `failure` holds the server's error when it ended the subscription by refusing a name; both are written under `lock`, failure first
    @volatile var failure: Option[Throwable]     = None
    @volatile private var ended                  = false

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
        } else if (ended) ready = None
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
          if (ended) settled = true
          else if (waiter != null) {
            hungry = waiter
            waiter = null
            settled = true
          } else if (backlog.size < capacity) {
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

    def terminate(): Unit = end(None)()

    // Ends the subscription, with the server's error if it refused a name, and returns the call that wakes a waiting consumer, to run after
    // the caller's locks are released. Only the first end counts.
    def end(error: Option[Throwable]): () => Unit = {
      var pending: Option[Delivery] => Unit = null
      lock.lock()
      try {
        if (!ended) {
          failure = error
          ended = true
        }
        backlog.clear()
        pending = waiter
        waiter = null
        notFull.signalAll() // release a reader blocked on backpressure
      } finally lock.unlock()
      () => if (pending != null) pending(None)
    }
  }

  final class RawSubscription private[internal] (sink: Sink, onClose: () => Unit) {

    // None once the subscription has ended
    def next(callback: Option[Delivery] => Unit): Unit = sink.next(callback)

    def cancelNext(callback: Option[Delivery] => Unit): Unit = sink.cancelNext(callback)

    // the server's error when it ended the subscription by refusing a name
    def failure: Option[Throwable] = sink.failure

    def close(): Unit = onClose()
  }
}

package sage.client.internal

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import sage.{Bytes, SageException}
import sage.SageException.{ConnectionLost, ServerError}
import sage.client.WatchdogConfig
import sage.commands.{Command, Connection, Reply}
import sage.protocol.Frame

/**
  * Runs `undo` when `body` throws anything, including an `InterruptedException` from a cancelled wait, then rethrows.
  */
private[internal] inline def onThrow[A](inline body: A)(inline undo: Throwable => Unit): A =
  try body
  catch {
    case error: Throwable =>
      undo(error)
      throw error
  }

/**
  * Defines the interface between connection logic and socket I/O. The connection submits [[Transport.Item]] values and receives parsed frames
  * through the `onFrame` callback supplied when the transport is created. The transport calls `onClosed` once when the connection ends,
  * including after `close()`. Before that callback, it calls `dropped()` for each queued item that was not written.
  */
private[client] trait Transport {

  /**
    * Begins the I/O. Called once, after the owner is ready to receive callbacks.
    */
  def start(): Unit

  /**
    * Adds an item to the write queue and returns immediately without throwing, even on an interrupted thread. The transport calls exactly
    * one of `writeAttempted` or `dropped`. On the write path, it calls `clearPayload` after capturing the bytes to write.
    */
  def send(item: Transport.Item): Unit

  /**
    * Idempotent. Blocks until the I/O threads have terminated and `onClosed` has run. It never throws, even on an interrupted thread, and
    * keeps the caller's interrupt flag set.
    */
  def close(): Unit
}

private[client] object Transport {

  trait Item {

    def payload: Bytes

    /**
      * Invoked after the payload has been captured for writing. The item can release its reference to the payload at this point.
      */
    def clearPayload(): Unit = ()

    /**
      * Invoked on the writer thread immediately before the first write attempt: from here on the command may execute server-side.
      */
    def writeAttempted(): Unit

    /**
      * Invoked when the connection terminated before any write attempt.
      */
    def dropped(): Unit
  }
}

/**
  * Owns one transport from `start` until it terminates and matches replies to commands in write order. `close` may run before `start`,
  * which then closes the transport at once. `isDead` becomes true when the connection is closed, drops a write, terminates, or is retired
  * by its pool. A command counts as in flight from `reserve` until its reply, failure or drop, including while the transport still queues
  * it, so a caller that checks `isQuiescent` or waits for `onDrained` sees queued work.
  */
abstract private[internal] class Pipe(factory: MultiplexedConnection.TransportFactory, scheduler: Scheduler) {

  private val transportRef   = new AtomicReference[Transport]()
  @volatile private var dead = false

  final def isDead: Boolean = dead

  final private[internal] def markDead(): Unit = dead = true

  // Publish transportRef before the blocking connect starts so close can abort it.
  final def start(): Unit = {
    val transport = factory(onFrame, () => { dead = true; onClosed() })
    transportRef.set(transport)
    if (dead) transport.close()
    else transport.start()
  }

  private def send(item: Transport.Item): Unit = transportRef.get().send(item)

  // the caller has reserved the entries
  final protected def sendAll(entries: Vector[Entry[?]]): Unit = send(new Batch(entries))

  def close(): Unit = {
    dead = true
    val transport = transportRef.get()
    if (transport != null) transport.close()
  }

  private val pending  = new ConcurrentLinkedQueue[Entry[?]]()
  private val inFlight = new AtomicInteger(0)

  final def reserve(n: Int): Unit = inFlight.addAndGet(n): Unit

  final protected def release(n: Int): Unit = if (inFlight.addAndGet(-n) == 0) onDrained()

  final protected def isIdle: Boolean = inFlight.get() == 0

  final def isQuiescent: Boolean = !isDead && isIdle

  protected def onDrained(): Unit = ()

  final def submit[A](command: Command[A], callback: Try[A] => Unit): Unit = {
    reserve(1)
    write(command, callback)
  }

  /**
    * Starts the transport and runs the setup commands in turn, blocking up to `timeoutMillis` for each reply. Closes the connection and
    * throws on a timeout or a failed reply, except that a [[Bootstrap.bestEffort]] command's `ServerError` is tolerated.
    */
  final def handshake(commands: Vector[Command[?]], timeoutMillis: Long): Unit = {
    start()
    commands.foreach { command =>
      step(command, timeoutMillis).filterNot(_ => Bootstrap.bestEffort(command)).foreach { error =>
        close()
        throw error
      }
    }
  }

  /**
    * Runs one setup command and returns the server's error reply, or `None` on success. Closes the connection and throws on a timeout or any
    * other failure.
    */
  final def step(command: Command[?], timeoutMillis: Long): Option[ServerError] =
    onThrow {
      Bootstrap.awaitReply[Any](timeoutMillis, ConnectionLost(mayHaveExecuted = false))(submit(command, _)) match {
        case Failure(error: ServerError) => Some(error)
        // a lost setup reply means the caller's own command was never sent
        case Failure(_: ConnectionLost)  => throw ConnectionLost(mayHaveExecuted = false)
        case Failure(error)              => throw error
        case Success(_)                  => None
      }
    }(_ => close())

  // the caller has reserved the command
  final def write[A](command: Command[A], callback: Try[A] => Unit): Unit = send(new Entry(command, callback))

  final protected def oldestWriteMillis: Option[Long] = Option(pending.peek()).map(_.sentAtMillis)

  protected def onFrame(frame: Frame): Unit =
    frame match {
      case _: Frame.Push => ()
      case reply         => answer(reply)
    }

  // completes the oldest pending entry; a reply with nothing pending means the stream desynced
  final protected def answer(reply: Frame): Unit = {
    val waiter = pending.poll()
    if (waiter == null) close()
    else waiter.complete(reply)
  }

  protected def onReadOnly(): Unit = close()

  protected def onClosed(): Unit = {
    var waiter = pending.poll()
    while (waiter != null) {
      waiter.fail(ConnectionLost(mayHaveExecuted = true))
      waiter = pending.poll()
    }
  }

  final protected class Entry[A](command: Command[A], callback: Try[A] => Unit) extends Transport.Item {

    @volatile var sentAtMillis: Long = 0L

    var payload: Bytes = command.encode

    override def clearPayload(): Unit = payload = Bytes.empty

    def writeAttempted(): Unit = {
      sentAtMillis = scheduler.nowMillis
      pending.add(this): Unit
    }

    // the transport calls dropped during teardown before onClosed; mark the connection dead first so a pool cannot reuse it meanwhile
    def dropped(): Unit = {
      markDead()
      settle(Failure(ConnectionLost(mayHaveExecuted = false)))
    }

    // Reply.decode guards against throwing user decoders: an escaped exception would otherwise lose the callback and hang the awaiting fiber
    def complete(frame: Frame): Unit =
      Reply.decode(command, frame) match {
        // A READONLY reply fails its command and retires the connection, because an in-place failover can leave the old master connected
        // but unable to accept writes. Mark it dead before delivering the reply so that a pool discards it on release.
        case failure @ Failure(ServerError("READONLY", _)) =>
          markDead()
          settle(failure)
          onReadOnly()
        case result                                        => settle(result)
      }

    def fail(error: SageException): Unit = settle(Failure(error))

    private def settle(result: Try[A]): Unit = {
      release(1)
      callback(result)
    }
  }

  // Concatenate a pipeline into one transport write and notify each entry individually when the write succeeds or fails; the transport
  // writes or drops the complete batch without splitting it across socket writes
  final private class Batch(entries: Vector[Entry[?]]) extends Transport.Item {

    val payload: Bytes = Bytes.concatBy(entries)(_.payload)

    override def clearPayload(): Unit = entries.foreach(_.clearPayload())

    def writeAttempted(): Unit = entries.foreach(_.writeAttempted())

    def dropped(): Unit = entries.foreach(_.dropped())
  }
}

/**
  * A [[Pipe]] checked by a watchdog. A push frame does not count as a reply, so push-only traffic still receives idle PING checks.
  */
abstract private[internal] class WatchedPipe(factory: MultiplexedConnection.TransportFactory, scheduler: Scheduler, watchdog: WatchdogConfig)
  extends Pipe(factory, scheduler) {

  @volatile private var lastReplyAtMillis: Long = scheduler.nowMillis
  // WatchedPipe.Stopped once unwatched, so a watch that loses the race with close cancels its own timer
  private val ticker                            = new AtomicReference[Scheduler.Cancelable]()

  protected def onPush(elements: Vector[Frame]): Unit

  final override protected def onFrame(frame: Frame): Unit =
    frame match {
      case Frame.Push(elements) => onPush(elements)
      case reply                =>
        lastReplyAtMillis = scheduler.nowMillis
        super.onFrame(reply)
    }

  // Runs tick every PING interval from go-live until unwatch or close.
  final def watch(): Unit =
    if (watchdog.enabled) {
      val handle = scheduler.every(watchdog.pingInterval)(tick())
      if (!ticker.compareAndSet(null, handle)) handle.cancel()
    }

  final def unwatch(): Unit = {
    val handle = ticker.getAndSet(WatchedPipe.Stopped)
    if (handle != null) handle.cancel()
  }

  protected def tick(): Unit = checkLiveness(0L)

  override protected def onClosed(): Unit = {
    unwatch()
    super.onClosed()
  }

  // Close when the oldest unanswered write is older than the timeout and graceUntilMillis has passed; otherwise PING when idle.
  final protected def checkLiveness(graceUntilMillis: Long): Unit = {
    val now = scheduler.nowMillis
    oldestWriteMillis match {
      // offload: close() blocks joining I/O threads, and the watchdog tick runs on the shared timer thread, which must not block
      case Some(sentAtMillis) =>
        if (now >= graceUntilMillis && now - sentAtMillis >= watchdog.pingTimeout.toMillis) scheduler.after(Duration.Zero)(close())
      // any reply to the PING, even an error such as NOPERM or LOADING, proves the connection is alive
      case None               => if (now - lastReplyAtMillis >= watchdog.pingInterval.toMillis) submit(Connection.ping(None), _ => ())
    }
  }
}

private object WatchedPipe {
  val Stopped: Scheduler.Cancelable = () => ()
}

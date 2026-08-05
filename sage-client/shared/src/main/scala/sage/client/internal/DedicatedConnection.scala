package sage.client.internal

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference, AtomicReferenceArray}

import scala.util.{Failure, Success, Try}

import sage.Bytes
import sage.SageException
import sage.SageException.ConnectionLost
import sage.commands.{Command, Reply}
import sage.protocol.Frame

/**
  * A connection borrowed exclusively from the [[DedicatedPool]]. It does not reconnect or run a watchdog. If the connection is lost, the
  * pool discards it and in-flight work fails with `ConnectionLost(mayHaveExecuted = true)`. Replies are matched in order. This allows the
  * synchronous `HELLO` setup to finish before the connection is borrowed and keeps a transaction's `MULTI`, queued-command, and `EXEC`
  * replies aligned with their commands.
  */
final private[client] class DedicatedConnection private (
  factory: MultiplexedConnection.TransportFactory,
  connectTimeoutMillis: Long
) {

  // the generation recorded when this connection joins the pool; see [[DedicatedPool.establishOutsideLock]]
  @volatile private var stampedEpoch: MultiplexedConnection.Generation = MultiplexedConnection.Generation.initial

  private val pending                 = new ConcurrentLinkedQueue[DedicatedConnection.Waiter]()
  private val transportRef            = new AtomicReference[Transport]()
  @volatile private var dead: Boolean = false
  // Count a command when submit accepts it. The asynchronous transport may hold it before writeAttempted adds it to pending. Checking only
  // pending.isEmpty could therefore return a connection to the pool while it still has a queued batch.
  private val inFlight                = new AtomicInteger(0)

  def epoch: MultiplexedConnection.Generation = stampedEpoch

  def stampEpoch(generation: MultiplexedConnection.Generation): Unit = stampedEpoch = generation

  def isHealthy: Boolean = !dead

  def isQuiescent: Boolean = !dead && inFlight.get() == 0

  def submit[A](command: Command[A], callback: Try[A] => Unit): Unit = {
    inFlight.incrementAndGet()
    transportRef.get().send(new Entry(command, callback))
  }

  /**
    * Sends `commands` as a single pipelined write and returns their raw reply frames in order. Used for a transaction's `MULTI` … `EXEC`
    * batch, whose `EXEC` array the caller decodes per-position against the original commands; the frames are returned undecoded.
    */
  def submitRaw(commands: Vector[Command[?]], callback: Try[Vector[Frame]] => Unit): Unit = {
    inFlight.incrementAndGet()
    transportRef.get().send(new RawBatch(commands, callback))
  }

  def close(): Unit = {
    dead = true
    val transport = transportRef.get()
    if (transport != null) transport.close()
  }

  /**
    * Opens the socket and runs the bootstrap synchronously; throws (no retry) if the connect or handshake fails.
    */
  def establish(bootstrap: Vector[Command[?]]): Unit = {
    start()
    runBootstrap(bootstrap)
  }

  private def start(): Unit = {
    val transport = factory(onFrame, onClosed)
    transportRef.set(transport)
    if (dead) transport.close()
    else transport.start()
  }

  private def runBootstrap(bootstrap: Vector[Command[?]]): Unit =
    Bootstrap.run(bootstrap, connectTimeoutMillis, (c, cb) => submit(c, cb), () => close())

  private def onFrame(frame: Frame): Unit =
    frame match {
      case _: Frame.Push => ()
      case reply         =>
        val waiter = pending.poll()
        if (waiter == null) close() // a reply with nothing pending means the stream desynced; discard
        else {
          // close before delivering a READONLY reply, ensuring the pool discards this connection when it is released
          if (Poison.isReadonly(reply)) close()
          waiter.complete(reply)
        }
    }

  private def onClosed(): Unit = {
    dead = true
    var waiter = pending.poll()
    while (waiter != null) {
      waiter.fail(ConnectionLost(mayHaveExecuted = true))
      waiter = pending.poll()
    }
  }

  final private class Entry[A](command: Command[A], callback: Try[A] => Unit) extends Transport.Item with DedicatedConnection.Waiter {

    var payload: Bytes = command.encode

    override def clearPayload(): Unit = payload = Bytes.empty

    def writeAttempted(): Unit =
      pending.add(this): Unit

    // the transport calls dropped during teardown before onClosed. Mark the connection dead first to prevent the pool from reusing it during close.
    def dropped(): Unit = {
      dead = true
      inFlight.decrementAndGet()
      callback(Failure(ConnectionLost(mayHaveExecuted = false)))
    }

    def complete(frame: Frame): Unit = {
      val result = Reply.decode(command, frame)
      inFlight.decrementAndGet()
      callback(result)
    }

    def fail(error: SageException): Unit = {
      inFlight.decrementAndGet()
      callback(Failure(error))
    }
  }

  // Write the batch as one transport item and store each reply at its matching index. Invoke the callback after all replies arrive, or after
  // the first failure.
  final private class RawBatch(commands: Vector[Command[?]], callback: Try[Vector[Frame]] => Unit) extends Transport.Item {

    private val n         = commands.length
    private val frames    = new AtomicReferenceArray[Frame](n)
    private val remaining = new AtomicInteger(n)
    private val done      = new AtomicBoolean(false)

    var payload: Bytes = Bytes.concatBy(commands)(_.encode)

    override def clearPayload(): Unit = payload = Bytes.empty

    def writeAttempted(): Unit = {
      var i = 0
      while (i < n) {
        pending.add(new Slot(i))
        i += 1
      }
    }

    def dropped(): Unit = {
      dead = true
      finish(Failure(ConnectionLost(mayHaveExecuted = false)))
    }

    private def finish(result: Try[Vector[Frame]]): Unit =
      if (done.compareAndSet(false, true)) {
        inFlight.decrementAndGet()
        callback(result)
      }

    final private class Slot(index: Int) extends DedicatedConnection.Waiter {

      def complete(frame: Frame): Unit = {
        frames.set(index, frame)
        if (remaining.decrementAndGet() == 0) finish(Success(Vector.tabulate(n)(frames.get)))
      }

      def fail(error: SageException): Unit = finish(Failure(error))
    }
  }
}

private[client] object DedicatedConnection {

  private trait Waiter {
    def complete(frame: Frame): Unit
    def fail(error: SageException): Unit
  }

  /**
    * Builds an unconnected connection; the pool runs the blocking [[DedicatedConnection.establish]] separately.
    */
  def create(factory: MultiplexedConnection.TransportFactory, connectTimeoutMillis: Long): DedicatedConnection =
    new DedicatedConnection(factory, connectTimeoutMillis)
}

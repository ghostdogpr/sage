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
  * A single connection held exclusively by one borrower at a time, borrowed from the [[DedicatedPool]]. Unlike the Multiplexed
  * Connection it carries no reconnect loop and no watchdog: on any connection loss it is marked dead and the pool discards it, failing the
  * in-flight work `ConnectionLost(mayHaveExecuted = true)`. Replies are matched FIFO so the synchronous HELLO bootstrap can
  * run on it before it is handed out, and so a transaction's `MULTI`/queue/`EXEC` replies line up with the commands that produced them.
  */
final private[client] class DedicatedConnection private (
  factory: MultiplexedConnection.TransportFactory,
  connectTimeoutMillis: Long
) {

  // stamped once, by the pool, with the Generation live the moment this connection joins it (see [[DedicatedPool.establishOutsideLock]])
  @volatile private var stampedEpoch: MultiplexedConnection.Generation = MultiplexedConnection.Generation.initial

  private val pending                 = new ConcurrentLinkedQueue[DedicatedConnection.Waiter]()
  private val transportRef            = new AtomicReference[Transport]()
  @volatile private var dead: Boolean = false
  // counts work from submit time, not write time: the transport queues asynchronously, so a command sits here before `writeAttempted`
  // moves it to `pending`. Recycling on `pending.isEmpty` alone could hand back a connection with a queued-but-unwritten batch.
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
    * Sends `commands` as a single pipelined write and hands back their raw reply frames in order. Used for a transaction's `MULTI` … `EXEC`
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
          // poison before delivering so release discards it rather than recycling it
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

    val payload: Bytes = command.encode

    def writeAttempted(): Unit = { val _ = pending.add(this) }

    // dropped fires only during teardown, ahead of onClosed; mark dead first so the pool can't recycle this connection mid-close
    def dropped(): Unit = { dead = true; inFlight.decrementAndGet(); callback(Failure(ConnectionLost(mayHaveExecuted = false))) }

    def complete(frame: Frame): Unit = {
      val result = Reply.decode(command, frame)
      inFlight.decrementAndGet()
      callback(result)
    }

    def fail(error: SageException): Unit = { inFlight.decrementAndGet(); callback(Failure(error)) }
  }

  // One write, N waiters: the batch is written (or dropped) atomically, and each reply frame fills its slot. The shared collector fires the
  // single callback once every frame has arrived, or once on the first failure.
  final private class RawBatch(commands: Vector[Command[?]], callback: Try[Vector[Frame]] => Unit) extends Transport.Item {

    private val n         = commands.length
    private val frames    = new AtomicReferenceArray[Frame](n)
    private val remaining = new AtomicInteger(n)
    private val done      = new AtomicBoolean(false)

    val payload: Bytes = Bytes.concatBy(commands)(_.encode)

    def writeAttempted(): Unit = {
      var i = 0
      while (i < n) { val _ = pending.add(new Slot(i)); i += 1 }
    }

    def dropped(): Unit = { dead = true; finish(Failure(ConnectionLost(mayHaveExecuted = false))) }

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

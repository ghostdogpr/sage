package sage.client.internal

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.Bytes
import sage.SageException
import sage.SageException.ConnectionLost
import sage.commands.{Command, Reply}
import sage.protocol.Frame

/**
  * A single connection held exclusively for one blocking command at a time, borrowed from the [[DedicatedPool]]. Unlike the Multiplexed
  * Connection it carries no reconnect loop and no watchdog: on any connection loss it is marked dead and the pool discards it, failing the
  * in-flight command `ConnectionLost(mayHaveExecuted = true)` per ADR-0006 (the command may have run before the reply was lost). Replies
  * are still matched FIFO so the synchronous HELLO bootstrap can run on it before it is handed out.
  */
final private[client] class DedicatedConnection private (
  factory: MultiplexedConnection.TransportFactory,
  connectTimeoutMillis: Long,
  val generation: Long
) {

  private val pending                 = new ConcurrentLinkedQueue[Entry[?]]()
  private val transportRef            = new AtomicReference[Transport]()
  @volatile private var dead: Boolean = false

  def isHealthy: Boolean = !dead

  def submit[A](command: Command[A], callback: Try[A] => Unit): Unit =
    transportRef.get().send(new Entry(command, callback))

  def close(): Unit = {
    dead = true
    val transport = transportRef.get()
    if (transport != null) transport.close()
  }

  private def start(): Unit = {
    val transport = factory(onFrame, onClosed)
    transportRef.set(transport)
    transport.start()
  }

  // blocks on each reply in turn; throws on failure so the caller discards the half-built connection
  private def runBootstrap(bootstrap: Vector[Command[?]]): Unit =
    bootstrap.foreach { command =>
      val latch   = new CountDownLatch(1)
      val outcome = new AtomicReference[Try[Any]]()
      submit[Any](
        command.asInstanceOf[Command[Any]],
        result => {
          outcome.set(result)
          latch.countDown()
        }
      )
      if (!latch.await(connectTimeoutMillis, TimeUnit.MILLISECONDS)) {
        close()
        throw ConnectionLost(mayHaveExecuted = false)
      }
      outcome.get() match {
        case Failure(error) =>
          close()
          throw error
        case Success(_)     => ()
      }
    }

  private def onFrame(frame: Frame): Unit =
    frame match {
      case _: Frame.Push | _: Frame.Attribute => ()
      case reply                              =>
        val entry = pending.poll()
        if (entry == null) close() // a reply with nothing pending means the stream desynced; discard
        else {
          // poison before delivering so release discards it rather than recycling it
          if (Poison.isReadonly(reply)) close()
          entry.complete(reply)
        }
    }

  private def onClosed(): Unit = {
    dead = true
    var entry = pending.poll()
    while (entry != null) {
      entry.fail(ConnectionLost(mayHaveExecuted = true))
      entry = pending.poll()
    }
  }

  final private class Entry[A](command: Command[A], callback: Try[A] => Unit) extends Transport.Item {

    val payload: Bytes = command.encode

    def writeAttempted(): Unit = { val _ = pending.add(this) }

    def dropped(): Unit = callback(Failure(ConnectionLost(mayHaveExecuted = false)))

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
}

private[client] object DedicatedConnection {

  /**
    * Connects and runs the bootstrap synchronously; throws (no retry) if the connect or handshake fails.
    */
  def establish(
    factory: MultiplexedConnection.TransportFactory,
    bootstrap: Vector[Command[?]],
    connectTimeoutMillis: Long,
    generation: Long
  ): DedicatedConnection = {
    val connection = new DedicatedConnection(factory, connectTimeoutMillis, generation)
    connection.start()
    connection.runBootstrap(bootstrap)
    connection
  }
}

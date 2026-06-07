package sage.client.internal

import java.util.concurrent.ConcurrentLinkedQueue

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.Bytes
import sage.SageException
import sage.SageException.{ConnectionLost, NotConnected}
import sage.commands.{Command, Reply}
import sage.protocol.Frame

/**
  * FIFO reply matching over a [[Transport]]. Wire order equals `pending` order because only the writer thread appends to `pending` (via
  * `writeAttempted`, in batch order just before each batch's write) — `submit` never touches it, so racing fibers cannot misalign the two.
  * No reconnect: once the connection is gone this Multiplexer is permanently dead.
  */
final private[client] class Multiplexer(factory: Multiplexer.TransportFactory) {

  private val pending          = new ConcurrentLinkedQueue[Entry[?]]()
  @volatile private var closed = false

  private val transport: Transport = factory(onFrame, () => onClosed())
  transport.start()

  def submit[A](command: Command[A], callback: Try[A] => Unit): Unit =
    if (closed) callback(Failure(NotConnected()))
    else transport.send(new Entry(command, callback))

  def close(): Unit = transport.close()

  // A callback may have no awaiter left (the fiber was interrupted); the reply is still consumed to keep FIFO alignment.
  private def onFrame(frame: Frame): Unit =
    frame match {
      case _: Frame.Push | _: Frame.Attribute => () // out-of-band frames, not replies: they must not consume a pending entry
      case reply                              =>
        val entry = pending.poll()
        if (entry == null) transport.close() // a reply nobody awaits: framing is broken, discard the connection
        else entry.complete(reply)
    }

  private def onClosed(): Unit = {
    closed = true
    var entry = pending.poll()
    while (entry != null) {
      entry.fail(ConnectionLost(mayHaveExecuted = true))
      entry = pending.poll()
    }
  }

  final private class Entry[A](command: Command[A], callback: Try[A] => Unit) extends Transport.Item {

    val payload: Bytes = command.encode

    def writeAttempted(): Unit = {
      val _ = pending.add(this)
    }

    def dropped(): Unit = callback(Failure(ConnectionLost(mayHaveExecuted = false)))

    // the catch guards against throwing user decoders: an escaped exception here would lose the callback and hang the awaiting fiber
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

private[client] object Multiplexer {

  type TransportFactory = (Frame => Unit, () => Unit) => Transport
}

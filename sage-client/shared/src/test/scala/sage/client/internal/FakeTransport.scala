package sage.client.internal

import scala.collection.mutable

import sage.Bytes
import sage.protocol.Frame

/**
  * A scripted Transport: `respond` supplies the connection's reply frames per written payload; with `autoWrite` off, queued items only
  * reach `writeAttempted` through an explicit `writeNext()`.
  */
final class FakeTransport(
  onFrame: Frame => Unit,
  onClosed: () => Unit,
  respond: Bytes => Seq[Frame] = _ => Nil,
  var autoWrite: Boolean = true
) extends Transport {

  private val writes: mutable.ArrayBuffer[Bytes]          = mutable.ArrayBuffer.empty
  private val queued: mutable.ArrayBuffer[Transport.Item] = mutable.ArrayBuffer.empty
  private var draining                                    = false

  def written: Vector[Bytes] = synchronized(writes.toVector)

  var closeCount: Int = 0

  def start(): Unit = ()

  def send(item: Transport.Item): Unit = {
    val shouldDrain = synchronized {
      val _ = queued += item
      if (autoWrite && !draining) { draining = true; true }
      else false
    }
    if (shouldDrain) drain()
  }

  /**
    * Simulates the writer thread draining one queued item.
    */
  def writeNext(): Unit = write(synchronized(queued.remove(0)))

  def emit(frame: Frame): Unit = onFrame(frame)

  def close(): Unit = {
    val (first, dropped) = synchronized {
      closeCount += 1
      if (closeCount == 1) { val pending = queued.toVector; queued.clear(); true -> pending }
      else false -> Vector.empty
    }
    if (first) {
      dropped.foreach(_.dropped())
      onClosed()
    }
  }

  private def drain(): Unit = {
    var item = next()
    while (item != null) {
      try write(item)
      catch {
        case error: Throwable =>
          synchronized { draining = false }
          throw error
      }
      item = next()
    }
  }

  private def next(): Transport.Item =
    synchronized {
      if (queued.nonEmpty) queued.remove(0)
      else { draining = false; null }
    }

  private def write(item: Transport.Item): Unit = {
    item.writeAttempted()
    synchronized { val _ = writes += item.payload }
    respond(item.payload).foreach(onFrame)
  }
}

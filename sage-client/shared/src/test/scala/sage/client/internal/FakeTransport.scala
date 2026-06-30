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

  val written: mutable.ArrayBuffer[Bytes]                 = mutable.ArrayBuffer.empty
  private val queued: mutable.ArrayBuffer[Transport.Item] = mutable.ArrayBuffer.empty

  var closeCount: Int = 0

  def start(): Unit = ()

  def send(item: Transport.Item): Unit =
    if (autoWrite) write(item)
    else {
      val _ = queued += item
    }

  /**
    * Simulates the writer thread draining one queued item.
    */
  def writeNext(): Unit = write(queued.remove(0))

  def emit(frame: Frame): Unit = onFrame(frame)

  def close(): Unit = {
    closeCount += 1
    if (closeCount == 1) {
      queued.foreach(_.dropped())
      queued.clear()
      onClosed()
    }
  }

  private def write(item: Transport.Item): Unit = {
    item.writeAttempted()
    val _ = written += item.payload
    respond(item.payload).foreach(onFrame)
  }
}

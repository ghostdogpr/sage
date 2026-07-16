package sage.client.internal

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import java.util.concurrent.atomic.AtomicBoolean

/**
  * A transport whose `start()` blocks like a socket connect until `close()` aborts it; `reached` fires when `start()` is entered.
  */
final class ConnectingTransport(onClosed: () => Unit) extends Transport {
  val reached                          = new CountDownLatch(1)
  private val gate                     = new CountDownLatch(1)
  private val closed                   = new AtomicBoolean(false)
  private val queued                   = new ConcurrentLinkedQueue[Transport.Item]()
  def wasClosed: Boolean               = closed.get()
  def start(): Unit                    = { reached.countDown(); gate.await(); throw new java.io.IOException("connect aborted") }
  def send(item: Transport.Item): Unit = { val _ = queued.add(item); if (closed.get()) drainQueue() }
  def close(): Unit                    = if (closed.compareAndSet(false, true)) { gate.countDown(); drainQueue(); onClosed() }

  private def drainQueue(): Unit = {
    var item = queued.poll()
    while (item != null) { item.dropped(); item = queued.poll() }
  }
}

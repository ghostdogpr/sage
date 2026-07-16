package sage.client.internal

import java.util.concurrent.CountDownLatch

/**
  * A transport whose `start()` blocks like a socket connect until `close()` aborts it; `reached` fires when `start()` is entered.
  */
final class ConnectingTransport(onClosed: () => Unit) extends Transport {
  val reached                          = new CountDownLatch(1)
  private val gate                     = new CountDownLatch(1)
  private val closed                   = new java.util.concurrent.atomic.AtomicBoolean(false)
  def wasClosed: Boolean               = closed.get()
  def start(): Unit                    = { reached.countDown(); gate.await(); throw new java.io.IOException("connect aborted") }
  def send(item: Transport.Item): Unit = item.dropped()
  def close(): Unit                    = if (closed.compareAndSet(false, true)) { gate.countDown(); onClosed() }
}

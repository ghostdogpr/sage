package sage.client.internal

import java.util.concurrent.CountDownLatch

/**
  * A transport whose `start()` blocks like a socket connect until `close()` aborts it, for exercising shutdown while a connection is still
  * establishing. `reached` fires once `start()` is entered (the connection is registered by then), so a test can wait for that before closing.
  */
final class ConnectingTransport(onClosed: () => Unit) extends Transport {
  val reached                          = new CountDownLatch(1)
  private val gate                     = new CountDownLatch(1)
  @volatile var wasClosed: Boolean     = false
  def start(): Unit                    = { reached.countDown(); gate.await(); throw new java.io.IOException("connect aborted") }
  def send(item: Transport.Item): Unit = item.dropped()
  def close(): Unit                    = { wasClosed = true; gate.countDown(); onClosed() }
}

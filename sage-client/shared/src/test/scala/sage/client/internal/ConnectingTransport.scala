package sage.client.internal

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import java.util.concurrent.atomic.AtomicBoolean

/**
  * A transport whose `start()` waits like a socket connection until `close()` stops it. `reached` is released when `start()` begins.
  */
final class ConnectingTransport(onClosed: () => Unit) extends Transport {
  val reached                          = new CountDownLatch(1)
  private val gate                     = new CountDownLatch(1)
  private val closed                   = new AtomicBoolean(false)
  private val queued                   = new ConcurrentLinkedQueue[Transport.Item]()
  def wasClosed: Boolean               = closed.get()
  def start(): Unit                    = {
    reached.countDown()
    gate.await()
    throw new java.io.IOException("connect aborted")
  }
  def send(item: Transport.Item): Unit = {
    queued.add(item)
    if (closed.get()) drainQueue()
  }
  def close(): Unit                    = if (closed.compareAndSet(false, true)) {
    gate.countDown()
    drainQueue()
    onClosed()
  }

  private def drainQueue(): Unit = {
    var item = queued.poll()
    while (item != null) {
      item.dropped()
      item = queued.poll()
    }
  }
}

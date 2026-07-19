package sage.client.internal

import java.io.IOException
import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import sage.protocol.{Frame, RespParser}

/**
  * A plain blocking socket pumped by two virtual threads — a reader feeding the RESP3 parser, a writer draining the send queue.
  * `start` resolves the hostname, connects, then hands the connected socket to `upgrade` (identity for plaintext, a TLS handshake for
  * `SSLSocket`); `close` aborts an in-progress connect or handshake by closing the plain socket, and never lets the I/O threads start once
  * it has run. Caveat: a hung DNS lookup inside `start` cannot be interrupted (a JDK limitation) — it unwinds when the OS resolver times
  * out; `close` closing the socket then prevents the TCP connect that would have followed.
  */
final private[client] class SocketTransport private (
  host: String,
  port: Int,
  connectTimeoutMillis: Int,
  upgrade: Socket => Socket,
  onFrame: Frame => Unit,
  onClosed: () => Unit
) extends Transport {

  private val socket = new Socket()
  private val queue  = new LinkedBlockingQueue[Transport.Item]()
  private val closed = new AtomicBoolean(false)

  // the socket whose streams the I/O threads use: the plain socket for plaintext, the SSLSocket after a TLS handshake. Assigned in `start`
  // before the threads launch (so a plain `var` is safely published via thread-start); `close` always tears down the plain `socket`, which
  // closes a layered SSLSocket too (autoClose).
  private var ioSocket: Socket = socket

  // serializes the I/O-thread start against termination so threads are either started-and-joined by close() or never started at all
  private val lifecycle      = new ReentrantLock()
  private var threadsStarted = false

  @volatile private[internal] var writeCount: Long = 0

  private val id                       = SocketTransport.ids.incrementAndGet()
  private[internal] val reader: Thread = Thread.ofVirtual().name(s"sage-reader-$id").unstarted(() => readLoop())
  private[internal] val writer: Thread = Thread.ofVirtual().name(s"sage-writer-$id").unstarted(() => writeLoop())

  def start(): Unit = {
    try {
      socket.setTcpNoDelay(true)
      socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis) // resolves the hostname here, fresh on every attempt
      socket.setSoTimeout(connectTimeoutMillis)                               // bound a TLS handshake's reads like the connect
      ioSocket = upgrade(socket)
      socket.setSoTimeout(0)                                                  // steady state: blocking reads with no timeout
    } catch {
      case NonFatal(error) =>
        terminate()
        throw error
    }
    val launched = locked {
      if (closed.get()) false
      else {
        reader.start()
        writer.start()
        threadsStarted = true
        true
      }
    }
    if (!launched) throw new IOException("transport closed during connect")
  }

  def send(item: Transport.Item): Unit = {
    queue.put(item)
    // `terminate` may have drained between the put and this check; draining again closes that window
    if (closed.get()) drainQueue()
  }

  def close(): Unit = {
    terminate()
    if (Thread.currentThread() ne reader) reader.join()
    if (Thread.currentThread() ne writer) writer.join()
  }

  private def readLoop(): Unit = {
    val parser = new RespParser
    val buffer = new Array[Byte](8192)
    try {
      val in   = ioSocket.getInputStream
      var done = false
      while (!done) {
        val n = in.read(buffer)
        if (n < 0) done = true
        else if (parser.feed(buffer, 0, n)(onFrame).isDefined) done = true
      }
    } catch {
      case _: InterruptedException => () // terminate() interrupts the reader to fence it
      case NonFatal(_)             => ()
    } finally terminate()
  }

  // Auto-pipelining: queued items are coalesced up to MaxBatchBytes per concat buffer; an item that would overflow the cap is
  // carried into the next batch. Items past `attempted` (and a carried item) never reached a write and are dropped on unwind.
  private def writeLoop(): Unit = {
    val batch                 = new java.util.ArrayList[Transport.Item]()
    var carry: Transport.Item = null
    var attempted             = 0
    var scratch: Array[Byte]  = null
    try {
      val out = ioSocket.getOutputStream
      while (true) {
        // consuming a carried item bypasses take(), the writer's only interruption point: re-check for close first
        if (carry != null && closed.get()) throw new InterruptedException
        val first      = if (carry != null) carry else queue.take()
        carry = null
        batch.add(first)
        var size: Long = first.payload.length
        var draining   = size < SocketTransport.MaxBatchBytes
        while (draining) {
          val item = queue.poll()
          if (item == null) draining = false
          else if (size + item.payload.length > SocketTransport.MaxBatchBytes) {
            carry = item
            draining = false
          } else {
            batch.add(item)
            size += item.payload.length
          }
        }
        if (batch.size == 1) {
          val payload = first.payload
          first.writeAttempted()
          first.clearPayload()
          attempted = 1
          out.write(payload.unsafeArray)
        } else {
          if (scratch == null || scratch.length < size) {
            var capacity = if (scratch == null) 1024L else scratch.length.toLong
            while (capacity < size) capacity *= 2
            scratch = new Array[Byte](math.min(capacity, SocketTransport.MaxBatchBytes).toInt)
          }
          var offset = 0
          batch.forEach { item =>
            val bytes = item.payload.unsafeArray
            System.arraycopy(bytes, 0, scratch, offset, bytes.length)
            offset += bytes.length
          }
          batch.forEach { item =>
            item.writeAttempted()
            item.clearPayload()
            attempted += 1
          }
          out.write(scratch, 0, offset)
        }
        writeCount += 1
        // clear before resetting: the finally's drop range must stay empty if anything is ever inserted here
        batch.clear()
        attempted = 0
      }
    } catch {
      case _: InterruptedException => ()
      case NonFatal(_)             => ()
    } finally {
      var i = attempted
      while (i < batch.size) {
        batch.get(i).dropped()
        i += 1
      }
      if (carry != null) carry.dropped()
      terminate()
    }
  }

  private def terminate(): Unit =
    if (closed.compareAndSet(false, true)) {
      try socket.close()
      catch {
        case _: IOException => ()
      }
      if (locked(threadsStarted)) {
        writer.interrupt()
        if (Thread.currentThread() ne writer) writer.join()
        // fence the reader before onClosed, else an in-flight reply races the consumer's pending drain (#94); interrupt frees a reader
        // parked on backpressure so the join cannot hang
        if (Thread.currentThread() ne reader) { reader.interrupt(); reader.join() }
      }
      drainQueue()
      onClosed()
    }

  private def drainQueue(): Unit = {
    var item = queue.poll()
    while (item != null) {
      item.dropped()
      item = queue.poll()
    }
  }

  private inline def locked[A](inline body: A): A = {
    lifecycle.lock()
    try body
    finally lifecycle.unlock()
  }
}

private[client] object SocketTransport {

  private val ids = new AtomicLong(0)

  // strict bound on multi-item concat buffers; a single over-sized payload is always a batch of one, written zero-copy
  private val MaxBatchBytes: Long = 512 * 1024

  /**
    * Builds an unconnected transport; `start()` resolves the host and connects, so the connect happens after the transport is published
    * and `close()` can reach the socket to abort it. `upgrade` turns the connected plain socket into the one the I/O threads use (identity
    * for plaintext, a TLS handshake otherwise).
    */
  def connect(
    host: String,
    port: Int,
    connectTimeout: FiniteDuration,
    upgrade: Socket => Socket,
    onFrame: Frame => Unit,
    onClosed: () => Unit
  ): SocketTransport =
    new SocketTransport(host, port, math.min(connectTimeout.toMillis, Int.MaxValue).toInt, upgrade, onFrame, onClosed)
}

package sage.client.internal

import java.io.IOException
import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import sage.Bytes
import sage.protocol.{Frame, RespParser}

/**
  * A plain blocking socket pumped by two virtual threads — a reader feeding the RESP3 parser, a writer draining the send queue.
  * `start` resolves the hostname and connects; `close` aborts an in-progress connect by closing the socket, and never lets the I/O
  * threads start once it has run. Caveat: a hung DNS lookup inside `start` cannot be interrupted (a JDK limitation) — it unwinds when the
  * OS resolver times out; `close` closing the socket then prevents the TCP connect that would have followed.
  */
final private[client] class SocketTransport private (
  host: String,
  port: Int,
  connectTimeoutMillis: Int,
  onFrame: Frame => Unit,
  onClosed: () => Unit
) extends Transport {

  private val socket = new Socket()
  private val queue  = new LinkedBlockingQueue[Transport.Item]()
  private val closed = new AtomicBoolean(false)

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
      val in   = socket.getInputStream
      var done = false
      while (!done) {
        val n = in.read(buffer)
        if (n < 0) done = true
        else {
          parser.feed(Bytes.wrap(IArray.unsafeFromArray(java.util.Arrays.copyOfRange(buffer, 0, n)))) match {
            case Right(frames) => frames.foreach(onFrame)
            case Left(_)       => done = true
          }
        }
      }
    } catch {
      case NonFatal(_) => ()
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
      val out = socket.getOutputStream
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
          first.writeAttempted()
          attempted = 1
          out.write(first.payload.unsafeArray)
        } else {
          if (scratch == null) scratch = new Array[Byte](SocketTransport.MaxBatchBytes.toInt)
          var offset = 0
          batch.forEach { item =>
            val bytes = item.payload.unsafeArray
            System.arraycopy(bytes, 0, scratch, offset, bytes.length)
            offset += bytes.length
          }
          batch.forEach { item =>
            item.writeAttempted()
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
    * and `close()` can reach the socket to abort it.
    */
  def connect(host: String, port: Int, connectTimeout: FiniteDuration, onFrame: Frame => Unit, onClosed: () => Unit): SocketTransport =
    new SocketTransport(host, port, math.min(connectTimeout.toMillis, Int.MaxValue).toInt, onFrame, onClosed)
}

package sage.client.internal

import java.io.IOException
import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import sage.Bytes
import sage.protocol.{Frame, RespParser}

/**
  * A plain blocking socket pumped by two virtual threads — a reader feeding the RESP3 parser, a writer draining the send queue.
  * `terminate` joins the writer before draining, so no item can slip into `writeAttempted` after the unwritten ones are dropped.
  */
final private[client] class SocketTransport private (socket: Socket, onFrame: Frame => Unit, onClosed: () => Unit) extends Transport {

  private val queue  = new LinkedBlockingQueue[Transport.Item]()
  private val closed = new AtomicBoolean(false)

  @volatile private[internal] var writeCount: Long = 0

  private val id                       = SocketTransport.ids.incrementAndGet()
  private[internal] val reader: Thread = Thread.ofVirtual().name(s"sage-reader-$id").unstarted(() => readLoop())
  private[internal] val writer: Thread = Thread.ofVirtual().name(s"sage-writer-$id").unstarted(() => writeLoop())

  def start(): Unit = {
    reader.start()
    writer.start()
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

  // Auto-pipelining: items queued while the previous write was in flight are coalesced, up to MaxBatchBytes per socket write.
  // `attempted` tracks how many batch items have had writeAttempted: anything past it never reached a write and is dropped on unwind.
  private def writeLoop(): Unit = {
    val batch     = new java.util.ArrayList[Transport.Item]()
    var attempted = 0
    try {
      val out = socket.getOutputStream
      while (true) {
        val first = queue.take()
        batch.add(first)
        var size: Long = first.payload.length
        var next       = if (size < SocketTransport.MaxBatchBytes) queue.poll() else null
        while (next != null) {
          batch.add(next)
          size += next.payload.length
          next = if (size < SocketTransport.MaxBatchBytes) queue.poll() else null
        }
        if (batch.size == 1) {
          first.writeAttempted()
          attempted = 1
          out.write(first.payload.unsafeArray)
          writeCount += 1
        } else if (size <= SocketTransport.MaxSingleWrite) {
          val payload = new Array[Byte](size.toInt)
          var offset  = 0
          batch.forEach { item =>
            val bytes = item.payload.unsafeArray
            System.arraycopy(bytes, 0, payload, offset, bytes.length)
            offset += bytes.length
          }
          batch.forEach { item =>
            item.writeAttempted()
            attempted += 1
          }
          out.write(payload)
          writeCount += 1
        } else {
          var i = 0
          while (i < batch.size) {
            val item = batch.get(i)
            item.writeAttempted()
            attempted = i + 1
            out.write(item.payload.unsafeArray)
            writeCount += 1
            i += 1
          }
        }
        attempted = 0
        batch.clear()
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
      terminate()
    }
  }

  private def terminate(): Unit =
    if (closed.compareAndSet(false, true)) {
      try socket.close()
      catch {
        case _: IOException => ()
      }
      writer.interrupt()
      if (Thread.currentThread() ne writer) writer.join()
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
}

private[client] object SocketTransport {

  private val ids = new AtomicLong(0)

  // bounds the coalescing drain (and so the concat buffer); one over-sized item may still exceed it, hence the per-item fallback
  private val MaxBatchBytes: Long  = 512 * 1024
  private val MaxSingleWrite: Long = Int.MaxValue - 8

  /**
    * Blocking connect; the returned transport is not started.
    */
  def connect(host: String, port: Int, connectTimeout: FiniteDuration, onFrame: Frame => Unit, onClosed: () => Unit): SocketTransport = {
    val socket = new Socket()
    try {
      socket.setTcpNoDelay(true)
      socket.connect(new InetSocketAddress(host, port), math.min(connectTimeout.toMillis, Int.MaxValue).toInt)
    } catch {
      case NonFatal(error) =>
        try socket.close()
        catch {
          case _: IOException => ()
        }
        throw error
    }
    new SocketTransport(socket, onFrame, onClosed)
  }
}

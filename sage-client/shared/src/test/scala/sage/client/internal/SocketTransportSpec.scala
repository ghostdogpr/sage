package sage.client.internal

import java.io.InputStream
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.*

import sage.Bytes
import sage.protocol.Frame

class SocketTransportSpec extends munit.FunSuite {

  final private class RecordingItem(val text: String) extends Transport.Item {
    var payload: Bytes                = Bytes.utf8(text)
    @volatile var writeAttempts: Int  = 0
    @volatile var drops: Int          = 0
    @volatile var clears: Int         = 0
    def writeAttempted(): Unit        = writeAttempts += 1
    def dropped(): Unit               = drops += 1
    override def clearPayload(): Unit = { clears += 1; payload = Bytes.empty }
  }

  private def withTransport(
    onClosed: () => Unit,
    onFrame: Frame => Unit = _ => (),
    beforeStart: SocketTransport => Unit = _ => ()
  )(body: (SocketTransport, Socket) => Unit): Unit = {
    val server = new ServerSocket(0)
    try {
      val transport = SocketTransport.connect("127.0.0.1", server.getLocalPort, 5.seconds, identity, onFrame, onClosed)
      beforeStart(transport)
      transport.start()
      val peer      = server.accept()
      try body(transport, peer)
      finally peer.close()
    } finally server.close()
  }

  private def readExactly(in: InputStream, n: Int): String = {
    val bytes = in.readNBytes(n)
    new String(bytes, StandardCharsets.UTF_8)
  }

  private def awaitUntil(condition: => Boolean, label: String): Unit = {
    var remaining = 100
    while (!condition && remaining > 0) {
      Thread.sleep(20)
      remaining -= 1
    }
    assert(condition, s"timed out waiting for: $label")
  }

  private def assertAllArrive(peer: Socket, items: Seq[RecordingItem]): Unit = {
    val expected = items.map(_.text).mkString
    assertEquals(readExactly(peer.getInputStream, expected.length), expected)
  }

  test("writes payloads, delivers parsed frames, and joins its threads on close") {
    @volatile var frames      = Vector.empty[Frame]
    @volatile var closedCount = 0
    withTransport(onClosed = () => closedCount += 1, onFrame = frame => frames :+= frame) { (transport, peer) =>
      val item = new RecordingItem("PING\r\n")
      transport.send(item)
      assertEquals(readExactly(peer.getInputStream, 6), "PING\r\n")
      assertEquals(item.writeAttempts, 1)
      assertEquals(item.clears, 1)
      peer.getOutputStream.write("+PONG\r\n".getBytes(StandardCharsets.UTF_8))
      peer.getOutputStream.flush()
      awaitUntil(frames == Vector(Frame.SimpleString("PONG")), "the PONG frame")
      transport.close()
      transport.close()
      assertEquals(closedCount, 1)
      assert(!transport.reader.isAlive)
      assert(!transport.writer.isAlive)
      assertEquals(item.drops, 0)
    }
  }

  test("items queued together are batched into a single socket write") {
    val items = (1 to 10).map(i => new RecordingItem(s"PING $i\r\n"))
    withTransport(onClosed = () => (), beforeStart = transport => items.foreach(transport.send)) { (transport, peer) =>
      assertAllArrive(peer, items)
      awaitWriteCount(transport, 1L)
      items.foreach { item =>
        assertEquals(item.writeAttempts, 1)
        assertEquals(item.clears, 1)
      }
      transport.close()
    }
  }

  test("coalescing never exceeds the byte cap and an over-sized item is written alone") {
    val sizes = Vector(200, 200, 600, 200).map(_ * 1024)
    val items = sizes.zipWithIndex.map { case (size, i) => new RecordingItem((i + 1).toString * size) }
    withTransport(onClosed = () => (), beforeStart = transport => items.foreach(transport.send)) { (transport, peer) =>
      // [1,2] coalesce under the cap; 3 would overflow it and goes alone; 4 follows alone
      assertAllArrive(peer, items)
      awaitWriteCount(transport, 3L)
      items.foreach(item => assertEquals(item.writeAttempts, 1))
      transport.close()
    }
  }

  test("items summing exactly to the byte cap coalesce into one write") {
    val items = (1 to 2).map(i => new RecordingItem(i.toString * (256 * 1024)))
    withTransport(onClosed = () => (), beforeStart = transport => items.foreach(transport.send)) { (transport, peer) =>
      assertAllArrive(peer, items)
      awaitWriteCount(transport, 1L)
      transport.close()
    }
  }

  // the counter is bumped after the socket write, so the bytes can reach the peer before the writer thread increments it
  private def awaitWriteCount(transport: SocketTransport, expected: Long): Unit = {
    awaitUntil(transport.writeCount >= expected, s"writeCount to reach $expected")
    assertEquals(transport.writeCount, expected)
  }

  test("connection loss after a batched write leaves every item with exactly one hook fired") {
    @volatile var closedCount = 0
    val items                 = (1 to 3).map(i => new RecordingItem(s"PING $i\r\n"))
    withTransport(onClosed = () => closedCount += 1, beforeStart = transport => items.foreach(transport.send)) { (transport, peer) =>
      assertAllArrive(peer, items)
      peer.close()
      awaitUntil(closedCount == 1, "the transport to observe the disconnect")
      items.foreach { item =>
        assertEquals(item.writeAttempts, 1)
        assertEquals(item.drops, 0)
      }
      transport.close()
    }
  }

  test("a malformed frame poisons the connection") {
    @volatile var closedCount = 0
    withTransport(onClosed = () => closedCount += 1) { (transport, peer) =>
      peer.getOutputStream.write("?garbage\r\n".getBytes(StandardCharsets.UTF_8))
      peer.getOutputStream.flush()
      awaitUntil(closedCount == 1, "the poisoned connection to close")
      transport.close()
      assertEquals(closedCount, 1)
    }
  }

  test("onClosed runs only after the reader is fenced, so a buffered frame cannot race the close") {
    val readerAliveAtClose                      = new AtomicBoolean(false)
    val inOnFrame                               = new CountDownLatch(1)
    val proceed                                 = new CountDownLatch(1)
    @volatile var transportRef: SocketTransport = null
    withTransport(
      onClosed = () => { readerAliveAtClose.set(transportRef.reader.isAlive); proceed.countDown() },
      onFrame = _ => { inOnFrame.countDown(); proceed.await() }
    ) { (transport, peer) =>
      transportRef = transport
      peer.getOutputStream.write("+PONG\r\n".getBytes(StandardCharsets.UTF_8))
      peer.getOutputStream.flush()
      assert(inOnFrame.await(5, TimeUnit.SECONDS), "reader should reach frame delivery")

      val closer = new Thread(() => transport.close())
      closer.start()
      closer.join(5000)
      assert(!closer.isAlive, "close() must not hang")
      assert(!readerAliveAtClose.get(), "onClosed must run only after the reader has been joined")
    }
  }

  test("a server-initiated disconnect closes the transport and drops unwritten items") {
    @volatile var closedCount = 0
    withTransport(onClosed = () => closedCount += 1) { (transport, peer) =>
      peer.close()
      awaitUntil(closedCount == 1, "the transport to observe the disconnect")
      val item = new RecordingItem("PING\r\n")
      transport.send(item)
      assertEquals(item.drops, 1)
      assertEquals(item.writeAttempts, 0)
      assertEquals(item.clears, 0)
      transport.close()
    }
  }
}

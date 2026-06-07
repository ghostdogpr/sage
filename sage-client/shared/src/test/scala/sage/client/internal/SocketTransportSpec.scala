package sage.client.internal

import java.io.InputStream
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets

import scala.concurrent.duration.*

import sage.Bytes
import sage.protocol.Frame

class SocketTransportSpec extends munit.FunSuite {

  final private class RecordingItem(text: String) extends Transport.Item {
    val payload: Bytes               = Bytes.utf8(text)
    @volatile var writeAttempts: Int = 0
    @volatile var drops: Int         = 0
    def writeAttempted(): Unit       = writeAttempts += 1
    def dropped(): Unit              = drops += 1
  }

  private def withTransport(
    onClosed: () => Unit,
    onFrame: Frame => Unit = _ => ()
  )(body: (SocketTransport, Socket) => Unit): Unit = {
    val server = new ServerSocket(0)
    try {
      val transport = SocketTransport.connect("127.0.0.1", server.getLocalPort, 5.seconds, onFrame, onClosed)
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

  test("writes payloads, delivers parsed frames, and joins its threads on close") {
    @volatile var frames      = Vector.empty[Frame]
    @volatile var closedCount = 0
    withTransport(onClosed = () => closedCount += 1, onFrame = frame => frames :+= frame) { (transport, peer) =>
      val item = new RecordingItem("PING\r\n")
      transport.send(item)
      assertEquals(readExactly(peer.getInputStream, 6), "PING\r\n")
      assertEquals(item.writeAttempts, 1)
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

  test("a server-initiated disconnect closes the transport and drops unwritten items") {
    @volatile var closedCount = 0
    withTransport(onClosed = () => closedCount += 1) { (transport, peer) =>
      peer.close()
      awaitUntil(closedCount == 1, "the transport to observe the disconnect")
      val item = new RecordingItem("PING\r\n")
      transport.send(item)
      assertEquals(item.drops, 1)
      assertEquals(item.writeAttempts, 0)
      transport.close()
    }
  }
}

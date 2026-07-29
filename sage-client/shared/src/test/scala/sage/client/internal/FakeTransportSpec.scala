package sage.client.internal

import scala.collection.mutable

import sage.Bytes
import sage.protocol.Frame

class FakeTransportSpec extends munit.FunSuite {

  final private class Item(text: String) extends Transport.Item {
    val payload: Bytes         = Bytes.utf8(text)
    def writeAttempted(): Unit = ()
    def dropped(): Unit        = ()
  }

  test("auto writes finish the current item's replies before draining a reentrant send") {
    val frames              = mutable.ArrayBuffer.empty[Frame]
    val second              = new Item("second")
    var fake: FakeTransport = null
    fake = new FakeTransport(
      frame => {
        frames += frame
        if (frame == Frame.Integer(1L)) fake.send(second)
      },
      () => (),
      payload =>
        if (payload.asUtf8String == "first") Seq(Frame.Integer(1L), Frame.Integer(2L))
        else Seq(Frame.Integer(3L))
    )

    fake.send(new Item("first"))

    assertEquals(frames.toVector, Vector(Frame.Integer(1L), Frame.Integer(2L), Frame.Integer(3L)))
    assertEquals(fake.written.map(_.asUtf8String), Vector("first", "second"))
  }
}

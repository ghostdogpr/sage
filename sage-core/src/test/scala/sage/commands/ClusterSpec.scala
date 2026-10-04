package sage.commands

import sage.SageException.DecodeError
import sage.cluster.{Node, Slot, SlotRange}
import sage.protocol.Frame
import sage.protocol.Frames.bulk

class ClusterSpec extends munit.FunSuite {

  private def int(value: Long): Frame                          = Frame.Integer(value)
  private def node(host: String, port: Int, id: String): Frame =
    Frame.Array(Vector(bulk(host), int(port.toLong), bulk(id)))

  private val queried = Node("10.9.9.9", 7000)

  private def run(frame: Frame): Either[DecodeError, Vector[SlotRange]] = Cluster.slots(queried).decode(frame)

  test("decodes a range with its master and replicas") {
    val reply = Frame.Array(
      Vector(
        Frame.Array(Vector(int(0), int(5460), node("10.0.0.1", 6379, "m1"), node("10.0.0.2", 6379, "r1")))
      )
    )
    assertEquals(
      run(reply),
      Right(Vector(SlotRange(Slot.at(0).get, Slot.at(5460).get, Node("10.0.0.1", 6379), Vector(Node("10.0.0.2", 6379)))))
    )
  }

  test("keeps each range of the same master as listed") {
    val master = node("10.0.0.1", 6379, "m1")
    val reply  = Frame.Array(
      Vector(
        Frame.Array(Vector(int(0), int(10), master)),
        Frame.Array(Vector(int(100), int(110), master))
      )
    )
    assertEquals(
      run(reply),
      Right(
        Vector(
          SlotRange(Slot.at(0).get, Slot.at(10).get, Node("10.0.0.1", 6379), Vector.empty),
          SlotRange(Slot.at(100).get, Slot.at(110).get, Node("10.0.0.1", 6379), Vector.empty)
        )
      )
    )
  }

  test("keeps distinct masters in first-seen order") {
    val reply = Frame.Array(
      Vector(
        Frame.Array(Vector(int(0), int(10), node("a", 6379, "a1"))),
        Frame.Array(Vector(int(11), int(20), node("b", 6379, "b1")))
      )
    )
    assertEquals(run(reply).map(_.map(_.master)), Right(Vector(Node("a", 6379), Node("b", 6379))))
  }

  test("an empty topology decodes to no shards") {
    assertEquals(run(Frame.Array(Vector.empty)), Right(Vector.empty))
  }

  test("a slot index out of range fails the whole decode") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(int(0), int(20000), node("a", 6379, "a1")))))
    assert(run(reply).isLeft)
  }

  test("a null or empty endpoint decodes to the host of the queried node") {
    val master  = Frame.Array(Vector(Frame.Null, int(6379), bulk("m1")))
    val replica = node("", 6380, "r1")
    val reply   = Frame.Array(Vector(Frame.Array(Vector(int(0), int(10), master, replica))))
    assertEquals(
      run(reply).map(_.map(range => range.master +: range.replicas)),
      Right(Vector(Vector(Node("10.9.9.9", 6379), Node("10.9.9.9", 6380))))
    )
  }

  test("a `?` endpoint stays literal: it means an unknown node, not the queried one") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(int(0), int(10), node("?", 6379, "m1")))))
    assertEquals(run(reply).map(_.map(_.master)), Right(Vector(Node("?", 6379))))
  }

  test("a non-array reply fails") {
    assert(run(Frame.SimpleString("OK")).isLeft)
  }

  test("a slot value outside Int range fails decode rather than wrapping into a valid slot") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(int(0), int(Int.MaxValue.toLong + 1L), node("a", 6379, "a1")))))
    assert(run(reply).isLeft)
  }
}

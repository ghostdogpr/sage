package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.cluster.{Node, Shard, Slot, SlotRange}
import sage.protocol.Frame

class ClusterSpec extends munit.FunSuite {

  private def int(value: Long): Frame                          = Frame.Integer(value)
  private def bulk(text: String): Frame                        = Frame.BulkString(Bytes.utf8(text))
  private def node(host: String, port: Int, id: String): Frame =
    Frame.Array(Vector(bulk(host), int(port.toLong), bulk(id)))

  private def run(frame: Frame): Either[DecodeError, Vector[Shard]] = Cluster.slots.decode(frame)

  test("decodes a range into a Shard with master and replicas") {
    val reply = Frame.Array(
      Vector(
        Frame.Array(Vector(int(0), int(5460), node("10.0.0.1", 6379, "m1"), node("10.0.0.2", 6379, "r1")))
      )
    )
    assertEquals(
      run(reply),
      Right(Vector(Shard(Node("10.0.0.1", 6379), Vector(Node("10.0.0.2", 6379)), Vector(SlotRange(Slot.unsafe(0), Slot.unsafe(5460))))))
    )
  }

  test("merges multiple ranges owned by the same master into one Shard") {
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
          Shard(
            Node("10.0.0.1", 6379),
            Vector.empty,
            Vector(SlotRange(Slot.unsafe(0), Slot.unsafe(10)), SlotRange(Slot.unsafe(100), Slot.unsafe(110)))
          )
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

  test("a non-array reply fails") {
    assert(run(Frame.SimpleString("OK")).isLeft)
  }

  test("a slot value outside Int range fails decode rather than wrapping into a valid slot") {
    val reply = Frame.Array(Vector(Frame.Array(Vector(int(0), int(Int.MaxValue.toLong + 1L), node("a", 6379, "a1")))))
    assert(run(reply).isLeft)
  }
}

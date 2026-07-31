package sage.commands

import sage.protocol.Frame
import sage.protocol.Frames.bulk

class PubsubSpec extends munit.FunSuite with BroadcastFolds {

  private val introspection = Vector(
    "PUBSUB CHANNELS"      -> Pubsub.pubsubChannels(),
    "PUBSUB SHARDCHANNELS" -> Pubsub.pubsubShardChannels(),
    "PUBSUB NUMSUB"        -> Pubsub.pubsubNumSub("news"),
    "PUBSUB SHARDNUMSUB"   -> Pubsub.pubsubShardNumSub("news"),
    "PUBSUB NUMPAT"        -> Pubsub.pubsubNumPat
  )

  test("every PUBSUB introspection form broadcasts per master, since a node answers only for the subscribers attached to it") {
    introspection.foreach { case (name, command) =>
      assert(command.allMasters, s"$name must sweep every slot-owning master")
      assert(command.rawFrame.allMasters, s"$name must keep allMasters through rawFrame")
      command.broadcast match {
        case BroadcastReduce.Fold(_) => ()
        case other                   => fail(s"$name must merge with a Fold, got $other")
      }
    }
  }

  test("PUBSUB introspection stays transaction-legal with node-local semantics, as KEYS does") {
    introspection.foreach { case (name, command) =>
      assert(!command.requiresClusterWideTxResult, s"$name must not be rejected inside a transaction")
    }
  }

  test("PUBLISH and SPUBLISH stay single-node: the cluster bus fans PUBLISH out, and SPUBLISH routes by its channel's slot") {
    assert(!Pubsub.publish("news", "hello").allMasters)
    assert(!Pubsub.sPublish("orders", "placed").allMasters)
    assertEquals(Pubsub.sPublish("orders", "placed").keyIndices, Command.FirstKey)
  }

  test("CHANNELS merges each master's slice and reports a channel held on two masters once") {
    val merge = fold(Pubsub.pubsubChannels())
    val first = Frame.Array(Vector(bulk("news"), bulk("sport")))
    val other = Frame.Array(Vector(bulk("news"), bulk("weather")))
    assertEquals(Reply.run(Pubsub.pubsubChannels(), merge(first, other)), Right(Vector("news", "sport", "weather")))
  }

  test("CHANNELS merges an empty master in either position without losing the other's slice") {
    val merge    = fold(Pubsub.pubsubChannels())
    val occupied = Frame.Array(Vector(bulk("news")))
    val bare     = Frame.Array(Vector.empty)
    assertEquals(Reply.run(Pubsub.pubsubChannels(), merge(occupied, bare)), Right(Vector("news")))
    assertEquals(Reply.run(Pubsub.pubsubChannels(), merge(bare, occupied)), Right(Vector("news")))
  }

  test("the CHANNELS merge passes a malformed reply through in either operand position, so it never hides behind a valid one") {
    val merge = fold(Pubsub.pubsubChannels())
    val valid = Frame.Array(Vector(bulk("news")))
    val bad   = Frame.SimpleString("nonsense")
    assertEquals(merge(valid, bad), bad)
    assertEquals(merge(bad, valid), bad)
    assert(Reply.run(Pubsub.pubsubChannels(), merge(valid, bad)).isLeft)
  }

  test("NUMSUB sums a channel's subscribers across masters instead of letting the decoder's Map keep only the last count") {
    val merge = fold(Pubsub.pubsubNumSub("news", "sport"))
    val first = Frame.Array(Vector(bulk("news"), Frame.Integer(1L), bulk("sport"), Frame.Integer(0L)))
    val other = Frame.Array(Vector(bulk("news"), Frame.Integer(2L), bulk("sport"), Frame.Integer(5L)))
    assertEquals(Reply.run(Pubsub.pubsubNumSub("news", "sport"), merge(first, other)), Right(Map("news" -> 3L, "sport" -> 5L)))
  }

  test("the NUMSUB merge keeps each channel's first-seen position and admits a master that reports a channel the other does not") {
    val merge  = fold(Pubsub.pubsubNumSub("news", "sport"))
    val first  = Frame.Array(Vector(bulk("news"), Frame.Integer(1L)))
    val other  = Frame.Array(Vector(bulk("sport"), Frame.Integer(4L), bulk("news"), Frame.Integer(2L)))
    val merged = merge(first, other)
    assertEquals(Reply.run(Pubsub.pubsubNumSub("news", "sport"), merged), Right(Map("news" -> 3L, "sport" -> 4L)))
    assertEquals(merged, Frame.Array(Vector(bulk("news"), Frame.Integer(3L), bulk("sport"), Frame.Integer(4L))))
  }

  test("SHARDNUMSUB sums per channel too, so a shard channel keeps its owner's count when the other masters report zero") {
    val merge = fold(Pubsub.pubsubShardNumSub("orders"))
    val owner = Frame.Array(Vector(bulk("orders"), Frame.Integer(3L)))
    val bare  = Frame.Array(Vector(bulk("orders"), Frame.Integer(0L)))
    assertEquals(Reply.run(Pubsub.pubsubShardNumSub("orders"), merge(bare, owner)), Right(Map("orders" -> 3L)))
  }

  test("the NUMSUB merge passes an odd-length or mistyped reply through in either operand position") {
    val merge = fold(Pubsub.pubsubNumSub("news"))
    val valid = Frame.Array(Vector(bulk("news"), Frame.Integer(1L)))
    val odd   = Frame.Array(Vector(bulk("news")))
    val typed = Frame.Array(Vector(bulk("news"), bulk("1")))
    assertEquals(merge(valid, odd), odd)
    assertEquals(merge(odd, valid), odd)
    assertEquals(merge(valid, typed), typed)
    assert(Reply.run(Pubsub.pubsubNumSub("news"), merge(valid, odd)).isLeft)
  }

  test("NUMPAT sums each master's pattern count, with checked overflow and malformed passthrough") {
    val merge = fold(Pubsub.pubsubNumPat)
    assertEquals(Reply.run(Pubsub.pubsubNumPat, merge(Frame.Integer(2L), Frame.Integer(3L))), Right(5L))
    val bad   = Frame.SimpleString("nonsense")
    assertEquals(merge(Frame.Integer(2L), bad), bad)
    assertEquals(merge(bad, Frame.Integer(2L)), bad)
    intercept[ArithmeticException](merge(Frame.Integer(Long.MaxValue), Frame.Integer(1L)))
  }
}

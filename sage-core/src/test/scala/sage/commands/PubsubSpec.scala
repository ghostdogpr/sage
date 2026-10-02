package sage.commands

import sage.SageException.DecodeError
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
    assertEquals(Reply.decode(Pubsub.pubsubChannels(), merge(first, other)).toEither, Right(Vector("news", "sport", "weather")))
  }

  test("CHANNELS merges an empty master in either position without losing the other's slice") {
    val merge    = fold(Pubsub.pubsubChannels())
    val occupied = Frame.Array(Vector(bulk("news")))
    val bare     = Frame.Array(Vector.empty)
    assertEquals(Reply.decode(Pubsub.pubsubChannels(), merge(occupied, bare)).toEither, Right(Vector("news")))
    assertEquals(Reply.decode(Pubsub.pubsubChannels(), merge(bare, occupied)).toEither, Right(Vector("news")))
  }

  test("the CHANNELS merge fails on a malformed reply in either operand position, so it never hides behind a valid one") {
    val merge = fold(Pubsub.pubsubChannels())
    val valid = Frame.Array(Vector(bulk("news")))
    val bad   = Frame.SimpleString("nonsense")
    intercept[DecodeError](merge(valid, bad))
    intercept[DecodeError](merge(bad, valid))
  }

  test("NUMSUB sums a channel's subscribers across masters instead of letting the decoder's Map keep only the last count") {
    val merge = fold(Pubsub.pubsubNumSub("news", "sport"))
    val first = Frame.Array(Vector(bulk("news"), Frame.Integer(1L), bulk("sport"), Frame.Integer(0L)))
    val other = Frame.Array(Vector(bulk("news"), Frame.Integer(2L), bulk("sport"), Frame.Integer(5L)))
    assertEquals(Reply.decode(Pubsub.pubsubNumSub("news", "sport"), merge(first, other)).toEither, Right(Map("news" -> 3L, "sport" -> 5L)))
  }

  test("the NUMSUB merge admits a master that reports a channel the other does not") {
    val merge = fold(Pubsub.pubsubNumSub("news", "sport"))
    val first = Frame.Array(Vector(bulk("news"), Frame.Integer(1L)))
    val other = Frame.Array(Vector(bulk("sport"), Frame.Integer(4L), bulk("news"), Frame.Integer(2L)))
    assertEquals(Reply.decode(Pubsub.pubsubNumSub("news", "sport"), merge(first, other)).toEither, Right(Map("news" -> 3L, "sport" -> 4L)))
  }

  test("NUMSUB with a repeated channel counts each master once, as a standalone server's reply does") {
    val merge = fold(Pubsub.pubsubNumSub("news", "news"))
    val reply = Frame.Array(Vector(bulk("news"), Frame.Integer(1L), bulk("news"), Frame.Integer(1L)))
    assertEquals(Reply.decode(Pubsub.pubsubNumSub("news", "news"), reply).toEither, Right(Map("news" -> 1L)))
    assertEquals(Reply.decode(Pubsub.pubsubNumSub("news", "news"), merge(reply, reply)).toEither, Right(Map("news" -> 2L)))
  }

  test("SHARDNUMSUB sums per channel too, so a shard channel keeps its owner's count when the other masters report zero") {
    val merge = fold(Pubsub.pubsubShardNumSub("orders"))
    val owner = Frame.Array(Vector(bulk("orders"), Frame.Integer(3L)))
    val bare  = Frame.Array(Vector(bulk("orders"), Frame.Integer(0L)))
    assertEquals(Reply.decode(Pubsub.pubsubShardNumSub("orders"), merge(bare, owner)).toEither, Right(Map("orders" -> 3L)))
  }

  test("the NUMSUB merge fails on an odd-length or mistyped reply in either operand position") {
    val merge = fold(Pubsub.pubsubNumSub("news"))
    val valid = Frame.Array(Vector(bulk("news"), Frame.Integer(1L)))
    val odd   = Frame.Array(Vector(bulk("news")))
    val typed = Frame.Array(Vector(bulk("news"), bulk("1")))
    intercept[DecodeError](merge(valid, odd))
    intercept[DecodeError](merge(odd, valid))
    intercept[DecodeError](merge(valid, typed))
  }

  test("NUMPAT sums each master's pattern count, with checked overflow and malformed replies rejected") {
    val merge = fold(Pubsub.pubsubNumPat)
    assertEquals(Reply.decode(Pubsub.pubsubNumPat, merge(Frame.Integer(2L), Frame.Integer(3L))).toEither, Right(5L))
    val bad   = Frame.SimpleString("nonsense")
    intercept[DecodeError](merge(Frame.Integer(2L), bad))
    intercept[DecodeError](merge(bad, Frame.Integer(2L)))
    intercept[ArithmeticException](merge(Frame.Integer(Long.MaxValue), Frame.Integer(1L)))
  }

  test("an invalidate push whose key list does not decode flushes the cache instead of being ignored") {
    def decode(keys: Frame) = Invalidation.decode(Vector(bulk("invalidate"), keys))
    decode(Frame.Array(Vector(bulk("k")))) match {
      case Some(Invalidation.Evict(keys)) => assertEquals(keys.map(_.asUtf8String), Vector("k"))
      case other                          => fail(s"expected Evict, got $other")
    }
    assertEquals(decode(Frame.Null), Some(Invalidation.FlushAll))
    assertEquals(decode(Frame.Array(Vector(Frame.Integer(1)))), Some(Invalidation.FlushAll))
    assertEquals(decode(bulk("k")), Some(Invalidation.FlushAll))
  }
}

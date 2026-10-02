package sage.commands

import java.time.Instant

import scala.concurrent.duration.*

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame
import sage.protocol.Frames.bulk

class KeysSpec extends munit.FunSuite {

  test("TTL and PTTL decode the sentinels and a remaining duration in their unit") {
    assertEquals(Reply.decode(Keys.ttl("k"), Frame.Integer(-2)).toEither, Right(Ttl.NoKey))
    assertEquals(Reply.decode(Keys.ttl("k"), Frame.Integer(-1)).toEither, Right(Ttl.NoExpiry))
    assertEquals(Reply.decode(Keys.ttl("k"), Frame.Integer(42)).toEither, Right(Ttl.Expires(42.seconds)))
    assertEquals(Reply.decode(Keys.pTtl("k"), Frame.Integer(42)).toEither, Right(Ttl.Expires(42.millis)))
    assert(Reply.decode(Keys.ttl("k"), Frame.Integer(-3)).toEither.isLeft)
  }

  test("EXPIRETIME and PEXPIRETIME decode the sentinels and an absolute timestamp in their unit") {
    assertEquals(Reply.decode(Keys.expireTime("k"), Frame.Integer(-2)).toEither, Right(ExpiryTime.NoKey))
    assertEquals(Reply.decode(Keys.expireTime("k"), Frame.Integer(-1)).toEither, Right(ExpiryTime.NoExpiry))
    assertEquals(Reply.decode(Keys.expireTime("k"), Frame.Integer(2000000000L)).toEither, Right(ExpiryTime.At(Instant.ofEpochSecond(2000000000L))))
    assertEquals(
      Reply.decode(Keys.pExpireTime("k"), Frame.Integer(2000000000123L)).toEither,
      Right(ExpiryTime.At(Instant.ofEpochMilli(2000000000123L)))
    )
  }

  test("TYPE decodes every key type, none as None, and an unclassified type as Other") {
    val expected = Map(
      "string" -> RedisType.String,
      "list"   -> RedisType.List,
      "set"    -> RedisType.Set,
      "zset"   -> RedisType.ZSet,
      "hash"   -> RedisType.Hash,
      "stream" -> RedisType.Stream
    )
    expected.foreach { case (wire, tpe) =>
      assertEquals(Reply.decode(Keys.typeOf("k"), Frame.SimpleString(wire)).toEither, Right(Some(tpe)))
    }
    assertEquals(Reply.decode(Keys.typeOf("k"), Frame.SimpleString("none")).toEither, Right(None))
    assertEquals(Reply.decode(Keys.typeOf("k"), Frame.SimpleString("ReJSON-RL")).toEither, Right(Some(RedisType.Other("ReJSON-RL"))))
  }

  test("SCAN decodes a mid-iteration page with a next cursor") {
    val reply = Frame.Array(
      Vector(
        bulk("17"),
        Frame.Array(Vector(bulk("a"), bulk("b")))
      )
    )
    Reply.decode(Keys.scan[String](ScanCursor.start), reply).toEither match {
      case Right(page) =>
        assertEquals(page.items, Vector("a", "b"))
        assert(page.next.isDefined)
      case other       => fail(s"expected a page, got $other")
    }
  }

  test("SCAN decodes a zero cursor as iteration complete, even with keys in the page") {
    val reply = Frame.Array(Vector(bulk("0"), Frame.Array(Vector(bulk("last")))))
    Reply.decode(Keys.scan[String](ScanCursor.start), reply).toEither match {
      case Right(page) =>
        assertEquals(page.items, Vector("last"))
        assertEquals(page.next, None)
      case other       => fail(s"expected a page, got $other")
    }
  }

  test("a returned cursor feeds the next SCAN call") {
    val reply = Frame.Array(Vector(bulk("17"), Frame.Array(Vector.empty)))
    Reply.decode(Keys.scan[String](ScanCursor.start), reply).toEither match {
      case Right(ScanPage(_, Some(next))) =>
        assertEquals(Keys.scan[String](next).args.head.asUtf8String, "17")
      case other                          => fail(s"expected a next cursor, got $other")
    }
  }

  test("SCAN rejects a malformed reply shape") {
    assert(Reply.decode(Keys.scan[String](ScanCursor.start), Frame.Array(Vector(Frame.Integer(0)))).toEither.isLeft)
    assert(Reply.decode(Keys.scan[String](ScanCursor.start), Frame.Integer(0)).toEither.isLeft)
  }

  test("RANDOMKEY decodes null as None on an empty database") {
    assertEquals(Reply.decode(Keys.randomKey[String], Frame.Null).toEither, Right(None))
    assertEquals(Reply.decode(Keys.randomKey[String], bulk("k")).toEither, Right(Some("k")))
  }

  test("multi-key commands mark every position for the slot engine") {
    assertEquals(Keys.del("a", "b", "c").keyIndices, Vector(0, 1, 2))
    assertEquals(Keys.exists("a").keyIndices, Vector(0))
    assertEquals(Keys.copy("src", "dst").keyIndices, Vector(0, 1))
    assertEquals(Keys.rename("src", "dst").keyIndices, Vector(0, 1))
    assertEquals(Keys.keys[String]("*").keyIndices, Vector.empty[Int])
    assertEquals(Keys.scan[String](ScanCursor.start).keyIndices, Vector.empty[Int])
    assertEquals(Keys.randomKey[String].keyIndices, Vector.empty[Int])
  }

  test("KEYS is a concatenating all-masters read, so a cluster sweeps every master and merges the slices") {
    val command = Keys.keys[String]("*")
    assert(command.allMasters)
    assertEquals(command.broadcast, BroadcastReduce.Concat)
    assert(command.isReadOnly)
    assertEquals(command.rawFrame.allMasters, true)
    assertEquals(command.rawFrame.broadcast, BroadcastReduce.Concat)
  }

  test("KEYS concatenates per-master key arrays and rejects a master reply that is not an array instead of reporting it as a key") {
    val keys = Keys.keys[String]("*")
    assertEquals(keys.reduceReplies(Frame.Array(Vector(bulk("a"))), Vector(Frame.Set(Vector(bulk("b"))))), Frame.Array(Vector(bulk("a"), bulk("b"))))
    intercept[DecodeError](keys.reduceReplies(Frame.Array(Vector(bulk("a"))), Vector(bulk("b"))))
  }

  test("expire picks the wire command from the duration's precision") {
    assertEquals(Keys.expire("k", 90.seconds).name, "EXPIRE")
    assertEquals(Keys.expire("k", 90500.millis).name, "PEXPIRE")
    assertEquals(Keys.expireAt("k", Instant.ofEpochSecond(2000000000L)).name, "EXPIREAT")
    assertEquals(Keys.expireAt("k", Instant.ofEpochMilli(2000000000123L)).name, "PEXPIREAT")
  }

  test("a positive sub-millisecond expiry rounds up to one millisecond rather than truncating to zero") {
    assertEquals(Keys.expire("k", 500.micros).args(1).asUtf8String, "1")
    assertEquals(Keys.expire("k", 1.nano).args(1).asUtf8String, "1")
    assertEquals(Strings.set("k", "v", expiry = SetExpiry.In(500.micros)).args.last.asUtf8String, "1")
    assertEquals(Strings.getEx[String, String]("k", GetExpiry.In(500.micros)).args.last.asUtf8String, "1")
    assertEquals(Keys.expireAt("k", Instant.ofEpochSecond(2000000000L, 1)).args(1).asUtf8String, "2000000000001")
  }

  test("RESTORE expiries and the MIGRATE timeout never encode the wire value 0, which means no expiry or the default timeout") {
    assertEquals(Keys.restore("k", Bytes.utf8("p"), RestoreExpiry.In(Duration.Zero)).args(1).asUtf8String, "1")
    assertEquals(Keys.restore("k", Bytes.utf8("p"), RestoreExpiry.At(Instant.EPOCH)).args(1).asUtf8String, "1")
    assertEquals(Keys.migrate("h", 6380, 0, 500.micros)("k").args(4).asUtf8String, "1")
  }

  test("an extreme instant saturates instead of throwing while building the command") {
    val command = Keys.expireAt("k", Instant.MAX)
    assertEquals(command.name, "PEXPIREAT")
    assertEquals(command.args(1).asUtf8String, Long.MaxValue.toString)
  }
}

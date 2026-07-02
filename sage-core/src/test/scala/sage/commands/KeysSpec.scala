package sage.commands

import java.time.Instant

import scala.concurrent.duration.*

import sage.Bytes
import sage.protocol.Frame

class KeysSpec extends munit.FunSuite {

  test("TTL and PTTL decode the sentinels and a remaining duration in their unit") {
    assertEquals(Reply.run(Keys.ttl("k"), Frame.Integer(-2)), Right(Ttl.NoKey))
    assertEquals(Reply.run(Keys.ttl("k"), Frame.Integer(-1)), Right(Ttl.NoExpiry))
    assertEquals(Reply.run(Keys.ttl("k"), Frame.Integer(42)), Right(Ttl.Expires(42.seconds)))
    assertEquals(Reply.run(Keys.pTtl("k"), Frame.Integer(42)), Right(Ttl.Expires(42.millis)))
    assert(Reply.run(Keys.ttl("k"), Frame.Integer(-3)).isLeft)
  }

  test("EXPIRETIME and PEXPIRETIME decode the sentinels and an absolute timestamp in their unit") {
    assertEquals(Reply.run(Keys.expireTime("k"), Frame.Integer(-2)), Right(ExpiryTime.NoKey))
    assertEquals(Reply.run(Keys.expireTime("k"), Frame.Integer(-1)), Right(ExpiryTime.NoExpiry))
    assertEquals(Reply.run(Keys.expireTime("k"), Frame.Integer(2000000000L)), Right(ExpiryTime.At(Instant.ofEpochSecond(2000000000L))))
    assertEquals(
      Reply.run(Keys.pExpireTime("k"), Frame.Integer(2000000000123L)),
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
      assertEquals(Reply.run(Keys.typeOf("k"), Frame.SimpleString(wire)), Right(Some(tpe)))
    }
    assertEquals(Reply.run(Keys.typeOf("k"), Frame.SimpleString("none")), Right(None))
    assertEquals(Reply.run(Keys.typeOf("k"), Frame.SimpleString("ReJSON-RL")), Right(Some(RedisType.Other("ReJSON-RL"))))
  }

  test("SCAN decodes a mid-iteration page with a next cursor") {
    val reply = Frame.Array(
      Vector(
        Frame.BulkString(Bytes.utf8("17")),
        Frame.Array(Vector(Frame.BulkString(Bytes.utf8("a")), Frame.BulkString(Bytes.utf8("b"))))
      )
    )
    Reply.run(Keys.scan[String](ScanCursor.start), reply) match {
      case Right(page) =>
        assertEquals(page.items, Vector("a", "b"))
        assert(page.next.isDefined)
      case other       => fail(s"expected a page, got $other")
    }
  }

  test("SCAN decodes a zero cursor as iteration complete, even with keys in the page") {
    val reply = Frame.Array(Vector(Frame.BulkString(Bytes.utf8("0")), Frame.Array(Vector(Frame.BulkString(Bytes.utf8("last"))))))
    Reply.run(Keys.scan[String](ScanCursor.start), reply) match {
      case Right(page) =>
        assertEquals(page.items, Vector("last"))
        assertEquals(page.next, None)
      case other       => fail(s"expected a page, got $other")
    }
  }

  test("a returned cursor feeds the next SCAN call") {
    val reply = Frame.Array(Vector(Frame.BulkString(Bytes.utf8("17")), Frame.Array(Vector.empty)))
    Reply.run(Keys.scan[String](ScanCursor.start), reply) match {
      case Right(ScanPage(_, Some(next))) =>
        assertEquals(Keys.scan[String](next).args.head.asUtf8String, "17")
      case other                          => fail(s"expected a next cursor, got $other")
    }
  }

  test("SCAN rejects a malformed reply shape") {
    assert(Reply.run(Keys.scan[String](ScanCursor.start), Frame.Array(Vector(Frame.Integer(0)))).isLeft)
    assert(Reply.run(Keys.scan[String](ScanCursor.start), Frame.Integer(0)).isLeft)
  }

  test("RANDOMKEY decodes null as None on an empty database") {
    assertEquals(Reply.run(Keys.randomKey[String], Frame.Null), Right(None))
    assertEquals(Reply.run(Keys.randomKey[String], Frame.BulkString(Bytes.utf8("k"))), Right(Some("k")))
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

  test("KEYS is an aggregating all-masters read, so a cluster sweeps every master and merges the slices") {
    val command = Keys.keys[String]("*")
    assert(command.allMasters)
    assert(command.aggregate)
    assert(command.isReadOnly)
    assertEquals(command.rawFrame.allMasters, true)
    assertEquals(command.rawFrame.aggregate, true)
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
}

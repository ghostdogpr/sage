package sage.commands

import java.time.Instant

import scala.concurrent.duration.*

/**
  * One constructed Command per builder variant, paired with its expected wire words. The coverage spec derives the implemented-command
  * set from these, so a builder without a sample reports its command as missing — keep every distinct wire name reachable.
  */
object CommandSamples {

  final case class Sample(command: Command[?], wire: Vector[String])

  private val wholeSecond     = Instant.ofEpochSecond(2000000000L)
  private val withMillis      = Instant.ofEpochMilli(2000000000123L)

  val all: Vector[Sample] = Vector(
    Sample(Connection.ping(), Vector("PING")),
    Sample(Connection.ping(Some("hi")), Vector("PING", "hi")),
    Sample(Connection.hello(), Vector("HELLO", "3")),
    Sample(Connection.hello(Some(("user", "pass"))), Vector("HELLO", "3", "AUTH", "user", "pass")),
    Sample(Strings.append("k", "v"), Vector("APPEND", "k", "v")),
    Sample(Strings.decr("k"), Vector("DECR", "k")),
    Sample(Strings.decrBy("k", 2L), Vector("DECRBY", "k", "2")),
    Sample(Strings.get[String, String]("k"), Vector("GET", "k")),
    Sample(Strings.getDel[String, String]("k"), Vector("GETDEL", "k")),
    Sample(Strings.getEx[String, String]("k"), Vector("GETEX", "k")),
    Sample(Strings.getEx[String, String]("k", GetExpiry.In(90.seconds)), Vector("GETEX", "k", "EX", "90")),
    Sample(Strings.getEx[String, String]("k", GetExpiry.In(90500.millis)), Vector("GETEX", "k", "PX", "90500")),
    Sample(Strings.getEx[String, String]("k", GetExpiry.At(wholeSecond)), Vector("GETEX", "k", "EXAT", "2000000000")),
    Sample(Strings.getEx[String, String]("k", GetExpiry.At(withMillis)), Vector("GETEX", "k", "PXAT", "2000000000123")),
    Sample(Strings.getEx[String, String]("k", GetExpiry.Persist), Vector("GETEX", "k", "PERSIST")),
    Sample(Strings.getRange[String, String]("k", 0L, 4L), Vector("GETRANGE", "k", "0", "4")),
    Sample(Strings.incr("k"), Vector("INCR", "k")),
    Sample(Strings.incrBy("k", 2L), Vector("INCRBY", "k", "2")),
    Sample(Strings.incrByFloat("k", 1.5), Vector("INCRBYFLOAT", "k", "1.5")),
    Sample(Strings.mGet[String, String]("a", "b", "c"), Vector("MGET", "a", "b", "c")),
    Sample(Strings.mSet(("a", "1"), ("b", "2")), Vector("MSET", "a", "1", "b", "2")),
    Sample(Strings.mSetNx(("a", "1"), ("b", "2")), Vector("MSETNX", "a", "1", "b", "2")),
    Sample(Strings.set("k", "v"), Vector("SET", "k", "v")),
    Sample(Strings.set("k", "v", condition = SetCondition.IfNotExists), Vector("SET", "k", "v", "NX")),
    Sample(Strings.set("k", "v", condition = SetCondition.IfExists), Vector("SET", "k", "v", "XX")),
    Sample(Strings.set("k", "v", expiry = SetExpiry.KeepTtl), Vector("SET", "k", "v", "KEEPTTL")),
    Sample(Strings.set("k", "v", expiry = SetExpiry.In(90.seconds)), Vector("SET", "k", "v", "EX", "90")),
    Sample(Strings.set("k", "v", expiry = SetExpiry.In(90500.millis)), Vector("SET", "k", "v", "PX", "90500")),
    Sample(Strings.set("k", "v", expiry = SetExpiry.In(500.micros)), Vector("SET", "k", "v", "PX", "1")),
    Sample(Strings.set("k", "v", expiry = SetExpiry.At(wholeSecond)), Vector("SET", "k", "v", "EXAT", "2000000000")),
    Sample(Strings.set("k", "v", expiry = SetExpiry.At(withMillis)), Vector("SET", "k", "v", "PXAT", "2000000000123")),
    Sample(
      Strings.setGet("k", "v", expiry = SetExpiry.In(90.seconds), condition = SetCondition.IfNotExists),
      Vector("SET", "k", "v", "NX", "GET", "EX", "90")
    ),
    Sample(Strings.setRange("k", 5L, "v"), Vector("SETRANGE", "k", "5", "v")),
    Sample(Strings.strLen("k"), Vector("STRLEN", "k")),
    Sample(Keys.copy("src", "dst"), Vector("COPY", "src", "dst")),
    Sample(Keys.copy("src", "dst", replace = true), Vector("COPY", "src", "dst", "REPLACE")),
    Sample(Keys.del("a", "b"), Vector("DEL", "a", "b")),
    Sample(Keys.exists("a", "b"), Vector("EXISTS", "a", "b")),
    Sample(Keys.expire("k", 90.seconds), Vector("EXPIRE", "k", "90")),
    Sample(Keys.expire("k", 90500.millis), Vector("PEXPIRE", "k", "90500")),
    Sample(Keys.expire("k", 500.micros), Vector("PEXPIRE", "k", "1")),
    Sample(Keys.expire("k", 1500.micros), Vector("PEXPIRE", "k", "2")),
    Sample(Keys.expire("k", 90.seconds, ExpireCondition.IfNoExpiry), Vector("EXPIRE", "k", "90", "NX")),
    Sample(Keys.expire("k", 90.seconds, ExpireCondition.IfHasExpiry), Vector("EXPIRE", "k", "90", "XX")),
    Sample(Keys.expire("k", 90.seconds, ExpireCondition.IfGreater), Vector("EXPIRE", "k", "90", "GT")),
    Sample(Keys.expire("k", 90.seconds, ExpireCondition.IfLess), Vector("EXPIRE", "k", "90", "LT")),
    Sample(Keys.expireAt("k", wholeSecond), Vector("EXPIREAT", "k", "2000000000")),
    Sample(Keys.expireAt("k", withMillis), Vector("PEXPIREAT", "k", "2000000000123")),
    Sample(Keys.expireAt("k", wholeSecond.plusNanos(1)), Vector("PEXPIREAT", "k", "2000000000001")),
    Sample(Keys.expireTime("k"), Vector("EXPIRETIME", "k")),
    Sample(Keys.pExpireTime("k"), Vector("PEXPIRETIME", "k")),
    Sample(Keys.keys[String]("user:*"), Vector("KEYS", "user:*")),
    Sample(Keys.persist("k"), Vector("PERSIST", "k")),
    Sample(Keys.pTtl("k"), Vector("PTTL", "k")),
    Sample(Keys.randomKey[String], Vector("RANDOMKEY")),
    Sample(Keys.rename("src", "dst"), Vector("RENAME", "src", "dst")),
    Sample(Keys.renameNx("src", "dst"), Vector("RENAMENX", "src", "dst")),
    Sample(Keys.scan[String](ScanCursor.start), Vector("SCAN", "0")),
    Sample(
      Keys.scan[String](ScanCursor.start, pattern = Some("user:*"), count = Some(100L), ofType = Some(RedisType.Hash)),
      Vector("SCAN", "0", "MATCH", "user:*", "COUNT", "100", "TYPE", "hash")
    ),
    Sample(Keys.touch("a", "b"), Vector("TOUCH", "a", "b")),
    Sample(Keys.ttl("k"), Vector("TTL", "k")),
    Sample(Keys.typeOf("k"), Vector("TYPE", "k")),
    Sample(Keys.unlink("a", "b"), Vector("UNLINK", "a", "b"))
  )
}

package sage.commands

import java.time.Instant

import scala.concurrent.duration.*

/**
  * One constructed Command per builder variant, paired with its expected wire words. The coverage spec derives the implemented-command
  * set from these, so a builder without a sample reports its command as missing — keep every distinct wire name reachable.
  */
object CommandSamples {

  final case class Sample(command: Command[?], wire: Vector[String])

  private val wholeSecond = Instant.ofEpochSecond(2000000000L)
  private val withMillis  = Instant.ofEpochMilli(2000000000123L)

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
    Sample(Keys.unlink("a", "b"), Vector("UNLINK", "a", "b")),
    Sample(Hashes.hSet("h", ("f1", "v1"), ("f2", "v2")), Vector("HSET", "h", "f1", "v1", "f2", "v2")),
    Sample(Hashes.hSetNx("h", "f", "v"), Vector("HSETNX", "h", "f", "v")),
    Sample(Hashes.hGet[String, String, String]("h", "f"), Vector("HGET", "h", "f")),
    Sample(Hashes.hmGet[String, String, String]("h", "f1", "f2"), Vector("HMGET", "h", "f1", "f2")),
    Sample(Hashes.hDel("h", "f1", "f2"), Vector("HDEL", "h", "f1", "f2")),
    Sample(Hashes.hExists("h", "f"), Vector("HEXISTS", "h", "f")),
    Sample(Hashes.hLen("h"), Vector("HLEN", "h")),
    Sample(Hashes.hStrLen("h", "f"), Vector("HSTRLEN", "h", "f")),
    Sample(Hashes.hKeys[String, String]("h"), Vector("HKEYS", "h")),
    Sample(Hashes.hVals[String, String]("h"), Vector("HVALS", "h")),
    Sample(Hashes.hGetAll[String, String, String]("h"), Vector("HGETALL", "h")),
    Sample(Hashes.hIncrBy("h", "f", 2L), Vector("HINCRBY", "h", "f", "2")),
    Sample(Hashes.hIncrByFloat("h", "f", 1.5), Vector("HINCRBYFLOAT", "h", "f", "1.5")),
    Sample(Hashes.hRandField[String, String]("h"), Vector("HRANDFIELD", "h")),
    Sample(Hashes.hRandField[String, String]("h", 2L), Vector("HRANDFIELD", "h", "2")),
    Sample(Hashes.hRandFieldWithValues[String, String, String]("h", 2L), Vector("HRANDFIELD", "h", "2", "WITHVALUES")),
    Sample(Hashes.hScan[String, String, String]("h", ScanCursor.start), Vector("HSCAN", "h", "0")),
    Sample(
      Hashes.hScan[String, String, String]("h", ScanCursor.start, pattern = Some("f*"), count = Some(10L)),
      Vector("HSCAN", "h", "0", "MATCH", "f*", "COUNT", "10")
    ),
    Sample(Hashes.hScanNoValues[String, String]("h", ScanCursor.start), Vector("HSCAN", "h", "0", "NOVALUES")),
    Sample(Lists.lPush("l", "a", "b"), Vector("LPUSH", "l", "a", "b")),
    Sample(Lists.rPush("l", "a"), Vector("RPUSH", "l", "a")),
    Sample(Lists.lPushX("l", "a"), Vector("LPUSHX", "l", "a")),
    Sample(Lists.rPushX("l", "a"), Vector("RPUSHX", "l", "a")),
    Sample(Lists.lPop[String, String]("l"), Vector("LPOP", "l")),
    Sample(Lists.rPop[String, String]("l"), Vector("RPOP", "l")),
    Sample(Lists.lPopCount[String, String]("l", 2L), Vector("LPOP", "l", "2")),
    Sample(Lists.rPopCount[String, String]("l", 2L), Vector("RPOP", "l", "2")),
    Sample(Lists.lLen("l"), Vector("LLEN", "l")),
    Sample(Lists.lRange[String, String]("l", 0L, -1L), Vector("LRANGE", "l", "0", "-1")),
    Sample(Lists.lIndex[String, String]("l", 0L), Vector("LINDEX", "l", "0")),
    Sample(Lists.lSet("l", 0L, "v"), Vector("LSET", "l", "0", "v")),
    Sample(Lists.lInsert("l", InsertPosition.Before, "pivot", "v"), Vector("LINSERT", "l", "BEFORE", "pivot", "v")),
    Sample(Lists.lInsert("l", InsertPosition.After, "pivot", "v"), Vector("LINSERT", "l", "AFTER", "pivot", "v")),
    Sample(Lists.lRem("l", -1L, "v"), Vector("LREM", "l", "-1", "v")),
    Sample(Lists.lTrim("l", 0L, 10L), Vector("LTRIM", "l", "0", "10")),
    Sample(Lists.lPos("l", "v"), Vector("LPOS", "l", "v")),
    Sample(Lists.lPos("l", "v", rank = Some(-1L), maxLen = Some(100L)), Vector("LPOS", "l", "v", "RANK", "-1", "MAXLEN", "100")),
    Sample(Lists.lPosCount("l", "v", 0L), Vector("LPOS", "l", "v", "COUNT", "0")),
    Sample(
      Lists.lPosCount("l", "v", 2L, rank = Some(1L), maxLen = Some(50L)),
      Vector("LPOS", "l", "v", "RANK", "1", "COUNT", "2", "MAXLEN", "50")
    ),
    Sample(Lists.lMove[String, String]("src", "dst", ListSide.Left, ListSide.Right), Vector("LMOVE", "src", "dst", "LEFT", "RIGHT")),
    Sample(Lists.lMpop[String, String]("a", "b")(ListSide.Left), Vector("LMPOP", "2", "a", "b", "LEFT")),
    Sample(Lists.lMpop[String, String]("a")(ListSide.Right, count = Some(5L)), Vector("LMPOP", "1", "a", "RIGHT", "COUNT", "5"))
  )
}

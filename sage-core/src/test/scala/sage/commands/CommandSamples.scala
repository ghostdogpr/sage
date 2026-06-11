package sage.commands

import java.time.Instant

import scala.concurrent.duration.*

import sage.Bytes

/**
  * One constructed Command per builder variant, paired with its expected wire words. The coverage spec derives the implemented-command
  * set from these, so a builder without a sample reports its command as missing — keep every distinct wire name reachable.
  */
object CommandSamples {

  final case class Sample(command: Command[?], wire: Vector[String])

  private val wholeSecond = Instant.ofEpochSecond(2000000000L)
  private val withMillis  = Instant.ofEpochMilli(2000000000123L)
  private val payload     = Bytes.utf8("payload")

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
    Sample(Lists.lMpop[String, String]("a")(ListSide.Right, count = Some(5L)), Vector("LMPOP", "1", "a", "RIGHT", "COUNT", "5")),
    Sample(Lists.blPop[String, String]("a", "b")(BlockTimeout.After(1.second)), Vector("BLPOP", "a", "b", "1")),
    Sample(Lists.blPop[String, String]("a")(BlockTimeout.Forever), Vector("BLPOP", "a", "0")),
    Sample(Lists.blPop[String, String]("a")(BlockTimeout.After(Duration.Zero)), Vector("BLPOP", "a", "0.001")),
    Sample(Lists.brPop[String, String]("a")(BlockTimeout.After(1500.millis)), Vector("BRPOP", "a", "1.5")),
    Sample(
      Lists.blMove[String, String]("src", "dst", ListSide.Left, ListSide.Right, BlockTimeout.After(1.second)),
      Vector("BLMOVE", "src", "dst", "LEFT", "RIGHT", "1")
    ),
    Sample(Lists.blMpop[String, String]("a", "b")(ListSide.Left, BlockTimeout.Forever), Vector("BLMPOP", "0", "2", "a", "b", "LEFT")),
    Sample(
      Lists.blMpop[String, String]("a")(ListSide.Right, BlockTimeout.After(1.second), count = Some(5L)),
      Vector("BLMPOP", "1", "1", "a", "RIGHT", "COUNT", "5")
    ),
    Sample(Sets.sAdd("s", "a", "b"), Vector("SADD", "s", "a", "b")),
    Sample(Sets.sRem("s", "a"), Vector("SREM", "s", "a")),
    Sample(Sets.sCard("s"), Vector("SCARD", "s")),
    Sample(Sets.sIsMember("s", "a"), Vector("SISMEMBER", "s", "a")),
    Sample(Sets.sMisMember("s", "a", "b"), Vector("SMISMEMBER", "s", "a", "b")),
    Sample(Sets.sMembers[String, String]("s"), Vector("SMEMBERS", "s")),
    Sample(Sets.sMove("s", "d", "m"), Vector("SMOVE", "s", "d", "m")),
    Sample(Sets.sPop[String, String]("s"), Vector("SPOP", "s")),
    Sample(Sets.sPopCount[String, String]("s", 2L), Vector("SPOP", "s", "2")),
    Sample(Sets.sRandMember[String, String]("s"), Vector("SRANDMEMBER", "s")),
    Sample(Sets.sRandMemberCount[String, String]("s", -5L), Vector("SRANDMEMBER", "s", "-5")),
    Sample(Sets.sDiff[String, String]("a", "b"), Vector("SDIFF", "a", "b")),
    Sample(Sets.sDiffStore("d", "a", "b"), Vector("SDIFFSTORE", "d", "a", "b")),
    Sample(Sets.sInter[String, String]("a", "b"), Vector("SINTER", "a", "b")),
    Sample(Sets.sInterStore("d", "a", "b"), Vector("SINTERSTORE", "d", "a", "b")),
    Sample(Sets.sInterCard("a", "b")(), Vector("SINTERCARD", "2", "a", "b")),
    Sample(Sets.sInterCard("a", "b")(limit = Some(5L)), Vector("SINTERCARD", "2", "a", "b", "LIMIT", "5")),
    Sample(Sets.sUnion[String, String]("a", "b"), Vector("SUNION", "a", "b")),
    Sample(Sets.sUnionStore("d", "a", "b"), Vector("SUNIONSTORE", "d", "a", "b")),
    Sample(Sets.sScan[String, String]("s", ScanCursor.start), Vector("SSCAN", "s", "0")),
    Sample(
      Sets.sScan[String, String]("s", ScanCursor.start, pattern = Some("a*"), count = Some(10L)),
      Vector("SSCAN", "s", "0", "MATCH", "a*", "COUNT", "10")
    ),
    Sample(SortedSets.zAdd("z")(("a", 1.0)), Vector("ZADD", "z", "1.0", "a")),
    Sample(SortedSets.zAdd("z")(("a", 1.0), ("b", 2.5)), Vector("ZADD", "z", "1.0", "a", "2.5", "b")),
    Sample(SortedSets.zAdd("z", ZAddCondition.IfNotExists)(("a", 1.0)), Vector("ZADD", "z", "NX", "1.0", "a")),
    Sample(
      SortedSets.zAdd("z", ZAddCondition.IfExistsAndGreater, changed = true)(("a", 1.0)),
      Vector("ZADD", "z", "XX", "GT", "CH", "1.0", "a")
    ),
    Sample(SortedSets.zAddIncr("z", "a", 1.5), Vector("ZADD", "z", "INCR", "1.5", "a")),
    Sample(SortedSets.zAddIncr("z", "a", 1.5, ZAddCondition.IfExists), Vector("ZADD", "z", "XX", "INCR", "1.5", "a")),
    Sample(SortedSets.zCard("z"), Vector("ZCARD", "z")),
    Sample(SortedSets.zScore[String, String]("z", "a"), Vector("ZSCORE", "z", "a")),
    Sample(SortedSets.zMScore[String, String]("z", "a", "b"), Vector("ZMSCORE", "z", "a", "b")),
    Sample(SortedSets.zIncrBy("z", "a", 2.5), Vector("ZINCRBY", "z", "2.5", "a")),
    Sample(SortedSets.zRank[String, String]("z", "a"), Vector("ZRANK", "z", "a")),
    Sample(SortedSets.zRankWithScore[String, String]("z", "a"), Vector("ZRANK", "z", "a", "WITHSCORE")),
    Sample(SortedSets.zRevRank[String, String]("z", "a"), Vector("ZREVRANK", "z", "a")),
    Sample(SortedSets.zRevRankWithScore[String, String]("z", "a"), Vector("ZREVRANK", "z", "a", "WITHSCORE")),
    Sample(SortedSets.zCount("z", ScoreBoundary.Inclusive(1.0), ScoreBoundary.PosInf), Vector("ZCOUNT", "z", "1.0", "+inf")),
    Sample(SortedSets.zCount("z", ScoreBoundary.Exclusive(1.0), ScoreBoundary.Inclusive(5.0)), Vector("ZCOUNT", "z", "(1.0", "5.0")),
    Sample(SortedSets.zLexCount[String, String]("z", LexBoundary.Min, LexBoundary.Max), Vector("ZLEXCOUNT", "z", "-", "+")),
    Sample(
      SortedSets.zLexCount[String, String]("z", LexBoundary.Inclusive("a"), LexBoundary.Exclusive("z")),
      Vector("ZLEXCOUNT", "z", "[a", "(z")
    ),
    Sample(SortedSets.zRange[String, String]("z", ZRange.ByRank(0L, -1L)), Vector("ZRANGE", "z", "0", "-1")),
    Sample(SortedSets.zRange[String, String]("z", ZRange.ByRank(0L, -1L, rev = true)), Vector("ZRANGE", "z", "0", "-1", "REV")),
    Sample(
      SortedSets.zRange[String, String]("z", ZRange.ByScore(ScoreBoundary.NegInf, ScoreBoundary.PosInf)),
      Vector("ZRANGE", "z", "-inf", "+inf", "BYSCORE")
    ),
    Sample(
      SortedSets.zRange[String, String](
        "z",
        ZRange.ByScore(ScoreBoundary.Inclusive(1.0), ScoreBoundary.Inclusive(5.0), limit = Some(Limit(0L, 10L)), rev = true)
      ),
      Vector("ZRANGE", "z", "5.0", "1.0", "BYSCORE", "REV", "LIMIT", "0", "10")
    ),
    Sample(SortedSets.zRange[String, String]("z", ZRange.ByLex(LexBoundary.Min, LexBoundary.Max)), Vector("ZRANGE", "z", "-", "+", "BYLEX")),
    Sample(
      SortedSets.zRange[String, String]("z", ZRange.ByLex(LexBoundary.Inclusive("a"), LexBoundary.Exclusive("z"), limit = Some(Limit(0L, 5L)))),
      Vector("ZRANGE", "z", "[a", "(z", "BYLEX", "LIMIT", "0", "5")
    ),
    Sample(SortedSets.zRangeWithScores[String, String]("z", ZRange.ByRank(0L, -1L)), Vector("ZRANGE", "z", "0", "-1", "WITHSCORES")),
    Sample(
      SortedSets.zRangeStore[String, String]("d", "z", ZRange.ByScore(ScoreBoundary.NegInf, ScoreBoundary.PosInf)),
      Vector("ZRANGESTORE", "d", "z", "-inf", "+inf", "BYSCORE")
    ),
    Sample(SortedSets.zRem("z", "a", "b"), Vector("ZREM", "z", "a", "b")),
    Sample(SortedSets.zRemRangeByRank("z", 0L, 1L), Vector("ZREMRANGEBYRANK", "z", "0", "1")),
    Sample(
      SortedSets.zRemRangeByScore("z", ScoreBoundary.NegInf, ScoreBoundary.Inclusive(5.0)),
      Vector("ZREMRANGEBYSCORE", "z", "-inf", "5.0")
    ),
    Sample(SortedSets.zRemRangeByLex[String, String]("z", LexBoundary.Min, LexBoundary.Max), Vector("ZREMRANGEBYLEX", "z", "-", "+")),
    Sample(SortedSets.zPopMin[String, String]("z"), Vector("ZPOPMIN", "z")),
    Sample(SortedSets.zPopMinCount[String, String]("z", 2L), Vector("ZPOPMIN", "z", "2")),
    Sample(SortedSets.zPopMax[String, String]("z"), Vector("ZPOPMAX", "z")),
    Sample(SortedSets.zPopMaxCount[String, String]("z", 2L), Vector("ZPOPMAX", "z", "2")),
    Sample(SortedSets.zMpop[String, String]("a", "b")(MinMax.Min), Vector("ZMPOP", "2", "a", "b", "MIN")),
    Sample(SortedSets.zMpop[String, String]("a")(MinMax.Max, count = Some(3L)), Vector("ZMPOP", "1", "a", "MAX", "COUNT", "3")),
    Sample(SortedSets.bzPopMin[String, String]("a", "b")(BlockTimeout.After(1.second)), Vector("BZPOPMIN", "a", "b", "1")),
    Sample(SortedSets.bzPopMax[String, String]("a")(BlockTimeout.Forever), Vector("BZPOPMAX", "a", "0")),
    Sample(SortedSets.bzMpop[String, String]("a", "b")(MinMax.Min, BlockTimeout.After(1.second)), Vector("BZMPOP", "1", "2", "a", "b", "MIN")),
    Sample(
      SortedSets.bzMpop[String, String]("a")(MinMax.Max, BlockTimeout.After(1.second), count = Some(2L)),
      Vector("BZMPOP", "1", "1", "a", "MAX", "COUNT", "2")
    ),
    Sample(SortedSets.zRandMember[String, String]("z"), Vector("ZRANDMEMBER", "z")),
    Sample(SortedSets.zRandMemberCount[String, String]("z", -5L), Vector("ZRANDMEMBER", "z", "-5")),
    Sample(SortedSets.zRandMemberWithScores[String, String]("z", 2L), Vector("ZRANDMEMBER", "z", "2", "WITHSCORES")),
    Sample(SortedSets.zUnion[String, String]("a", "b")(), Vector("ZUNION", "2", "a", "b")),
    Sample(
      SortedSets.zUnion[String, String]("a", "b")(weights = Some(Vector(1.0, 2.0)), aggregate = Aggregate.Max),
      Vector("ZUNION", "2", "a", "b", "WEIGHTS", "1.0", "2.0", "AGGREGATE", "MAX")
    ),
    Sample(SortedSets.zUnionWithScores[String, String]("a", "b")(), Vector("ZUNION", "2", "a", "b", "WITHSCORES")),
    Sample(SortedSets.zUnionStore("d", "a", "b")(), Vector("ZUNIONSTORE", "d", "2", "a", "b")),
    Sample(SortedSets.zInter[String, String]("a", "b")(), Vector("ZINTER", "2", "a", "b")),
    Sample(SortedSets.zInterWithScores[String, String]("a", "b")(), Vector("ZINTER", "2", "a", "b", "WITHSCORES")),
    Sample(SortedSets.zInterStore("d", "a", "b")(aggregate = Aggregate.Min), Vector("ZINTERSTORE", "d", "2", "a", "b", "AGGREGATE", "MIN")),
    Sample(SortedSets.zInterCard("a", "b")(), Vector("ZINTERCARD", "2", "a", "b")),
    Sample(SortedSets.zInterCard("a", "b")(limit = Some(1L)), Vector("ZINTERCARD", "2", "a", "b", "LIMIT", "1")),
    Sample(SortedSets.zDiff[String, String]("a", "b"), Vector("ZDIFF", "2", "a", "b")),
    Sample(SortedSets.zDiffWithScores[String, String]("a", "b"), Vector("ZDIFF", "2", "a", "b", "WITHSCORES")),
    Sample(SortedSets.zDiffStore("d", "a", "b"), Vector("ZDIFFSTORE", "d", "2", "a", "b")),
    Sample(SortedSets.zScan[String, String]("z", ScanCursor.start), Vector("ZSCAN", "z", "0")),
    Sample(
      SortedSets.zScan[String, String]("z", ScanCursor.start, pattern = Some("a*"), count = Some(10L)),
      Vector("ZSCAN", "z", "0", "MATCH", "a*", "COUNT", "10")
    ),
    Sample(Pubsub.publish("chan", "msg"), Vector("PUBLISH", "chan", "msg")),
    Sample(Pubsub.sPublish("chan", "msg"), Vector("SPUBLISH", "chan", "msg")),
    Sample(Pubsub.pubsubChannels(), Vector("PUBSUB", "CHANNELS")),
    Sample(Pubsub.pubsubChannels(Some("news.*")), Vector("PUBSUB", "CHANNELS", "news.*")),
    Sample(Pubsub.pubsubShardChannels(), Vector("PUBSUB", "SHARDCHANNELS")),
    Sample(Pubsub.pubsubShardChannels(Some("news.*")), Vector("PUBSUB", "SHARDCHANNELS", "news.*")),
    Sample(Pubsub.pubsubNumSub("a", "b"), Vector("PUBSUB", "NUMSUB", "a", "b")),
    Sample(Pubsub.pubsubShardNumSub("a", "b"), Vector("PUBSUB", "SHARDNUMSUB", "a", "b")),
    Sample(Pubsub.pubsubNumPat, Vector("PUBSUB", "NUMPAT")),
    Sample(Hashes.hExpire("h", 90.seconds)("f1", "f2"), Vector("HEXPIRE", "h", "90", "FIELDS", "2", "f1", "f2")),
    Sample(Hashes.hExpire("h", 90500.millis)("f"), Vector("HPEXPIRE", "h", "90500", "FIELDS", "1", "f")),
    Sample(Hashes.hExpire("h", 90.seconds, ExpireCondition.IfGreater)("f"), Vector("HEXPIRE", "h", "90", "GT", "FIELDS", "1", "f")),
    Sample(Hashes.hExpireAt("h", wholeSecond)("f"), Vector("HEXPIREAT", "h", "2000000000", "FIELDS", "1", "f")),
    Sample(Hashes.hExpireAt("h", withMillis)("f"), Vector("HPEXPIREAT", "h", "2000000000123", "FIELDS", "1", "f")),
    Sample(Hashes.hExpireTime("h")("f"), Vector("HEXPIRETIME", "h", "FIELDS", "1", "f")),
    Sample(Hashes.hpExpireTime("h")("f"), Vector("HPEXPIRETIME", "h", "FIELDS", "1", "f")),
    Sample(Hashes.hTtl("h")("f"), Vector("HTTL", "h", "FIELDS", "1", "f")),
    Sample(Hashes.hpTtl("h")("f"), Vector("HPTTL", "h", "FIELDS", "1", "f")),
    Sample(Hashes.hPersist("h")("f"), Vector("HPERSIST", "h", "FIELDS", "1", "f")),
    Sample(Hashes.hGetDel[String, String, String]("h")("f1", "f2"), Vector("HGETDEL", "h", "FIELDS", "2", "f1", "f2")),
    Sample(Hashes.hGetEx[String, String, String]("h")("f"), Vector("HGETEX", "h", "FIELDS", "1", "f")),
    Sample(Hashes.hGetEx[String, String, String]("h", GetExpiry.In(90.seconds))("f"), Vector("HGETEX", "h", "EX", "90", "FIELDS", "1", "f")),
    Sample(Hashes.hGetEx[String, String, String]("h", GetExpiry.Persist)("f"), Vector("HGETEX", "h", "PERSIST", "FIELDS", "1", "f")),
    Sample(Hashes.hSetEx("h")(("f", "v")), Vector("HSETEX", "h", "FIELDS", "1", "f", "v")),
    Sample(
      Hashes.hSetEx("h", HSetExCondition.IfNoneExist, SetExpiry.In(90.seconds))(("f1", "v1"), ("f2", "v2")),
      Vector("HSETEX", "h", "FNX", "EX", "90", "FIELDS", "2", "f1", "v1", "f2", "v2")
    ),
    Sample(Keys.sort[String, String]("k"), Vector("SORT", "k")),
    Sample(
      Keys.sort[String, String]("k", by = Some("w_*"), limit = Some(Limit(0L, 10L)), get = Vector("d_*", "#"), order = SortOrder.Desc, alpha = true),
      Vector("SORT", "k", "BY", "w_*", "LIMIT", "0", "10", "GET", "d_*", "GET", "#", "DESC", "ALPHA")
    ),
    Sample(Keys.sortStore[String]("dst", "k"), Vector("SORT", "k", "STORE", "dst")),
    Sample(Keys.sortRo[String, String]("k", alpha = true), Vector("SORT_RO", "k", "ALPHA")),
    Sample(Keys.move("k", 1), Vector("MOVE", "k", "1")),
    Sample(Keys.dump("k"), Vector("DUMP", "k")),
    Sample(Keys.restore("k", payload), Vector("RESTORE", "k", "0", "payload")),
    Sample(
      Keys.restore("k", payload, RestoreExpiry.In(90.seconds), replace = true, idleTime = Some(5.seconds), freq = Some(10L)),
      Vector("RESTORE", "k", "90000", "payload", "REPLACE", "IDLETIME", "5", "FREQ", "10")
    ),
    Sample(Keys.restore("k", payload, RestoreExpiry.At(withMillis)), Vector("RESTORE", "k", "2000000000123", "payload", "ABSTTL")),
    Sample(Keys.migrate("localhost", 6380, 0, 5.seconds)("k"), Vector("MIGRATE", "localhost", "6380", "", "0", "5000", "KEYS", "k")),
    Sample(
      Keys.migrate("localhost", 6380, 1, 5.seconds, copy = true, replace = true, auth = MigrateAuth.UserPassword("u", "p"))("k1", "k2"),
      Vector("MIGRATE", "localhost", "6380", "", "1", "5000", "COPY", "REPLACE", "AUTH2", "u", "p", "KEYS", "k1", "k2")
    ),
    Sample(Keys.objectEncoding("k"), Vector("OBJECT", "ENCODING", "k")),
    Sample(Keys.objectRefCount("k"), Vector("OBJECT", "REFCOUNT", "k")),
    Sample(Keys.objectFreq("k"), Vector("OBJECT", "FREQ", "k")),
    Sample(Keys.objectIdleTime("k"), Vector("OBJECT", "IDLETIME", "k")),
    Sample(Strings.lcs[String, String]("k1", "k2"), Vector("LCS", "k1", "k2")),
    Sample(Strings.lcsLen("k1", "k2"), Vector("LCS", "k1", "k2", "LEN")),
    Sample(
      Strings.lcsIdx("k1", "k2", minMatchLen = Some(4L), withMatchLen = true),
      Vector("LCS", "k1", "k2", "IDX", "MINMATCHLEN", "4", "WITHMATCHLEN")
    ),
    Sample(Strings.digest("k"), Vector("DIGEST", "k")),
    Sample(Strings.delex[String, String]("k"), Vector("DELEX", "k")),
    Sample(Strings.delex("k", DelexCondition.IfEq("v")), Vector("DELEX", "k", "IFEQ", "v")),
    Sample(Strings.delex[String, String]("k", DelexCondition.IfDigestNe("abcd")), Vector("DELEX", "k", "IFDNE", "abcd")),
    Sample(Strings.msetEx()(("a", "1"), ("b", "2")), Vector("MSETEX", "2", "a", "1", "b", "2")),
    Sample(
      Strings.msetEx(condition = SetCondition.IfNotExists, expiry = SetExpiry.In(90.seconds))(("a", "1")),
      Vector("MSETEX", "1", "a", "1", "NX", "EX", "90")
    ),
    Sample(Strings.increxBy("k", 5L), Vector("INCREX", "k", "BYINT", "5")),
    Sample(
      Strings.increxBy("k", 5L, saturate = true, upperBound = Some(100L), expiry = IncrExpiry.In(90.seconds, onlyIfNoTtl = true)),
      Vector("INCREX", "k", "BYINT", "5", "SATURATE", "UBOUND", "100", "EX", "90", "ENX")
    ),
    Sample(Strings.increxByFloat("k", 1.5), Vector("INCREX", "k", "BYFLOAT", "1.5")),
    Sample(Geo.geoAdd("k")(("Palermo", GeoCoordinates(15.0, 37.5))), Vector("GEOADD", "k", "15.0", "37.5", "Palermo")),
    Sample(
      Geo.geoAdd("k", GeoAddCondition.IfNotExists, changed = true)(("m", GeoCoordinates(1.0, 2.0))),
      Vector("GEOADD", "k", "NX", "CH", "1.0", "2.0", "m")
    ),
    Sample(Geo.geoAdd("k", GeoAddCondition.IfExists)(("m", GeoCoordinates(1.0, 2.0))), Vector("GEOADD", "k", "XX", "1.0", "2.0", "m")),
    Sample(Geo.geoDist("k", "a", "b"), Vector("GEODIST", "k", "a", "b", "m")),
    Sample(Geo.geoDist("k", "a", "b", GeoUnit.Kilometers), Vector("GEODIST", "k", "a", "b", "km")),
    Sample(Geo.geoHash("k", "a", "b"), Vector("GEOHASH", "k", "a", "b")),
    Sample(Geo.geoPos("k", "a", "b"), Vector("GEOPOS", "k", "a", "b")),
    Sample(
      Geo.geoSearch("k", GeoOrigin.FromMember("m"), GeoShape.ByRadius(200.0, GeoUnit.Kilometers)),
      Vector("GEOSEARCH", "k", "FROMMEMBER", "m", "BYRADIUS", "200.0", "km")
    ),
    Sample(
      Geo.geoSearch[String, String](
        "k",
        GeoOrigin.FromLonLat(GeoCoordinates(15.0, 37.5)),
        GeoShape.ByBox(1.0, 2.0, GeoUnit.Meters),
        sort = Some(GeoSort.Asc),
        count = Some(GeoCount(10, any = true))
      ),
      Vector("GEOSEARCH", "k", "FROMLONLAT", "15.0", "37.5", "BYBOX", "1.0", "2.0", "m", "ASC", "COUNT", "10", "ANY")
    ),
    Sample(
      Geo.geoSearchWith("k", GeoOrigin.FromMember("m"), GeoShape.ByRadius(5.0, GeoUnit.Miles), withCoord = true, withDist = true, withHash = true),
      Vector("GEOSEARCH", "k", "FROMMEMBER", "m", "BYRADIUS", "5.0", "mi", "WITHCOORD", "WITHDIST", "WITHHASH")
    ),
    Sample(
      Geo.geoSearchStore("dst", "src", GeoOrigin.FromMember("m"), GeoShape.ByRadius(1.0, GeoUnit.Meters), storeDist = true),
      Vector("GEOSEARCHSTORE", "dst", "src", "FROMMEMBER", "m", "BYRADIUS", "1.0", "m", "STOREDIST")
    ),
    Sample(Bitmaps.setBit("k", 7L, true), Vector("SETBIT", "k", "7", "1")),
    Sample(Bitmaps.getBit("k", 7L), Vector("GETBIT", "k", "7")),
    Sample(Bitmaps.bitCount("k"), Vector("BITCOUNT", "k")),
    Sample(Bitmaps.bitCount("k", Some(BitRange(0L, 10L, BitUnit.Bit))), Vector("BITCOUNT", "k", "0", "10", "BIT")),
    Sample(Bitmaps.bitPos("k", true), Vector("BITPOS", "k", "1")),
    Sample(Bitmaps.bitPos("k", false, Some(BitPosRange.FromStart(2L))), Vector("BITPOS", "k", "0", "2")),
    Sample(Bitmaps.bitPos("k", true, Some(BitPosRange.Within(0L, 5L, BitUnit.Byte))), Vector("BITPOS", "k", "1", "0", "5", "BYTE")),
    Sample(Bitmaps.bitOpAnd("dst", "a", "b"), Vector("BITOP", "AND", "dst", "a", "b")),
    Sample(Bitmaps.bitOpOr("dst", "a", "b"), Vector("BITOP", "OR", "dst", "a", "b")),
    Sample(Bitmaps.bitOpXor("dst", "a", "b"), Vector("BITOP", "XOR", "dst", "a", "b")),
    Sample(Bitmaps.bitOpNot("dst", "src"), Vector("BITOP", "NOT", "dst", "src")),
    Sample(
      Bitmaps.bitField(
        "k",
        BitFieldOp.Set(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L), 255L),
        BitFieldOp.Get(BitFieldType.Signed(8), BitFieldOffset.TypeWidth(0L)),
        BitFieldOp.Overflow(BitFieldOverflow.Sat),
        BitFieldOp.IncrBy(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L), 10L)
      ),
      Vector("BITFIELD", "k", "SET", "u8", "0", "255", "GET", "i8", "#0", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "10")
    ),
    Sample(
      Bitmaps.bitFieldRo("k", BitFieldOp.Get(BitFieldType.Unsigned(8), BitFieldOffset.Absolute(0L))),
      Vector("BITFIELD_RO", "k", "GET", "u8", "0")
    ),
    Sample(HyperLogLog.pfAdd("k", "a", "b"), Vector("PFADD", "k", "a", "b")),
    Sample(HyperLogLog.pfAdd[String, String]("k"), Vector("PFADD", "k")),
    Sample(HyperLogLog.pfCount("a", "b"), Vector("PFCOUNT", "a", "b")),
    Sample(HyperLogLog.pfMerge("dst", "a", "b"), Vector("PFMERGE", "dst", "a", "b")),
    Sample(HyperLogLog.pfMerge[String]("dst"), Vector("PFMERGE", "dst")),
    Sample(Streams.xAdd("k")(("f", "v")), Vector("XADD", "k", "*", "f", "v")),
    Sample(
      Streams.xAdd(
        "k",
        XAddId.Explicit(StreamId(5L, 0L)),
        Some(Trimming.Approximate(TrimThreshold.MaxLen(1000L), Some(100L))),
        StreamDeletionPolicy.DelRef
      )(
        ("f", "v")
      ),
      Vector("XADD", "k", "DELREF", "MAXLEN", "~", "1000", "LIMIT", "100", "5-0", "f", "v")
    ),
    Sample(Streams.xAdd("k", XAddId.AutoSeq(5L))(("f", "v")), Vector("XADD", "k", "5-*", "f", "v")),
    Sample(Streams.xAddNoMkStream("k")(("f", "v")), Vector("XADD", "k", "NOMKSTREAM", "*", "f", "v")),
    Sample(Streams.xLen("k"), Vector("XLEN", "k")),
    Sample(Streams.xDel("k")(StreamId(1L, 0L), StreamId(2L, 3L)), Vector("XDEL", "k", "1-0", "2-3")),
    Sample(Streams.xTrim("k", Trimming.Exact(TrimThreshold.MaxLen(5L))), Vector("XTRIM", "k", "MAXLEN", "=", "5")),
    Sample(Streams.xTrim("k", Trimming.Approximate(TrimThreshold.MinId(StreamId(7L, 0L)))), Vector("XTRIM", "k", "MINID", "~", "7-0")),
    Sample(
      Streams.xTrim("k", Trimming.Approximate(TrimThreshold.MaxLen(1000L), Some(100L)), StreamDeletionPolicy.DelRef),
      Vector("XTRIM", "k", "MAXLEN", "~", "1000", "LIMIT", "100", "DELREF")
    ),
    Sample(Streams.xSetId("k", GroupStartId.At(StreamId(5L, 0L))), Vector("XSETID", "k", "5-0")),
    Sample(Streams.xRange[String, String, String]("k"), Vector("XRANGE", "k", "-", "+")),
    Sample(
      Streams.xRange[String, String, String]("k", StreamRangeId.Inclusive(StreamId(1L, 0L)), StreamRangeId.Exclusive(StreamId(9L, 0L)), Some(10L)),
      Vector("XRANGE", "k", "1-0", "(9-0", "COUNT", "10")
    ),
    Sample(Streams.xRevRange[String, String, String]("k"), Vector("XREVRANGE", "k", "+", "-")),
    Sample(Streams.xRead[String, String, String](("k", ReadId.New))(), Vector("XREAD", "STREAMS", "k", "$")),
    Sample(
      Streams.xRead[String, String, String](("k", ReadId.After(StreamId(0L, 0L))))(count = Some(5L), block = Some(BlockTimeout.After(1.second))),
      Vector("XREAD", "COUNT", "5", "BLOCK", "1000", "STREAMS", "k", "0-0")
    ),
    Sample(
      Streams.xReadGroup[String, String, String]("g", "c")(("k", GroupReadId.New))(),
      Vector("XREADGROUP", "GROUP", "g", "c", "STREAMS", "k", ">")
    ),
    Sample(
      Streams.xReadGroup[String, String, String]("g", "c")(("k", GroupReadId.New))(
        count = Some(5L),
        block = Some(BlockTimeout.After(1.second)),
        noAck = true
      ),
      Vector("XREADGROUP", "GROUP", "g", "c", "COUNT", "5", "BLOCK", "1000", "NOACK", "STREAMS", "k", ">")
    ),
    Sample(Streams.xAck("k", "g")(StreamId(1L, 0L)), Vector("XACK", "k", "g", "1-0")),
    Sample(Streams.xGroupCreate("k", "g"), Vector("XGROUP", "CREATE", "k", "g", "$")),
    Sample(
      Streams.xGroupCreate("k", "g", GroupStartId.At(StreamId(0L, 0L)), mkStream = true, entriesRead = Some(7L)),
      Vector("XGROUP", "CREATE", "k", "g", "0-0", "MKSTREAM", "ENTRIESREAD", "7")
    ),
    Sample(Streams.xGroupSetId("k", "g"), Vector("XGROUP", "SETID", "k", "g", "$")),
    Sample(Streams.xGroupDestroy("k", "g"), Vector("XGROUP", "DESTROY", "k", "g")),
    Sample(Streams.xGroupCreateConsumer("k", "g", "c"), Vector("XGROUP", "CREATECONSUMER", "k", "g", "c")),
    Sample(Streams.xGroupDelConsumer("k", "g", "c"), Vector("XGROUP", "DELCONSUMER", "k", "g", "c")),
    Sample(Streams.xClaim[String, String, String]("k", "g", "c", 5.seconds)(StreamId(1L, 0L))(), Vector("XCLAIM", "k", "g", "c", "5000", "1-0")),
    Sample(
      Streams.xClaim[String, String, String]("k", "g", "c", 5.seconds)(StreamId(1L, 0L))(
        idle = Some(ClaimIdle.Idle(2.seconds)),
        retryCount = Some(3L),
        force = true
      ),
      Vector("XCLAIM", "k", "g", "c", "5000", "1-0", "IDLE", "2000", "RETRYCOUNT", "3", "FORCE")
    ),
    Sample(Streams.xClaimJustId("k", "g", "c", 5.seconds)(StreamId(1L, 0L))(), Vector("XCLAIM", "k", "g", "c", "5000", "1-0", "JUSTID")),
    Sample(Streams.xAutoClaim[String, String, String]("k", "g", "c", 5.seconds), Vector("XAUTOCLAIM", "k", "g", "c", "5000", "0-0")),
    Sample(
      Streams.xAutoClaimJustId("k", "g", "c", 5.seconds, StreamId(0L, 0L), Some(50L)),
      Vector("XAUTOCLAIM", "k", "g", "c", "5000", "0-0", "COUNT", "50", "JUSTID")
    ),
    Sample(Streams.xPending("k", "g"), Vector("XPENDING", "k", "g")),
    Sample(Streams.xPendingExtended("k", "g"), Vector("XPENDING", "k", "g", "-", "+", "10")),
    Sample(
      Streams.xPendingExtended(
        "k",
        "g",
        StreamRangeId.Inclusive(StreamId(1L, 0L)),
        StreamRangeId.Exclusive(StreamId(9L, 0L)),
        5L,
        Some("c"),
        Some(1.second)
      ),
      Vector("XPENDING", "k", "g", "IDLE", "1000", "1-0", "(9-0", "5", "c")
    ),
    Sample(StreamInfo.xInfoStream[String, String, String]("k"), Vector("XINFO", "STREAM", "k")),
    Sample(StreamInfo.xInfoStreamFull[String, String, String]("k", Some(10L)), Vector("XINFO", "STREAM", "k", "FULL", "COUNT", "10")),
    Sample(StreamInfo.xInfoGroups("k"), Vector("XINFO", "GROUPS", "k")),
    Sample(StreamInfo.xInfoConsumers("k", "g"), Vector("XINFO", "CONSUMERS", "k", "g")),
    Sample(Streams.xDelEx("k")(StreamId(1L, 0L)), Vector("XDELEX", "k", "IDS", "1", "1-0")),
    Sample(
      Streams.xDelEx("k", StreamDeletionPolicy.Acked)(StreamId(1L, 0L), StreamId(2L, 0L)),
      Vector("XDELEX", "k", "ACKED", "IDS", "2", "1-0", "2-0")
    ),
    Sample(Streams.xAckDel("k", "g")(StreamId(1L, 0L)), Vector("XACKDEL", "k", "g", "IDS", "1", "1-0")),
    Sample(Streams.xNack("k", "g", NackMode.Fail)(StreamId(1L, 0L))(), Vector("XNACK", "k", "g", "FAIL", "IDS", "1", "1-0")),
    Sample(
      Streams.xNack("k", "g", NackMode.Fatal)(StreamId(1L, 0L))(retryCount = Some(2L), force = true),
      Vector("XNACK", "k", "g", "FATAL", "IDS", "1", "1-0", "RETRYCOUNT", "2", "FORCE")
    ),
    Sample(Scripting.eval("return 1"), Vector("EVAL", "return 1", "0")),
    Sample(Scripting.eval("return KEYS[1]", Seq("k")), Vector("EVAL", "return KEYS[1]", "1", "k")),
    Sample(Scripting.eval("s", Seq("k1", "k2"), Seq("a")), Vector("EVAL", "s", "2", "k1", "k2", "a")),
    Sample(Scripting.evalRo("return 1"), Vector("EVAL_RO", "return 1", "0")),
    Sample(Scripting.evalRo("s", Seq("k")), Vector("EVAL_RO", "s", "1", "k")),
    Sample(Scripting.evalSha("abc"), Vector("EVALSHA", "abc", "0")),
    Sample(Scripting.evalSha("abc", Seq("k")), Vector("EVALSHA", "abc", "1", "k")),
    Sample(Scripting.evalSha("abc", Seq("k"), Seq("a")), Vector("EVALSHA", "abc", "1", "k", "a")),
    Sample(Scripting.evalShaRo("abc"), Vector("EVALSHA_RO", "abc", "0")),
    Sample(Scripting.scriptLoad("return 1"), Vector("SCRIPT", "LOAD", "return 1")),
    Sample(Scripting.scriptExists("a", "b"), Vector("SCRIPT", "EXISTS", "a", "b")),
    Sample(Scripting.scriptFlush(), Vector("SCRIPT", "FLUSH")),
    Sample(Scripting.scriptFlush(Some(FlushMode.Async)), Vector("SCRIPT", "FLUSH", "ASYNC")),
    Sample(Scripting.scriptKill, Vector("SCRIPT", "KILL")),
    Sample(Scripting.scriptShow("abc"), Vector("SCRIPT", "SHOW", "abc")),
    Sample(Functions.fCall("f"), Vector("FCALL", "f", "0")),
    Sample(Functions.fCall("f", Seq("k")), Vector("FCALL", "f", "1", "k")),
    Sample(Functions.fCall("f", Seq("k"), Seq("a")), Vector("FCALL", "f", "1", "k", "a")),
    Sample(Functions.fCallRo("f"), Vector("FCALL_RO", "f", "0")),
    Sample(Functions.functionLoad("code"), Vector("FUNCTION", "LOAD", "code")),
    Sample(Functions.functionLoad("code", replace = true), Vector("FUNCTION", "LOAD", "REPLACE", "code")),
    Sample(Functions.functionDelete("lib"), Vector("FUNCTION", "DELETE", "lib")),
    Sample(Functions.functionFlush(), Vector("FUNCTION", "FLUSH")),
    Sample(Functions.functionFlush(Some(FlushMode.Sync)), Vector("FUNCTION", "FLUSH", "SYNC")),
    Sample(Functions.functionKill, Vector("FUNCTION", "KILL")),
    Sample(Functions.functionDump, Vector("FUNCTION", "DUMP")),
    Sample(Functions.functionRestore(payload), Vector("FUNCTION", "RESTORE", "payload")),
    Sample(Functions.functionRestore(payload, Some(RestorePolicy.Flush)), Vector("FUNCTION", "RESTORE", "payload", "FLUSH")),
    Sample(Functions.functionList(), Vector("FUNCTION", "LIST")),
    Sample(Functions.functionList(Some("lib"), withCode = true), Vector("FUNCTION", "LIST", "LIBRARYNAME", "lib", "WITHCODE")),
    Sample(Functions.functionStats, Vector("FUNCTION", "STATS")),
    Sample(Server.configGet("maxmemory"), Vector("CONFIG", "GET", "maxmemory")),
    Sample(Server.configGet("a", "b"), Vector("CONFIG", "GET", "a", "b")),
    Sample(Server.configSet(("maxmemory", "100mb")), Vector("CONFIG", "SET", "maxmemory", "100mb")),
    Sample(Server.configSet(("a", "1"), ("b", "2")), Vector("CONFIG", "SET", "a", "1", "b", "2")),
    Sample(Server.info(), Vector("INFO")),
    Sample(Server.info("server", "clients"), Vector("INFO", "server", "clients")),
    Sample(Server.dbSize, Vector("DBSIZE")),
    Sample(Server.time, Vector("TIME")),
    Sample(Server.role, Vector("ROLE")),
    Sample(Server.flushAll(), Vector("FLUSHALL")),
    Sample(Server.flushAll(Some(FlushMode.Async)), Vector("FLUSHALL", "ASYNC")),
    Sample(Server.flushDb(), Vector("FLUSHDB")),
    Sample(Server.flushDb(Some(FlushMode.Sync)), Vector("FLUSHDB", "SYNC")),
    Sample(Server.waitReplicas(2L, 1.second), Vector("WAIT", "2", "1000")),
    Sample(Server.waitAof(1L, 0L, 1.second), Vector("WAITAOF", "1", "0", "1000")),
    Sample(Server.memoryUsage("k"), Vector("MEMORY", "USAGE", "k")),
    Sample(Server.memoryUsage("k", Some(5L)), Vector("MEMORY", "USAGE", "k", "SAMPLES", "5")),
    Sample(Server.memoryPurge, Vector("MEMORY", "PURGE")),
    Sample(Server.slowLogGet(), Vector("SLOWLOG", "GET")),
    Sample(Server.slowLogGet(Some(10L)), Vector("SLOWLOG", "GET", "10")),
    Sample(Server.slowLogLen, Vector("SLOWLOG", "LEN")),
    Sample(Server.slowLogReset, Vector("SLOWLOG", "RESET")),
    Sample(Server.commandLogGet(10L, CommandLogType.Slow), Vector("COMMANDLOG", "GET", "10", "slow")),
    Sample(Server.commandLogGet(-1L, CommandLogType.LargeRequest), Vector("COMMANDLOG", "GET", "-1", "large-request")),
    Sample(Server.commandLogLen(CommandLogType.LargeReply), Vector("COMMANDLOG", "LEN", "large-reply")),
    Sample(Server.commandLogReset(CommandLogType.Slow), Vector("COMMANDLOG", "RESET", "slow")),
    Sample(Server.latencyHistory("event-loop"), Vector("LATENCY", "HISTORY", "event-loop")),
    Sample(Server.latencyLatest, Vector("LATENCY", "LATEST")),
    Sample(Server.latencyReset(), Vector("LATENCY", "RESET")),
    Sample(Server.latencyReset("e1", "e2"), Vector("LATENCY", "RESET", "e1", "e2")),
    Sample(Server.latencyHistogram(), Vector("LATENCY", "HISTOGRAM")),
    Sample(Server.latencyHistogram("get", "set"), Vector("LATENCY", "HISTOGRAM", "get", "set")),
    Sample(Server.commandCount, Vector("COMMAND", "COUNT")),
    Sample(Server.commandList(), Vector("COMMAND", "LIST")),
    Sample(Server.commandList(Some(CommandFilterBy.Module("json"))), Vector("COMMAND", "LIST", "FILTERBY", "MODULE", "json")),
    Sample(Server.commandGetKeys("SET", "k", "v"), Vector("COMMAND", "GETKEYS", "SET", "k", "v")),
    Sample(Server.commandGetKeysAndFlags("GET", "k"), Vector("COMMAND", "GETKEYSANDFLAGS", "GET", "k")),
    Sample(Server.commandInfo("get", "set"), Vector("COMMAND", "INFO", "get", "set")),
    Sample(Server.clusterInfo, Vector("CLUSTER", "INFO")),
    Sample(Server.clusterNodes, Vector("CLUSTER", "NODES")),
    Sample(Server.clusterMyId, Vector("CLUSTER", "MYID")),
    Sample(Server.clusterKeySlot("k"), Vector("CLUSTER", "KEYSLOT", "k")),
    Sample(Server.clusterCountKeysInSlot(866), Vector("CLUSTER", "COUNTKEYSINSLOT", "866")),
    Sample(Acl.aclWhoAmI, Vector("ACL", "WHOAMI")),
    Sample(Acl.aclList, Vector("ACL", "LIST")),
    Sample(Acl.aclUsers, Vector("ACL", "USERS")),
    Sample(Acl.aclCat(), Vector("ACL", "CAT")),
    Sample(Acl.aclCat(Some("read")), Vector("ACL", "CAT", "read")),
    Sample(Acl.aclGetUser("default"), Vector("ACL", "GETUSER", "default")),
    Sample(Acl.aclLog(), Vector("ACL", "LOG")),
    Sample(Acl.aclLog(Some(10L)), Vector("ACL", "LOG", "10")),
    Sample(Connection.echo("hi"), Vector("ECHO", "hi")),
    Sample(Connection.clientId, Vector("CLIENT", "ID")),
    Sample(Connection.clientGetName, Vector("CLIENT", "GETNAME")),
    Sample(Connection.clientInfo, Vector("CLIENT", "INFO")),
    Sample(Connection.clientList, Vector("CLIENT", "LIST")),
    Sample(Connection.clientGetRedir, Vector("CLIENT", "GETREDIR")),
    Sample(Keys.delIfEq("lock", "token"), Vector("DELIFEQ", "lock", "token")),
    Sample(Streams.xCfgSet("s", idmpDuration = Some(1.hour)), Vector("XCFGSET", "s", "IDMP-DURATION", "3600")),
    Sample(Streams.xCfgSet("s", idmpMaxSize = Some(100L)), Vector("XCFGSET", "s", "IDMP-MAXSIZE", "100")),
    Sample(
      Streams.xCfgSet("s", idmpDuration = Some(1.hour), idmpMaxSize = Some(100L)),
      Vector("XCFGSET", "s", "IDMP-DURATION", "3600", "IDMP-MAXSIZE", "100")
    ),
    Sample(Arrays.arSet("a", 0L, "x", "y"), Vector("ARSET", "a", "0", "x", "y")),
    Sample(Arrays.arMSet("a", 1L -> "x", 5L -> "y"), Vector("ARMSET", "a", "1", "x", "5", "y")),
    Sample(Arrays.arGet[String, String]("a", 3L), Vector("ARGET", "a", "3")),
    Sample(Arrays.arMGet[String, String]("a", 1L, 2L), Vector("ARMGET", "a", "1", "2")),
    Sample(Arrays.arLen("a"), Vector("ARLEN", "a")),
    Sample(Arrays.arCount("a"), Vector("ARCOUNT", "a")),
    Sample(Arrays.arGetRange[String, String]("a", 0L, 4L), Vector("ARGETRANGE", "a", "0", "4")),
    Sample(Arrays.arRing("a", 3L, "x", "y"), Vector("ARRING", "a", "3", "x", "y")),
    Sample(Arrays.arLastItems[String, String]("a", 2L), Vector("ARLASTITEMS", "a", "2")),
    Sample(Arrays.arLastItems[String, String]("a", 2L, rev = true), Vector("ARLASTITEMS", "a", "2", "REV")),
    Sample(Arrays.arDel("a", 1L, 2L), Vector("ARDEL", "a", "1", "2")),
    Sample(Arrays.arDelRange("a", 0L -> 1L, 4L -> 5L), Vector("ARDELRANGE", "a", "0", "1", "4", "5")),
    Sample(Arrays.arInsert("a", "x", "y"), Vector("ARINSERT", "a", "x", "y")),
    Sample(Arrays.arNext("a"), Vector("ARNEXT", "a")),
    Sample(Arrays.arSeek("a", 100L), Vector("ARSEEK", "a", "100")),
    Sample(Arrays.arScan[String, String]("a", 0L, 10L), Vector("ARSCAN", "a", "0", "10")),
    Sample(Arrays.arScan[String, String]("a", 0L, 10L, Some(5L)), Vector("ARSCAN", "a", "0", "10", "LIMIT", "5")),
    Sample(Arrays.arGrep("a", 0L, 10L)(ArMatch.Glob("x*")), Vector("ARGREP", "a", "0", "10", "GLOB", "x*")),
    Sample(
      Arrays.arGrep("a", 0L, 10L, combine = ArGrepCombine.And, limit = Some(5L), noCase = true)(ArMatch.Exact("x"), ArMatch.Re("^y")),
      Vector("ARGREP", "a", "0", "10", "EXACT", "x", "AND", "RE", "^y", "LIMIT", "5", "NOCASE")
    ),
    Sample(
      Arrays.arGrepWithValues[String, String]("a", 0L, 10L)(ArMatch.Match("x")),
      Vector("ARGREP", "a", "0", "10", "MATCH", "x", "WITHVALUES")
    ),
    Sample(Arrays.arOpSum("a", 0L, 2L), Vector("AROP", "a", "0", "2", "SUM")),
    Sample(Arrays.arOpMin("a", 0L, 2L), Vector("AROP", "a", "0", "2", "MIN")),
    Sample(Arrays.arOpMax("a", 0L, 2L), Vector("AROP", "a", "0", "2", "MAX")),
    Sample(Arrays.arOpAnd("a", 0L, 2L), Vector("AROP", "a", "0", "2", "AND")),
    Sample(Arrays.arOpOr("a", 0L, 2L), Vector("AROP", "a", "0", "2", "OR")),
    Sample(Arrays.arOpXor("a", 0L, 2L), Vector("AROP", "a", "0", "2", "XOR")),
    Sample(Arrays.arOpUsed("a", 0L, 2L), Vector("AROP", "a", "0", "2", "USED")),
    Sample(Arrays.arOpMatch("a", 0L, 2L, "v"), Vector("AROP", "a", "0", "2", "MATCH", "v")),
    Sample(Arrays.arInfo("a"), Vector("ARINFO", "a")),
    Sample(Arrays.arInfoFull("a"), Vector("ARINFO", "a", "FULL"))
  )
}

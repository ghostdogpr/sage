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
    Sample(Pubsub.pubsubChannels(), Vector("PUBSUB", "CHANNELS")),
    Sample(Pubsub.pubsubChannels(Some("news.*")), Vector("PUBSUB", "CHANNELS", "news.*")),
    Sample(Pubsub.pubsubNumSub("a", "b"), Vector("PUBSUB", "NUMSUB", "a", "b")),
    Sample(Pubsub.pubsubNumPat, Vector("PUBSUB", "NUMPAT"))
  )
}

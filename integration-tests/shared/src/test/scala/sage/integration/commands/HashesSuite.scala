package sage.integration.commands

import java.time.Instant

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.*
import sage.integration.BothServersSuite
import sage.integration.Ttls.expiresWithin

class HashesSuite extends BothServersSuite {

  clientTest("HSET writes fields, HGET and HMGET read them back, HEXISTS and HDEL remove them") { client =>
    client.hSet("hash-basic", ("f1", "v1"), ("f2", "v2")).is(2L) >>
      client.hGet[String, String]("hash-basic", "f1").is(Some("v1")) >>
      client.hmGet[String, String]("hash-basic", "f1", "missing", "f2").is(Vector(Some("v1"), None, Some("v2"))) >>
      client.hExists("hash-basic", "f1").is(true) >>
      client.hDel("hash-basic", "f1", "missing").is(1L) >>
      client.hExists("hash-basic", "f1").is(false)
  }

  clientTest("HSETNX only writes an absent field") { client =>
    client.hSetNx("hash-setnx", "f", "one").is(true) >>
      client.hSetNx("hash-setnx", "f", "two").is(false) >>
      client.hGet[String, String]("hash-setnx", "f").is(Some("one"))
  }

  clientTest("HGETALL HKEYS HVALS HLEN HSTRLEN view the whole hash") { client =>
    client.hSet("hash-view", ("a", "1"), ("b", "22")) >>
      client.hGetAll[String, String]("hash-view").is(Map("a" -> "1", "b" -> "22")) >>
      client.hKeys[String]("hash-view").map(_.toSet).is(Set("a", "b")) >>
      client.hVals[String]("hash-view").map(_.toSet).is(Set("1", "22")) >>
      client.hLen("hash-view").is(2L) >>
      client.hStrLen("hash-view", "b").is(2L)
  }

  clientTest("HINCRBY and HINCRBYFLOAT count atomically on a field") { client =>
    client.hSet("hash-incr", ("n", "10")) >>
      client.hIncrBy("hash-incr", "n", 5L).is(15L) >>
      client.hIncrByFloat("hash-incr", "n", 0.5).is(15.5)
  }

  clientTest("HRANDFIELD returns a member, a count of members, and field/value pairs") { client =>
    for {
      _     <- client.hSet("hash-rand", ("a", "1"), ("b", "2"), ("c", "3"))
      _     <- client.hRandField[String]("hash-rand").satisfies(_.exists(Set("a", "b", "c")))
      few   <- client.hRandField[String]("hash-rand", 2L)
      pairs <- client.hRandFieldWithValues[String, String]("hash-rand", -5L)
      _     <- client.hRandField[String]("hash-rand-missing").is(None)
    } yield {
      assertEquals(few.size, 2)
      assert(few.toSet.subsetOf(Set("a", "b", "c")))
      assertEquals(pairs.size, 5)
      assert(pairs.forall { case (f, v) => Map("a" -> "1", "b" -> "2", "c" -> "3").get(f).contains(v) })
    }
  }

  clientTest("HSCAN streams field/value pairs and NOVALUES streams bare fields") { client =>
    client.hSet("hash-scan", ("a", "1"), ("b", "2"), ("c", "3")) >>
      client.hScan[String, String]("hash-scan", ScanCursor.start).map(_.items.toMap).is(Map("a" -> "1", "b" -> "2", "c" -> "3")) >>
      client.hScanNoValues[String]("hash-scan", ScanCursor.start).map(_.items.toSet).is(Set("a", "b", "c"))
  }

  // Hash field expiration exists only on Redis.
  redisTest("HEXPIRE/HTTL/HPERSIST set, read, and clear per-field TTLs") { client =>
    for {
      _   <- client.hSet("hfe-ttl", ("a", "1"), ("b", "2"))
      _   <- client.hExpire("hfe-ttl", 100.seconds)("a", "missing").is(Vector(FieldExpiry.Updated, FieldExpiry.NoField))
      ttl <- client.hTtl("hfe-ttl")("a", "b", "missing")
      _   <- client.hPersist("hfe-ttl")("a", "b").is(Vector(FieldPersist.Persisted, FieldPersist.NoExpiry))
      _   <- client.hTtl("hfe-ttl")("a").is(Vector(FieldTtl.NoExpiry))
    } yield {
      assert(expiresWithin(ttl(0), 100.seconds))
      assertEquals(ttl(1), FieldTtl.NoExpiry)
      assertEquals(ttl(2), FieldTtl.NoField)
    }
  }

  redisTest("a field-TTL command on a missing key reports NoField per field, not a null") { client =>
    client.hExpire("hfe-missing", 100.seconds)("a", "b").is(Vector(FieldExpiry.NoField, FieldExpiry.NoField))
  }

  redisTest("HEXPIREAT pins an absolute deadline that HEXPIRETIME reads back") { client =>
    val at = Instant.ofEpochSecond(Instant.now().getEpochSecond + 3600)
    client.hSet("hfe-at", ("a", "1")) >>
      client.hExpireAt("hfe-at", at)("a").is(Vector(FieldExpiry.Updated)) >>
      client.hExpireTime("hfe-at")("a").is(Vector(FieldExpiryTime.At(at)))
  }

  redisTest("HPTTL and HPEXPIRETIME read the millisecond-precision TTL and absolute deadline") { client =>
    for {
      now <- client.time
      at   = Instant.ofEpochSecond(now.getEpochSecond + 3600)
      _   <- client.hSet("hfe-px", ("a", "1"))
      _   <- client.hExpireAt("hfe-px", at)("a")
      ttl <- client.hpTtl("hfe-px")("a", "missing")
      _   <- client.hpExpireTime("hfe-px")("a").is(Vector(FieldExpiryTime.At(at)))
    } yield {
      assert(expiresWithin(ttl(0), 3600.seconds))
      assertEquals(ttl(1), FieldTtl.NoField)
    }
  }

  redisTest("HGETDEL returns field values and removes them") { client =>
    client.hSet("hfe-getdel", ("a", "1"), ("b", "2")) >>
      client.hGetDel[String, String]("hfe-getdel")("a", "missing").is(Vector(Some("1"), None)) >>
      client.hGetAll[String, String]("hfe-getdel").is(Map("b" -> "2"))
  }

  redisTest("HGETEX returns field values and sets their TTL") { client =>
    client.hSet("hfe-getex", ("a", "1")) >>
      client.hGetEx[String, String]("hfe-getex", GetExpiry.In(100.seconds))("a").is(Vector(Some("1"))) >>
      client.hTtl("hfe-getex")("a").satisfies(ttl => expiresWithin(ttl(0), 100.seconds))
  }

  redisTest("HSETEX sets fields with a shared TTL and honors FNX/FXX") { client =>
    client.hSetEx("hfe-setex", SetExpiry.In(100.seconds), HSetExCondition.IfNoneExist)(("a", "1"), ("b", "2")).is(true) >>
      client.hTtl("hfe-setex")("a").satisfies(ttl => expiresWithin(ttl(0), 100.seconds)) >>
      client.hSetEx("hfe-setex", condition = HSetExCondition.IfNoneExist)(("a", "9")).is(false) >>
      client.hGet[String, String]("hfe-setex", "a").is(Some("1")) >>
      client.hSetEx("hfe-setex", SetExpiry.KeepTtl, HSetExCondition.IfAllExist)(("a", "10")).is(true) >>
      client.hGet[String, String]("hfe-setex", "a").is(Some("10"))
  }
}

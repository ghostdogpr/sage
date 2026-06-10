package sage.integration.commands

import java.time.Instant

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.*
import sage.integration.{Images, ServerSuite}

/**
  * Hash field-expiration is a Redis-only family (absent in Valkey 8.1), so it has no cross-server counterpart.
  */
class RedisHashFieldExpirySuite extends ServerSuite(Images.redis) {

  test("HEXPIRE/HTTL/HPERSIST set, read, and clear per-field TTLs") {
    withClient { client =>
      for {
        _        <- client.hSet("hfe-ttl", ("a", "1"), ("b", "2"))
        set      <- client.hExpire("hfe-ttl", 100.seconds)("a", "missing")
        ttl      <- client.hTtl("hfe-ttl")("a", "b", "missing")
        persist  <- client.hPersist("hfe-ttl")("a", "b")
        afterTtl <- client.hTtl("hfe-ttl")("a")
      } yield {
        assertEquals(set, Vector(FieldExpiry.Updated, FieldExpiry.NoField))
        assert(ttl(0) match {
          case FieldTtl.Expires(d) => d > Duration.Zero && d <= 100.seconds
          case _                   => false
        })
        assertEquals(ttl(1), FieldTtl.NoExpiry)
        assertEquals(ttl(2), FieldTtl.NoField)
        assertEquals(persist, Vector(FieldPersist.Persisted, FieldPersist.NoExpiry))
        assertEquals(afterTtl, Vector(FieldTtl.NoExpiry))
      }
    }
  }

  test("a field-TTL command on a missing key reports NoField per field, not a null") {
    withClient { client =>
      for {
        result <- client.hExpire("hfe-missing", 100.seconds)("a", "b")
      } yield assertEquals(result, Vector(FieldExpiry.NoField, FieldExpiry.NoField))
    }
  }

  test("HEXPIREAT pins an absolute deadline that HEXPIRETIME reads back") {
    withClient { client =>
      val at = Instant.ofEpochSecond(Instant.now().getEpochSecond + 3600)
      for {
        _    <- client.hSet("hfe-at", ("a", "1"))
        set  <- client.hExpireAt("hfe-at", at)("a")
        time <- client.hExpireTime("hfe-at")("a")
      } yield {
        assertEquals(set, Vector(FieldExpiry.Updated))
        assertEquals(time, Vector(FieldExpiryTime.At(at)))
      }
    }
  }

  test("HGETDEL returns field values and removes them") {
    withClient { client =>
      for {
        _    <- client.hSet("hfe-getdel", ("a", "1"), ("b", "2"))
        got  <- client.hGetDel[String, String, String]("hfe-getdel")("a", "missing")
        left <- client.hGetAll[String, String, String]("hfe-getdel")
      } yield {
        assertEquals(got, Vector(Some("1"), None))
        assertEquals(left, Map("b" -> "2"))
      }
    }
  }

  test("HGETEX returns field values and sets their TTL") {
    withClient { client =>
      for {
        _   <- client.hSet("hfe-getex", ("a", "1"))
        got <- client.hGetEx[String, String, String]("hfe-getex", GetExpiry.In(100.seconds))("a")
        ttl <- client.hTtl("hfe-getex")("a")
      } yield {
        assertEquals(got, Vector(Some("1")))
        assert(ttl(0) match {
          case FieldTtl.Expires(d) => d > Duration.Zero
          case _                   => false
        })
      }
    }
  }

  test("HSETEX sets fields with a shared TTL and honors FNX/FXX") {
    withClient { client =>
      for {
        created <- client.hSetEx("hfe-setex", HSetExCondition.IfNoneExist, SetExpiry.In(100.seconds))(("a", "1"), ("b", "2"))
        ttl     <- client.hTtl("hfe-setex")("a")
        blocked <- client.hSetEx("hfe-setex", HSetExCondition.IfNoneExist)(("a", "9"))
        aStill  <- client.hGet[String, String, String]("hfe-setex", "a")
        updated <- client.hSetEx("hfe-setex", HSetExCondition.IfAllExist, SetExpiry.KeepTtl)(("a", "10"))
        aNew    <- client.hGet[String, String, String]("hfe-setex", "a")
      } yield {
        assertEquals(created, true)
        assert(ttl(0) match {
          case FieldTtl.Expires(_) => true
          case _                   => false
        })
        assertEquals(blocked, false)
        assertEquals(aStill, Some("1"))
        assertEquals(updated, true)
        assertEquals(aNew, Some("10"))
      }
    }
  }
}

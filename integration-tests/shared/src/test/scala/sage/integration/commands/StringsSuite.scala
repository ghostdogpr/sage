package sage.integration.commands

import java.time.Instant

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.{GetExpiry, LcsMatch, MatchRange, SetCondition, SetExpiry, Ttl}
import sage.integration.{Images, ServerSuite}

abstract class StringsSuite(image: String) extends ServerSuite(image) {

  test("APPEND grows the value and reports the new length") {
    withClient { client =>
      for {
        _      <- client.set("str-append", "abc")
        length <- client.append("str-append", "def")
        value  <- client.get[String, String]("str-append")
      } yield {
        assertEquals(length, 6L)
        assertEquals(value, Some("abcdef"))
      }
    }
  }

  test("INCR DECR INCRBY DECRBY count atomically") {
    withClient { client =>
      for {
        _     <- client.set("str-counter", 10)
        up    <- client.incr("str-counter")
        upBy  <- client.incrBy("str-counter", 5L)
        down  <- client.decr("str-counter")
        total <- client.decrBy("str-counter", 5L)
      } yield {
        assertEquals(up, 11L)
        assertEquals(upBy, 16L)
        assertEquals(down, 15L)
        assertEquals(total, 10L)
      }
    }
  }

  test("INCRBYFLOAT increments with float precision") {
    withClient { client =>
      for {
        _     <- client.set("str-float", "10.5")
        value <- client.incrByFloat("str-float", 0.25)
      } yield assertEquals(value, 10.75)
    }
  }

  test("GETDEL returns the value and removes the key") {
    withClient { client =>
      for {
        _       <- client.set("str-getdel", "gone")
        value   <- client.getDel[String, String]("str-getdel")
        after   <- client.get[String, String]("str-getdel")
        missing <- client.getDel[String, String]("str-getdel-missing")
      } yield {
        assertEquals(value, Some("gone"))
        assertEquals(after, None)
        assertEquals(missing, None)
      }
    }
  }

  test("GETEX sets, keeps, and removes the ttl") {
    withClient { client =>
      for {
        _         <- client.set("str-getex", "v")
        value     <- client.getEx[String, String]("str-getex", GetExpiry.In(60.seconds))
        withTtl   <- client.ttl("str-getex")
        kept      <- client.getEx[String, String]("str-getex")
        stillTtl  <- client.ttl("str-getex")
        persisted <- client.getEx[String, String]("str-getex", GetExpiry.Persist)
        noTtl     <- client.ttl("str-getex")
      } yield {
        assertEquals(value, Some("v"))
        assert(expiresWithin(withTtl, 60.seconds))
        assertEquals(kept, Some("v"))
        assert(expiresWithin(stillTtl, 60.seconds))
        assertEquals(persisted, Some("v"))
        assertEquals(noTtl, Ttl.NoExpiry)
      }
    }
  }

  test("GETRANGE SETRANGE STRLEN address the value by offset") {
    withClient { client =>
      for {
        _      <- client.set("str-range", "Hello World")
        head   <- client.getRange[String, String]("str-range", 0L, 4L)
        length <- client.setRange("str-range", 6L, "Redis")
        value  <- client.get[String, String]("str-range")
        len    <- client.strLen("str-range")
      } yield {
        assertEquals(head, "Hello")
        assertEquals(length, 11L)
        assertEquals(value, Some("Hello Redis"))
        assertEquals(len, 11L)
      }
    }
  }

  test("MGET returns values positionally with None for missing keys") {
    withClient { client =>
      for {
        _      <- client.mSet(("str-mget-a", "1"), ("str-mget-b", "2"))
        values <- client.mGet[String, String]("str-mget-a", "str-mget-missing", "str-mget-b")
      } yield assertEquals(values, Vector(Some("1"), None, Some("2")))
    }
  }

  test("MSETNX writes all keys or none") {
    withClient { client =>
      for {
        first  <- client.mSetNx(("str-msetnx-a", "1"), ("str-msetnx-b", "2"))
        second <- client.mSetNx(("str-msetnx-b", "x"), ("str-msetnx-c", "3"))
        c      <- client.get[String, String]("str-msetnx-c")
      } yield {
        assertEquals(first, true)
        assertEquals(second, false)
        assertEquals(c, None)
      }
    }
  }

  test("SET honors the existence conditions") {
    withClient { client =>
      for {
        created   <- client.set("str-cond", "one", condition = SetCondition.IfNotExists)
        duplicate <- client.set("str-cond", "two", condition = SetCondition.IfNotExists)
        updated   <- client.set("str-cond", "three", condition = SetCondition.IfExists)
        ghost     <- client.set("str-cond-missing", "x", condition = SetCondition.IfExists)
        value     <- client.get[String, String]("str-cond")
      } yield {
        assertEquals(created, true)
        assertEquals(duplicate, false)
        assertEquals(updated, true)
        assertEquals(ghost, false)
        assertEquals(value, Some("three"))
      }
    }
  }

  test("setGet returns the previous value") {
    withClient { client =>
      for {
        before <- client.setGet[String, String]("str-setget", "one")
        after  <- client.setGet[String, String]("str-setget", "two")
        value  <- client.get[String, String]("str-setget")
      } yield {
        assertEquals(before, None)
        assertEquals(after, Some("one"))
        assertEquals(value, Some("two"))
      }
    }
  }

  test("SET expiry: In sets a ttl, KeepTtl preserves it, the default clears it") {
    withClient { client =>
      for {
        _       <- client.set("str-ttl", "v", expiry = SetExpiry.In(60.seconds))
        initial <- client.ttl("str-ttl")
        _       <- client.set("str-ttl", "v2", expiry = SetExpiry.KeepTtl)
        kept    <- client.ttl("str-ttl")
        _       <- client.set("str-ttl", "v3")
        cleared <- client.ttl("str-ttl")
      } yield {
        assert(expiresWithin(initial, 60.seconds))
        assert(expiresWithin(kept, 60.seconds))
        assertEquals(cleared, Ttl.NoExpiry)
      }
    }
  }

  test("SET expiry: At pins an absolute deadline") {
    withClient { client =>
      val deadline = Instant.ofEpochSecond(Instant.now().getEpochSecond + 3600)
      for {
        _   <- client.set("str-at", "v", expiry = SetExpiry.At(deadline))
        ttl <- client.ttl("str-at")
      } yield assert(expiresWithin(ttl, 3600.seconds))
    }
  }

  test("LCS finds the subsequence, its length, and indexed matches") {
    withClient { client =>
      for {
        _        <- client.mSet(("str-lcs-1", "ohmytext"), ("str-lcs-2", "mynewtext"))
        sequence <- client.lcs[String, String]("str-lcs-1", "str-lcs-2")
        length   <- client.lcsLen("str-lcs-1", "str-lcs-2")
        idx      <- client.lcsIdx("str-lcs-1", "str-lcs-2", minMatchLen = Some(4L), withMatchLen = true)
      } yield {
        assertEquals(sequence, "mytext")
        assertEquals(length, 6L)
        assertEquals(idx.length, 6L)
        assertEquals(idx.matches, Vector(LcsMatch(MatchRange(4L, 7L), MatchRange(5L, 8L), Some(4L))))
      }
    }
  }

  private def expiresWithin(ttl: Ttl, bound: FiniteDuration): Boolean =
    ttl match {
      case Ttl.Expires(remaining) => remaining > Duration.Zero && remaining <= bound
      case _                      => false
    }
}

class RedisStringsSuite extends StringsSuite(Images.redis)

class ValkeyStringsSuite extends StringsSuite(Images.valkey)

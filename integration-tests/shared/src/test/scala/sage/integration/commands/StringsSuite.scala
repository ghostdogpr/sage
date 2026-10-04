package sage.integration.commands

import java.time.Instant

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.*
import sage.integration.BothServersSuite
import sage.integration.Ttls.expiresWithin

class StringsSuite extends BothServersSuite {

  clientTest("APPEND grows the value and reports the new length") { client =>
    client.set("str-append", "abc") >>
      client.append("str-append", "def").is(6L) >>
      client.get[String]("str-append").is(Some("abcdef"))
  }

  clientTest("INCR DECR INCRBY DECRBY count atomically") { client =>
    client.set("str-counter", 10) >>
      client.incr("str-counter").is(11L) >>
      client.incrBy("str-counter", 5L).is(16L) >>
      client.decr("str-counter").is(15L) >>
      client.decrBy("str-counter", 5L).is(10L)
  }

  clientTest("INCRBYFLOAT increments with float precision") { client =>
    client.set("str-float", "10.5") >>
      client.incrByFloat("str-float", 0.25).is(10.75)
  }

  clientTest("GETDEL returns the value and removes the key") { client =>
    client.set("str-getdel", "gone") >>
      client.getDel[String]("str-getdel").is(Some("gone")) >>
      client.get[String]("str-getdel").is(None) >>
      client.getDel[String]("str-getdel-missing").is(None)
  }

  clientTest("GETEX sets, keeps, and removes the ttl") { client =>
    client.set("str-getex", "v") >>
      client.getEx[String]("str-getex", GetExpiry.In(60.seconds)).is(Some("v")) >>
      client.ttl("str-getex").satisfies(expiresWithin(_, 60.seconds)) >>
      client.getEx[String]("str-getex").is(Some("v")) >>
      client.ttl("str-getex").satisfies(expiresWithin(_, 60.seconds)) >>
      client.getEx[String]("str-getex", GetExpiry.Persist).is(Some("v")) >>
      client.ttl("str-getex").is(Ttl.NoExpiry)
  }

  clientTest("GETRANGE SETRANGE STRLEN address the value by offset") { client =>
    client.set("str-range", "Hello World") >>
      client.getRange[String]("str-range", 0L, 4L).is("Hello") >>
      client.setRange("str-range", 6L, "Redis").is(11L) >>
      client.get[String]("str-range").is(Some("Hello Redis")) >>
      client.strLen("str-range").is(11L)
  }

  clientTest("MGET returns values positionally with None for missing keys") { client =>
    client.mSet(("str-mget-a", "1"), ("str-mget-b", "2")) >>
      client.mGet[String]("str-mget-a", "str-mget-missing", "str-mget-b").is(Vector(Some("1"), None, Some("2")))
  }

  clientTest("MSETNX writes all keys or none") { client =>
    client.mSetNx(("str-msetnx-a", "1"), ("str-msetnx-b", "2")).is(true) >>
      client.mSetNx(("str-msetnx-b", "x"), ("str-msetnx-c", "3")).is(false) >>
      client.get[String]("str-msetnx-c").is(None)
  }

  clientTest("SET honors the existence conditions") { client =>
    client.set("str-cond", "one", condition = SetCondition.IfNotExists).is(true) >>
      client.set("str-cond", "two", condition = SetCondition.IfNotExists).is(false) >>
      client.set("str-cond", "three", condition = SetCondition.IfExists).is(true) >>
      client.set("str-cond-missing", "x", condition = SetCondition.IfExists).is(false) >>
      client.get[String]("str-cond").is(Some("three"))
  }

  clientTest("setGet returns the previous value") { client =>
    client.setGet[String]("str-setget", "one").is(None) >>
      client.setGet[String]("str-setget", "two").is(Some("one")) >>
      client.get[String]("str-setget").is(Some("two"))
  }

  clientTest("SET expiry: In sets a ttl, KeepTtl preserves it, the default clears it") { client =>
    client.set("str-ttl", "v", expiry = SetExpiry.In(60.seconds)) >>
      client.ttl("str-ttl").satisfies(expiresWithin(_, 60.seconds)) >>
      client.set("str-ttl", "v2", expiry = SetExpiry.KeepTtl) >>
      client.ttl("str-ttl").satisfies(expiresWithin(_, 60.seconds)) >>
      client.set("str-ttl", "v3") >>
      client.ttl("str-ttl").is(Ttl.NoExpiry)
  }

  clientTest("SET expiry: At pins an absolute deadline") { client =>
    val deadline = Instant.ofEpochSecond(Instant.now().getEpochSecond + 3600)
    client.set("str-at", "v", expiry = SetExpiry.At(deadline)) >>
      client.ttl("str-at").satisfies(expiresWithin(_, 3600.seconds))
  }

  clientTest("LCS finds the subsequence, its length, and indexed matches") { client =>
    for {
      _   <- client.mSet(("str-lcs-1", "ohmytext"), ("str-lcs-2", "mynewtext"))
      _   <- client.lcs[String]("str-lcs-1", "str-lcs-2").is("mytext")
      _   <- client.lcsLen("str-lcs-1", "str-lcs-2").is(6L)
      idx <- client.lcsIdx("str-lcs-1", "str-lcs-2", minMatchLen = Some(4L), withMatchLen = true)
    } yield {
      assertEquals(idx.length, 6L)
      assertEquals(idx.matches, Vector(LcsMatch(MatchRange(4L, 7L), MatchRange(5L, 8L), Some(4L))))
    }
  }

  // DIGEST, DELEX, MSETEX and INCREX exist only on Redis.
  redisTest("DIGEST returns a stable hex digest, None for a missing key") { client =>
    for {
      _  <- client.set("sx-digest", "hello")
      d1 <- client.digest("sx-digest")
      _  <- client.digest("sx-digest").is(d1)
      _  <- client.digest("sx-digest-missing").is(None)
    } yield assert(d1.exists(_.nonEmpty))
  }

  redisTest("DELEX deletes only when the value or digest condition matches") { client =>
    for {
      _      <- client.set("sx-delex", "v1")
      _      <- client.delex("sx-delex", DelexCondition.IfEq("other")).is(false)
      _      <- client.exists("sx-delex").is(1L)
      digest <- client.digest("sx-delex").flatMap(required("DIGEST", _))
      _      <- client.delex[String]("sx-delex", DelexCondition.IfDigestNe(digest)).is(false)
      _      <- client.delex("sx-delex", DelexCondition.IfEq("v1")).is(true)
      _      <- client.exists("sx-delex").is(0L)
    } yield ()
  }

  redisTest("MSETEX sets multiple keys with a shared TTL and respects NX") { client =>
    client.msetEx(expiry = SetExpiry.In(100.seconds))(("sx-ms-a", "1"), ("sx-ms-b", "2")).is(true) >>
      client.get[String]("sx-ms-a").is(Some("1")) >>
      client.ttl("sx-ms-a").satisfies(expiresWithin(_, 100.seconds)) >>
      client.msetEx(condition = SetCondition.IfNotExists)(("sx-ms-a", "9")).is(false) >>
      client.get[String]("sx-ms-a").is(Some("1"))
  }

  redisTest("INCREX increments with expiry, saturating bounds, and rejects when out of range") { client =>
    client.increxBy("sx-incr", 5L, expiry = IncrExpiry.In(100.seconds)).is(IncrExResult(5L, 5L)) >>
      client.ttl("sx-incr").satisfies(expiresWithin(_, 100.seconds)) >>
      client.increxBy("sx-incr", 100L, saturate = true, upperBound = Some(10L)).is(IncrExResult(10L, 5L)) >>
      client.increxBy("sx-incr", 100L, upperBound = Some(10L)).is(IncrExResult(10L, 0L)) >>
      client.increxByFloat("sx-incr-f", 1.5).is(IncrExResult(1.5, 1.5))
  }
}

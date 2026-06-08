package sage.integration.commands

import java.time.Instant

import scala.concurrent.duration.*

import kyo.compat.*

import sage.client.internal.Client
import sage.commands.*
import sage.integration.{Images, ServerSuite}

abstract class KeysSuite(image: String) extends ServerSuite(image) {

  test("COPY copies and only overwrites with replace") {
    withClient { client =>
      for {
        _         <- client.set("keys-copy-src", "v1")
        _         <- client.set("keys-copy-taken", "v2")
        fresh     <- client.copy("keys-copy-src", "keys-copy-dst")
        ontoTaken <- client.copy("keys-copy-src", "keys-copy-taken")
        replaced  <- client.copy("keys-copy-src", "keys-copy-taken", replace = true)
        copied    <- client.get[String, String]("keys-copy-dst")
      } yield {
        assertEquals(fresh, true)
        assertEquals(ontoTaken, false)
        assertEquals(replaced, true)
        assertEquals(copied, Some("v1"))
      }
    }
  }

  test("EXISTS TOUCH DEL UNLINK count the keys they hit") {
    withClient { client =>
      for {
        _       <- client.mSet(("keys-cnt-a", "1"), ("keys-cnt-b", "2"), ("keys-cnt-c", "3"))
        present <- client.exists("keys-cnt-a", "keys-cnt-b", "keys-cnt-c", "keys-cnt-missing")
        touched <- client.touch("keys-cnt-a", "keys-cnt-b")
        deleted <- client.del("keys-cnt-a", "keys-cnt-b")
        removed <- client.unlink("keys-cnt-c", "keys-cnt-missing")
      } yield {
        assertEquals(present, 3L)
        assertEquals(touched, 2L)
        assertEquals(deleted, 2L)
        assertEquals(removed, 1L)
      }
    }
  }

  test("EXPIRE sets a ttl and PERSIST clears it") {
    withClient { client =>
      for {
        _         <- client.set("keys-expire", "v")
        applied   <- client.expire("keys-expire", 60.seconds)
        ttl       <- client.ttl("keys-expire")
        persisted <- client.persist("keys-expire")
        cleared   <- client.ttl("keys-expire")
        missing   <- client.expire("keys-expire-missing", 60.seconds)
      } yield {
        assertEquals(applied, true)
        assert(remaining(ttl).exists(r => r > Duration.Zero && r <= 60.seconds))
        assertEquals(persisted, true)
        assertEquals(cleared, Ttl.NoExpiry)
        assertEquals(missing, false)
      }
    }
  }

  test("a sub-second duration takes the millisecond path end to end") {
    withClient { client =>
      for {
        _   <- client.set("keys-pexpire", "v")
        _   <- client.expire("keys-pexpire", 90500.millis)
        ttl <- client.pTtl("keys-pexpire")
      } yield assert(remaining(ttl).exists(r => r > 89.seconds && r <= 90500.millis))
    }
  }

  test("EXPIRE conditions guard against the current ttl") {
    withClient { client =>
      for {
        _          <- client.set("keys-cond", "v")
        noExpiry   <- client.expire("keys-cond", 60.seconds, ExpireCondition.IfNoExpiry)
        notLonger  <- client.expire("keys-cond", 30.seconds, ExpireCondition.IfGreater)
        longer     <- client.expire("keys-cond", 120.seconds, ExpireCondition.IfGreater)
        shorter    <- client.expire("keys-cond", 60.seconds, ExpireCondition.IfLess)
        hasExpiry  <- client.expire("keys-cond", 90.seconds, ExpireCondition.IfHasExpiry)
        notWithout <- client.expire("keys-cond", 30.seconds, ExpireCondition.IfNoExpiry)
      } yield {
        assertEquals(noExpiry, true)
        assertEquals(notLonger, false)
        assertEquals(longer, true)
        assertEquals(shorter, true)
        assertEquals(hasExpiry, true)
        assertEquals(notWithout, false)
      }
    }
  }

  test("EXPIREAT and EXPIRETIME round-trip an absolute deadline") {
    withClient { client =>
      val deadline = Instant.ofEpochSecond(Instant.now().getEpochSecond + 3600)
      for {
        _        <- client.set("keys-at", "v")
        applied  <- client.expireAt("keys-at", deadline)
        seconds  <- client.expireTime("keys-at")
        millis   <- client.pExpireTime("keys-at")
        _        <- client.set("keys-at-plain", "v")
        noExpiry <- client.expireTime("keys-at-plain")
        noKey    <- client.expireTime("keys-at-missing")
      } yield {
        assertEquals(applied, true)
        assertEquals(seconds, ExpiryTime.At(deadline))
        assertEquals(millis, ExpiryTime.At(deadline))
        assertEquals(noExpiry, ExpiryTime.NoExpiry)
        assertEquals(noKey, ExpiryTime.NoKey)
      }
    }
  }

  test("TTL distinguishes a missing key from a key without expiry") {
    withClient { client =>
      for {
        noKey    <- client.ttl("keys-ttl-missing")
        _        <- client.set("keys-ttl-plain", "v")
        noExpiry <- client.pTtl("keys-ttl-plain")
      } yield {
        assertEquals(noKey, Ttl.NoKey)
        assertEquals(noExpiry, Ttl.NoExpiry)
      }
    }
  }

  test("KEYS returns the keys matching a pattern") {
    withClient { client =>
      for {
        _       <- client.mSet(("keys-glob:1", "a"), ("keys-glob:2", "b"), ("keys-other", "c"))
        matched <- client.keys[String]("keys-glob:*")
      } yield assertEquals(matched.toSet, Set("keys-glob:1", "keys-glob:2"))
    }
  }

  test("RANDOMKEY returns a key once data exists") {
    withClient { client =>
      for {
        _      <- client.set("keys-random", "v")
        random <- client.randomKey[String]
      } yield assert(random.isDefined)
    }
  }

  test("RENAME moves a key and RENAMENX refuses an occupied destination") {
    withClient { client =>
      for {
        _        <- client.set("keys-ren-a", "v")
        _        <- client.set("keys-ren-taken", "w")
        _        <- client.rename("keys-ren-a", "keys-ren-b")
        moved    <- client.get[String, String]("keys-ren-b")
        refused  <- client.renameNx("keys-ren-b", "keys-ren-taken")
        accepted <- client.renameNx("keys-ren-b", "keys-ren-c")
      } yield {
        assertEquals(moved, Some("v"))
        assertEquals(refused, false)
        assertEquals(accepted, true)
      }
    }
  }

  test("TYPE reports the key's type and None for a missing key") {
    withClient { client =>
      for {
        _       <- client.set("keys-type-str", "v")
        _       <- client.lPush("keys-type-list", "v")
        str     <- client.typeOf("keys-type-str")
        list    <- client.typeOf("keys-type-list")
        missing <- client.typeOf("keys-type-missing")
      } yield {
        assertEquals(str, Some(RedisType.String))
        assertEquals(list, Some(RedisType.List))
        assertEquals(missing, None)
      }
    }
  }

  test("SCAN visits every key, terminating on the zero cursor rather than an empty page") {
    withClient { client =>
      val pairs = (1 to 100).map(i => (s"keys-scan:$i", "v")).toVector
      for {
        _     <- client.mSet(pairs.head, pairs.tail*)
        found <- scanAll(client, pattern = Some("keys-scan:*"), count = Some(10L), ofType = None)
      } yield assertEquals(found, pairs.map(_._1).toSet)
    }
  }

  test("SCAN filters by type") {
    withClient { client =>
      for {
        _     <- client.set("keys-scant-str", "v")
        _     <- client.lPush("keys-scant-list", "v")
        found <- scanAll(client, pattern = Some("keys-scant-*"), count = None, ofType = Some(RedisType.List))
      } yield assertEquals(found, Set("keys-scant-list"))
    }
  }

  private def scanAll(
    client: Client[CIO],
    pattern: Option[String],
    count: Option[Long],
    ofType: Option[RedisType]
  ): CIO[Set[String]] = {
    def loop(cursor: ScanCursor, found: Set[String]): CIO[Set[String]] =
      client.scan[String](cursor, pattern, count, ofType).flatMap { page =>
        val collected = found ++ page.items
        page.next match {
          case Some(next) => loop(next, collected)
          case None       => CIO.value(collected)
        }
      }
    loop(ScanCursor.start, Set.empty)
  }

  private def remaining(ttl: Ttl): Option[FiniteDuration] =
    ttl match {
      case Ttl.Expires(value) => Some(value)
      case _                  => None
    }
}

class RedisKeysSuite extends KeysSuite(Images.redis)

class ValkeyKeysSuite extends KeysSuite(Images.valkey)

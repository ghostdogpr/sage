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

  test("SORT orders a list numerically and alphabetically, with LIMIT and DESC") {
    withClient { client =>
      for {
        _       <- client.rPush("keys-sort", "3", "1", "2")
        numeric <- client.sort[String, String]("keys-sort")
        desc    <- client.sort[String, String]("keys-sort", order = SortOrder.Desc, limit = Some(Limit(0L, 2L)))
        alpha   <- client.sort[String, String]("keys-sort", alpha = true, order = SortOrder.Desc)
      } yield {
        assertEquals(numeric, Vector(Some("1"), Some("2"), Some("3")))
        assertEquals(desc, Vector(Some("3"), Some("2")))
        assertEquals(alpha, Vector(Some("3"), Some("2"), Some("1")))
      }
    }
  }

  test("SORT BY/GET sorts by external weights and projects external values, nil for missing") {
    withClient { client =>
      for {
        _   <- client.rPush("keys-sortby", "1", "2", "3")
        _   <- client.mSet(("keys-w-1", "30"), ("keys-w-2", "10"), ("keys-w-3", "20"))
        _   <- client.mSet(("keys-d-1", "A"), ("keys-d-3", "C"))
        got <- client.sort[String, String]("keys-sortby", by = Some("keys-w-*"), get = Vector("keys-d-*", "#"))
      } yield assertEquals(got, Vector(None, Some("2"), Some("C"), Some("3"), Some("A"), Some("1")))
    }
  }

  test("SORT_RO reads without storing; SORT STORE writes the result and returns the count") {
    withClient { client =>
      for {
        _      <- client.rPush("keys-sortstore", "b", "a", "c")
        ro     <- client.sortRo[String, String]("keys-sortstore", alpha = true)
        stored <- client.sortStore("keys-sortstore-dst", "keys-sortstore", alpha = true)
        dst    <- client.lRange[String, String]("keys-sortstore-dst", 0L, -1L)
      } yield {
        assertEquals(ro, Vector(Some("a"), Some("b"), Some("c")))
        assertEquals(stored, 3L)
        assertEquals(dst, Vector("a", "b", "c"))
      }
    }
  }

  test("MOVE relocates a key out of the current database") {
    withClient { client =>
      for {
        _     <- client.set("keys-move", "v")
        moved <- client.move("keys-move", 1)
        gone  <- client.exists("keys-move")
        again <- client.move("keys-move", 1)
      } yield {
        assertEquals(moved, true)
        assertEquals(gone, 0L)
        assertEquals(again, false)
      }
    }
  }

  test("DUMP and RESTORE round-trip a value through its serialized form") {
    withClient { client =>
      for {
        _        <- client.set("keys-dump", "payload")
        dumped   <- client.dump("keys-dump")
        restored <- dumped.fold(CIO.value(()))(bytes => client.restore("keys-restore", bytes))
        value    <- client.get[String, String]("keys-restore")
        replaced <- dumped.fold(CIO.value(false))(bytes => client.restore("keys-restore", bytes, replace = true).map(_ => true))
        missing  <- client.dump("keys-dump-missing")
      } yield {
        assert(dumped.isDefined)
        assertEquals(value, Some("payload"))
        assertEquals(replaced, true)
        assertEquals(missing, None)
      }
    }
  }

  test("MIGRATE reports NOKEY when the source key is absent") {
    withClient { client =>
      for {
        result <- client.migrate("localhost", 6379, 0, 1.second)("keys-migrate-ghost")
      } yield assertEquals(result, MigrateResult.NoKey)
    }
  }

  test("OBJECT exposes encoding, refcount, and idle time; None for a missing key") {
    withClient { client =>
      for {
        _        <- client.set("keys-object", "12345")
        encoding <- client.objectEncoding("keys-object")
        refCount <- client.objectRefCount("keys-object")
        idle     <- client.objectIdleTime("keys-object")
        missEnc  <- client.objectEncoding("keys-object-missing")
        missRef  <- client.objectRefCount("keys-object-missing")
        missIdle <- client.objectIdleTime("keys-object-missing")
      } yield {
        assertEquals(encoding, Some("int"))
        assert(refCount.exists(_ >= 1L))
        assert(idle.exists(_ >= Duration.Zero))
        assertEquals(missEnc, None)
        assertEquals(missRef, None)
        assertEquals(missIdle, None)
      }
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

package sage.integration.commands

import java.time.Instant

import scala.concurrent.duration.*

import kyo.compat.*

import sage.client.internal.Paged
import sage.commands.*
import sage.integration.BothServersSuite
import sage.integration.Ttls.expiresWithin

class KeysSuite extends BothServersSuite {

  clientTest("COPY copies and only overwrites with replace") { client =>
    client.set("keys-copy-src", "v1") >>
      client.set("keys-copy-taken", "v2") >>
      client.copy("keys-copy-src", "keys-copy-dst").is(true) >>
      client.copy("keys-copy-src", "keys-copy-taken").is(false) >>
      client.copy("keys-copy-src", "keys-copy-taken", replace = true).is(true) >>
      client.get[String]("keys-copy-dst").is(Some("v1"))
  }

  clientTest("EXISTS TOUCH DEL UNLINK count the keys they hit") { client =>
    client.mSet(("keys-cnt-a", "1"), ("keys-cnt-b", "2"), ("keys-cnt-c", "3")) >>
      client.exists("keys-cnt-a", "keys-cnt-b", "keys-cnt-c", "keys-cnt-missing").is(3L) >>
      client.touch("keys-cnt-a", "keys-cnt-b").is(2L) >>
      client.del("keys-cnt-a", "keys-cnt-b").is(2L) >>
      client.unlink("keys-cnt-c", "keys-cnt-missing").is(1L)
  }

  clientTest("EXPIRE sets a ttl and PERSIST clears it") { client =>
    client.set("keys-expire", "v") >>
      client.expire("keys-expire", 60.seconds).is(true) >>
      client.ttl("keys-expire").satisfies(expiresWithin(_, 60.seconds)) >>
      client.persist("keys-expire").is(true) >>
      client.ttl("keys-expire").is(Ttl.NoExpiry) >>
      client.expire("keys-expire-missing", 60.seconds).is(false)
  }

  clientTest("a sub-second duration takes the millisecond path end to end") { client =>
    client.set("keys-pexpire", "v") >>
      client.expire("keys-pexpire", 90500.millis) >>
      client.pTtl("keys-pexpire").satisfies(expiresWithin(_, 90500.millis, above = 89.seconds))
  }

  clientTest("EXPIRE conditions guard against the current ttl") { client =>
    client.set("keys-cond", "v") >>
      client.expire("keys-cond", 60.seconds, ExpireCondition.IfNoExpiry).is(true) >>
      client.expire("keys-cond", 30.seconds, ExpireCondition.IfGreater).is(false) >>
      client.expire("keys-cond", 120.seconds, ExpireCondition.IfGreater).is(true) >>
      client.expire("keys-cond", 60.seconds, ExpireCondition.IfLess).is(true) >>
      client.expire("keys-cond", 90.seconds, ExpireCondition.IfHasExpiry).is(true) >>
      client.expire("keys-cond", 30.seconds, ExpireCondition.IfNoExpiry).is(false)
  }

  clientTest("EXPIREAT and EXPIRETIME round-trip an absolute deadline") { client =>
    val deadline = Instant.ofEpochSecond(Instant.now().getEpochSecond + 3600)
    client.set("keys-at", "v") >>
      client.expireAt("keys-at", deadline).is(true) >>
      client.expireTime("keys-at").is(ExpiryTime.At(deadline)) >>
      client.pExpireTime("keys-at").is(ExpiryTime.At(deadline)) >>
      client.set("keys-at-plain", "v") >>
      client.expireTime("keys-at-plain").is(ExpiryTime.NoExpiry) >>
      client.expireTime("keys-at-missing").is(ExpiryTime.NoKey)
  }

  clientTest("TTL distinguishes a missing key from a key without expiry") { client =>
    client.ttl("keys-ttl-missing").is(Ttl.NoKey) >>
      client.set("keys-ttl-plain", "v") >>
      client.pTtl("keys-ttl-plain").is(Ttl.NoExpiry)
  }

  clientTest("KEYS returns the keys matching a pattern") { client =>
    client.mSet(("keys-glob:1", "a"), ("keys-glob:2", "b"), ("keys-other", "c")) >>
      client.keys("keys-glob:*").map(_.toSet).is(Set("keys-glob:1", "keys-glob:2"))
  }

  clientTest("RANDOMKEY returns a key once data exists") { client =>
    client.set("keys-random", "v") >>
      client.randomKey.satisfies(_.isDefined)
  }

  clientTest("RENAME moves a key and RENAMENX refuses an occupied destination") { client =>
    client.set("keys-ren-a", "v") >>
      client.set("keys-ren-taken", "w") >>
      client.rename("keys-ren-a", "keys-ren-b") >>
      client.get[String]("keys-ren-b").is(Some("v")) >>
      client.renameNx("keys-ren-b", "keys-ren-taken").is(false) >>
      client.renameNx("keys-ren-b", "keys-ren-c").is(true)
  }

  clientTest("TYPE reports the key's type and None for a missing key") { client =>
    client.set("keys-type-str", "v") >>
      client.lPush("keys-type-list", "v") >>
      client.typeOf("keys-type-str").is(Some(RedisType.String)) >>
      client.typeOf("keys-type-list").is(Some(RedisType.List)) >>
      client.typeOf("keys-type-missing").is(None)
  }

  clientTest("SCAN visits every key, terminating on the zero cursor rather than an empty page") { client =>
    val first = ("keys-scan:0", "v")
    val rest  = (1 to 99).map(i => (s"keys-scan:$i", "v"))
    client.mSet(first, rest*) >>
      drain(Paged.scanAll[String](client.runner, Some("keys-scan:*"), Some(10L), None)).is((first +: rest).map(_._1).toSet)
  }

  clientTest("SCAN filters by type") { client =>
    client.set("keys-scant-str", "v") >>
      client.lPush("keys-scant-list", "v") >>
      drain(Paged.scanAll[String](client.runner, Some("keys-scant-*"), None, Some(RedisType.List))).is(Set("keys-scant-list"))
  }

  clientTest("SORT orders a list numerically and alphabetically, with LIMIT and DESC") { client =>
    client.rPush("keys-sort", "3", "1", "2") >>
      client.sort[String]("keys-sort").is(Vector(Some("1"), Some("2"), Some("3"))) >>
      client.sort[String]("keys-sort", order = SortOrder.Desc, limit = Some(Limit(0L, 2L))).is(Vector(Some("3"), Some("2"))) >>
      client.sort[String]("keys-sort", alpha = true, order = SortOrder.Desc).is(Vector(Some("3"), Some("2"), Some("1")))
  }

  clientTest("SORT BY/GET sorts by external weights and projects external values, nil for missing") { client =>
    client.rPush("keys-sortby", "1", "2", "3") >>
      client.mSet(("keys-w-1", "30"), ("keys-w-2", "10"), ("keys-w-3", "20")) >>
      client.mSet(("keys-d-1", "A"), ("keys-d-3", "C")) >>
      client
        .sort[String]("keys-sortby", by = Some("keys-w-*"), get = Vector("keys-d-*", "#"))
        .is(Vector(None, Some("2"), Some("C"), Some("3"), Some("A"), Some("1")))
  }

  clientTest("SORT_RO reads without storing; SORT STORE writes the result and returns the count") { client =>
    client.rPush("keys-sortstore", "b", "a", "c") >>
      client.sortRo[String]("keys-sortstore", alpha = true).is(Vector(Some("a"), Some("b"), Some("c"))) >>
      client.sortStore("keys-sortstore-dst", "keys-sortstore", alpha = true).is(3L) >>
      client.lRange[String]("keys-sortstore-dst", 0L, -1L).is(Vector("a", "b", "c"))
  }

  clientTest("MOVE relocates a key out of the current database") { client =>
    client.set("keys-move", "v") >>
      client.move("keys-move", 1).is(true) >>
      client.exists("keys-move").is(0L) >>
      client.move("keys-move", 1).is(false)
  }

  clientTest("DUMP and RESTORE round-trip a value through its serialized form") { client =>
    for {
      _     <- client.set("keys-dump", "payload")
      bytes <- client.dump("keys-dump").flatMap(required("DUMP", _))
      _     <- client.restore("keys-restore", bytes)
      _     <- client.get[String]("keys-restore").is(Some("payload"))
      _     <- client.restore("keys-restore", bytes, replace = true)
      _     <- client.dump("keys-dump-missing").is(None)
    } yield ()
  }

  clientTest("MIGRATE reports NOKEY when the source key is absent") { client =>
    client.migrate("localhost", 6379, 0, 1.second)("keys-migrate-ghost").is(MigrateResult.NoKey)
  }

  clientTest("OBJECT exposes encoding, refcount, and idle time; None for a missing key") { client =>
    client.set("keys-object", "12345") >>
      client.objectEncoding("keys-object").is(Some("int")) >>
      client.objectRefCount("keys-object").satisfies(_.exists(_ >= 1L)) >>
      client.objectIdleTime("keys-object").satisfies(_.exists(_ >= Duration.Zero)) >>
      client.objectEncoding("keys-object-missing").is(None) >>
      client.objectRefCount("keys-object-missing").is(None) >>
      client.objectIdleTime("keys-object-missing").is(None)
  }

  clientTest("OBJECT FREQ reports access frequency under an LFU policy, None for a missing key") { client =>
    client.configSet("maxmemory-policy" -> "allkeys-lfu") >>
      client.set("keys-freq", "v") >>
      client.objectFreq("keys-freq").satisfies(_.exists(_ >= 0L)) >>
      client.objectFreq("keys-freq-missing").is(None) >>
      client.configSet("maxmemory-policy" -> "noeviction")
  }

  clientTest("FLUSHALL empties the keyspace") { client =>
    client.set("keys-flush", "v") >>
      client.exists("keys-flush").is(1L) >>
      client.flushAll() >>
      client.exists("keys-flush").is(0L)
  }

  // DELIFEQ exists only on Valkey.
  valkeyTest("DELIFEQ deletes only when the current value matches") { client =>
    client.set("vk-lock", "token-1") >>
      client.delIfEq("vk-lock", "other").is(false) >>
      client.exists("vk-lock").is(1L) >>
      client.delIfEq("vk-lock", "token-1").is(true) >>
      client.exists("vk-lock").is(0L) >>
      client.delIfEq("vk-lock-absent", "x").is(false)
  }
}

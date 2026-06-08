package sage.integration.commands

import kyo.compat.*

import sage.commands.ScanCursor
import sage.integration.{Images, ServerSuite}

abstract class HashesSuite(image: String) extends ServerSuite(image) {

  test("HSET writes fields, HGET and HMGET read them back, HEXISTS and HDEL remove them") {
    withClient { client =>
      for {
        added   <- client.hSet("hash-basic", ("f1", "v1"), ("f2", "v2"))
        one     <- client.hGet[String, String, String]("hash-basic", "f1")
        many    <- client.hmGet[String, String, String]("hash-basic", "f1", "missing", "f2")
        present <- client.hExists("hash-basic", "f1")
        removed <- client.hDel("hash-basic", "f1", "missing")
        gone    <- client.hExists("hash-basic", "f1")
      } yield {
        assertEquals(added, 2L)
        assertEquals(one, Some("v1"))
        assertEquals(many, Vector(Some("v1"), None, Some("v2")))
        assertEquals(present, true)
        assertEquals(removed, 1L)
        assertEquals(gone, false)
      }
    }
  }

  test("HSETNX only writes an absent field") {
    withClient { client =>
      for {
        first  <- client.hSetNx("hash-setnx", "f", "one")
        second <- client.hSetNx("hash-setnx", "f", "two")
        value  <- client.hGet[String, String, String]("hash-setnx", "f")
      } yield {
        assertEquals(first, true)
        assertEquals(second, false)
        assertEquals(value, Some("one"))
      }
    }
  }

  test("HGETALL HKEYS HVALS HLEN HSTRLEN view the whole hash") {
    withClient { client =>
      for {
        _      <- client.hSet("hash-view", ("a", "1"), ("b", "22"))
        all    <- client.hGetAll[String, String, String]("hash-view")
        keys   <- client.hKeys[String, String]("hash-view")
        vals   <- client.hVals[String, String]("hash-view")
        len    <- client.hLen("hash-view")
        strLen <- client.hStrLen("hash-view", "b")
      } yield {
        assertEquals(all, Map("a" -> "1", "b" -> "22"))
        assertEquals(keys.toSet, Set("a", "b"))
        assertEquals(vals.toSet, Set("1", "22"))
        assertEquals(len, 2L)
        assertEquals(strLen, 2L)
      }
    }
  }

  test("HINCRBY and HINCRBYFLOAT count atomically on a field") {
    withClient { client =>
      for {
        _     <- client.hSet("hash-incr", ("n", "10"))
        byInt <- client.hIncrBy("hash-incr", "n", 5L)
        byFlt <- client.hIncrByFloat("hash-incr", "n", 0.5)
      } yield {
        assertEquals(byInt, 15L)
        assertEquals(byFlt, 15.5)
      }
    }
  }

  test("HRANDFIELD returns a member, a count of members, and field/value pairs") {
    withClient { client =>
      for {
        _      <- client.hSet("hash-rand", ("a", "1"), ("b", "2"), ("c", "3"))
        single <- client.hRandField[String, String]("hash-rand")
        few    <- client.hRandField[String, String]("hash-rand", 2L)
        pairs  <- client.hRandFieldWithValues[String, String, String]("hash-rand", -5L)
        empty  <- client.hRandField[String, String]("hash-rand-missing")
      } yield {
        assert(single.exists(Set("a", "b", "c")))
        assertEquals(few.size, 2)
        assert(few.toSet.subsetOf(Set("a", "b", "c")))
        assertEquals(pairs.size, 5)
        assert(pairs.forall { case (f, v) => Map("a" -> "1", "b" -> "2", "c" -> "3").get(f).contains(v) })
        assertEquals(empty, None)
      }
    }
  }

  test("HSCAN streams field/value pairs and NOVALUES streams bare fields") {
    withClient { client =>
      for {
        _         <- client.hSet("hash-scan", ("a", "1"), ("b", "2"), ("c", "3"))
        page      <- client.hScan[String, String, String]("hash-scan", ScanCursor.start)
        fieldPage <- client.hScanNoValues[String, String]("hash-scan", ScanCursor.start)
      } yield {
        assertEquals(page.items.toMap, Map("a" -> "1", "b" -> "2", "c" -> "3"))
        assertEquals(fieldPage.items.toSet, Set("a", "b", "c"))
      }
    }
  }
}

class RedisHashesSuite extends HashesSuite(Images.redis)

class ValkeyHashesSuite extends HashesSuite(Images.valkey)

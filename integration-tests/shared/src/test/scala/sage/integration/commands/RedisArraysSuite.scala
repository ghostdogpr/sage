package sage.integration.commands

import kyo.compat.*

import sage.commands.{ArGrepCombine, ArMatch}
import sage.integration.{Images, ServerSuite}

/**
  * The Array (`AR*`) data type is Redis-only (no Valkey counterpart) and shipped as a preview, so it has a single-server suite.
  */
class RedisArraysSuite extends ServerSuite(Images.redis) {

  clientTest("ARSET/ARGET/ARLEN/ARCOUNT cover writes, reads, and length") { client =>
    client.arSet("ar-basic", 0L, "a", "b", "c").is(3L) >>
      client.arGet[String]("ar-basic", 1L).is(Some("b")) >>
      client.arGet[String]("ar-basic", 99L).is(None) >>
      client.arLen("ar-basic").is(3L) >>
      client.arCount("ar-basic").is(3L)
  }

  clientTest("ARMSET/ARMGET/ARGETRANGE keep sparse gaps as None") { client =>
    client.arSet("ar-sparse", 0L, "a", "b", "c") >>
      client.arMSet("ar-sparse", 10L -> "x", 20L -> "y") >>
      client.arMGet[String]("ar-sparse", 10L, 11L, 20L).is(Vector(Some("x"), None, Some("y"))) >>
      client.arGetRange[String]("ar-sparse", 0L, 2L).is(Vector(Some("a"), Some("b"), Some("c")))
  }

  clientTest("ARRING wraps and ARLASTITEMS reads the most recent items") { client =>
    client.arRing("ar-ring", 3L, "a", "b", "c", "d", "e").is(1L) >>
      client.arLen("ar-ring").is(3L) >>
      client.arLastItems[String]("ar-ring", 2L).is(Vector("d", "e")) >>
      client.arLastItems[String]("ar-ring", 2L, rev = true).is(Vector("e", "d"))
  }

  clientTest("ARINSERT/ARNEXT/ARSEEK drive the write cursor") { client =>
    client.arInsert("ar-cursor", "p", "q").is(1L) >>
      client.arNext("ar-cursor").is(Some(2L)) >>
      client.arSeek("ar-cursor", 100L).is(true) >>
      client.arSeek("ar-cursor-absent", 5L).is(false)
  }

  clientTest("ARSCAN returns only existing index/value pairs") { client =>
    client.arSet("ar-scan", 0L, "a") >>
      client.arSet("ar-scan", 5L, "f") >>
      client.arScan[String]("ar-scan", 0L, 10L).is(Vector(0L -> "a", 5L -> "f"))
  }

  clientTest("ARDEL and ARDELRANGE delete by index and by ranges") { client =>
    client.arSet("ar-del", 0L, "a", "b", "c", "d", "e", "f", "g", "h") >>
      client.arDelRange("ar-del", 0L -> 1L, 4L -> 5L).is(4L) >>
      client.arDel("ar-del", 2L).is(1L) >>
      client.arScan[String]("ar-del", 0L, 10L).is(Vector(3L -> "d", 6L -> "g", 7L -> "h"))
  }

  clientTest("ARGREP matches indices and, WITHVALUES, index/value pairs") { client =>
    client.arSet("ar-grep", 0L, "apple", "banana", "apricot", "cherry") >>
      client.arGrep("ar-grep", 0L, 10L)(ArMatch.Glob("ap*")).is(Vector(0L, 2L)) >>
      client.arGrepWithValues[String]("ar-grep", 0L, 10L)(ArMatch.Glob("ap*")).is(Vector(0L -> "apple", 2L -> "apricot")) >>
      client.arGrep("ar-grep", 0L, 10L, combine = ArGrepCombine.And)(ArMatch.Glob("a*"), ArMatch.Glob("*e")).is(Vector(0L))
  }

  clientTest("AROP aggregates, bit-combines, and counts over a range") { client =>
    client.arSet("ar-op", 0L, "10", "20", "30") >>
      client.arOpSum("ar-op", 0L, 2L).is(Some(60.0)) >>
      client.arOpMin("ar-op", 0L, 2L).is(Some(10.0)) >>
      client.arOpMax("ar-op", 0L, 2L).is(Some(30.0)) >>
      client.arOpAnd("ar-op", 0L, 2L).is(Some(0L)) >>
      client.arOpOr("ar-op", 0L, 2L).is(Some(30L)) >>
      client.arOpXor("ar-op", 0L, 2L).is(Some(0L)) >>
      client.arOpUsed("ar-op", 0L, 2L).is(3L) >>
      client.arOpMatch("ar-op", 0L, 2L, "20").is(1L)
  }

  clientTest("ARINFO and ARINFO FULL report metadata") { client =>
    for {
      _    <- client.arSet("ar-info", 0L, "a", "b", "c")
      _    <- client.arMSet("ar-info", 100L -> "z")
      info <- client.arInfo("ar-info")
      full <- client.arInfoFull("ar-info")
    } yield {
      assertEquals(info.count, 4L)
      assertEquals(info.len, 101L)
      assertEquals(full.count, 4L)
      assert(full.sparseSlices.forall(_ >= 0L))
    }
  }
}

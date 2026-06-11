package sage.integration.commands

import kyo.compat.*

import sage.commands.{ArGrepCombine, ArMatch}
import sage.integration.{Images, ServerSuite}

/**
  * The Array (`AR*`) data type is Redis-only (no Valkey counterpart) and shipped as a preview, so it has a single-server suite (ADR-0026).
  */
class ArraysSuite extends ServerSuite(Images.redis) {

  test("ARSET/ARGET/ARLEN/ARCOUNT cover writes, reads, and length") {
    withClient { client =>
      for {
        filled  <- client.arSet("ar-basic", 0L, "a", "b", "c")
        b       <- client.arGet[String, String]("ar-basic", 1L)
        missing <- client.arGet[String, String]("ar-basic", 99L)
        len     <- client.arLen("ar-basic")
        count   <- client.arCount("ar-basic")
      } yield {
        assertEquals(filled, 3L)
        assertEquals(b, Some("b"))
        assertEquals(missing, None)
        assertEquals(len, 3L)
        assertEquals(count, 3L)
      }
    }
  }

  test("ARMSET/ARMGET/ARGETRANGE keep sparse gaps as None") {
    withClient { client =>
      for {
        _     <- client.arSet("ar-sparse", 0L, "a", "b", "c")
        _     <- client.arMSet("ar-sparse", 10L -> "x", 20L -> "y")
        got   <- client.arMGet[String, String]("ar-sparse", 10L, 11L, 20L)
        range <- client.arGetRange[String, String]("ar-sparse", 0L, 2L)
      } yield {
        assertEquals(got, Vector(Some("x"), None, Some("y")))
        assertEquals(range, Vector(Some("a"), Some("b"), Some("c")))
      }
    }
  }

  test("ARRING wraps and ARLASTITEMS reads the most recent items") {
    withClient { client =>
      for {
        last  <- client.arRing("ar-ring", 3L, "a", "b", "c", "d", "e")
        len   <- client.arLen("ar-ring")
        items <- client.arLastItems[String, String]("ar-ring", 2L)
        rev   <- client.arLastItems[String, String]("ar-ring", 2L, rev = true)
      } yield {
        assertEquals(last, 1L)
        assertEquals(len, 3L)
        assertEquals(items, Vector("d", "e"))
        assertEquals(rev, Vector("e", "d"))
      }
    }
  }

  test("ARINSERT/ARNEXT/ARSEEK drive the write cursor") {
    withClient { client =>
      for {
        last   <- client.arInsert("ar-cursor", "p", "q")
        next   <- client.arNext("ar-cursor")
        seeked <- client.arSeek("ar-cursor", 100L)
        absent <- client.arSeek("ar-cursor-absent", 5L)
      } yield {
        assertEquals(last, 1L)
        assertEquals(next, Some(2L))
        assertEquals(seeked, true)
        assertEquals(absent, false)
      }
    }
  }

  test("ARSCAN returns only existing index/value pairs") {
    withClient { client =>
      for {
        _    <- client.arSet("ar-scan", 0L, "a")
        _    <- client.arSet("ar-scan", 5L, "f")
        scan <- client.arScan[String, String]("ar-scan", 0L, 10L)
      } yield assertEquals(scan, Vector(0L -> "a", 5L -> "f"))
    }
  }

  test("ARDEL and ARDELRANGE delete by index and by ranges") {
    withClient { client =>
      for {
        _       <- client.arSet("ar-del", 0L, "a", "b", "c", "d", "e", "f", "g", "h")
        deleted <- client.arDelRange("ar-del", 0L -> 1L, 4L -> 5L)
        one     <- client.arDel("ar-del", 2L)
        scan    <- client.arScan[String, String]("ar-del", 0L, 10L)
      } yield {
        assertEquals(deleted, 4L)
        assertEquals(one, 1L)
        assertEquals(scan, Vector(3L -> "d", 6L -> "g", 7L -> "h"))
      }
    }
  }

  test("ARGREP matches indices and, WITHVALUES, index/value pairs") {
    withClient { client =>
      for {
        _       <- client.arSet("ar-grep", 0L, "apple", "banana", "apricot", "cherry")
        glob    <- client.arGrep("ar-grep", 0L, 10L)(ArMatch.Glob("ap*"))
        withVal <- client.arGrepWithValues[String, String]("ar-grep", 0L, 10L)(ArMatch.Glob("ap*"))
        anded   <- client.arGrep("ar-grep", 0L, 10L, combine = ArGrepCombine.And)(ArMatch.Glob("a*"), ArMatch.Glob("*e"))
      } yield {
        assertEquals(glob, Vector(0L, 2L))
        assertEquals(withVal, Vector(0L -> "apple", 2L -> "apricot"))
        assertEquals(anded, Vector(0L))
      }
    }
  }

  test("AROP aggregates, bit-combines, and counts over a range") {
    withClient { client =>
      for {
        _      <- client.arSet("ar-op", 0L, "10", "20", "30")
        sum    <- client.arOpSum("ar-op", 0L, 2L)
        min    <- client.arOpMin("ar-op", 0L, 2L)
        max    <- client.arOpMax("ar-op", 0L, 2L)
        and    <- client.arOpAnd("ar-op", 0L, 2L)
        used   <- client.arOpUsed("ar-op", 0L, 2L)
        atMost <- client.arOpMatch("ar-op", 0L, 2L, "20")
      } yield {
        assertEquals(sum, Some(60.0))
        assertEquals(min, Some(10.0))
        assertEquals(max, Some(30.0))
        assertEquals(and, Some(0L))
        assertEquals(used, 3L)
        assertEquals(atMost, 1L)
      }
    }
  }

  test("ARINFO and ARINFO FULL report metadata") {
    withClient { client =>
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
}

package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.{BlockTimeout, InsertPosition, ListSide}
import sage.integration.{Images, ServerSuite}

abstract class ListsSuite(image: String) extends ServerSuite(image) {

  test("RPUSH and LPUSH build a list that LRANGE LLEN and LINDEX read") {
    withClient { client =>
      for {
        _      <- client.rPush("list-build", "b", "c")
        _      <- client.lPush("list-build", "a")
        all    <- client.lRange[String, String]("list-build", 0L, -1L)
        len    <- client.lLen("list-build")
        second <- client.lIndex[String, String]("list-build", 1L)
      } yield {
        assertEquals(all, Vector("a", "b", "c"))
        assertEquals(len, 3L)
        assertEquals(second, Some("b"))
      }
    }
  }

  test("LPUSHX and RPUSHX only extend an existing list") {
    withClient { client =>
      for {
        absent  <- client.rPushX("list-x-missing", "v")
        _       <- client.rPush("list-x", "a")
        present <- client.rPushX("list-x", "b")
        head    <- client.lPushX("list-x", "z")
        all     <- client.lRange[String, String]("list-x", 0L, -1L)
      } yield {
        assertEquals(absent, 0L)
        assertEquals(present, 2L)
        assertEquals(head, 3L)
        assertEquals(all, Vector("z", "a", "b"))
      }
    }
  }

  test("LPOP and RPOP pop one or several elements, empty when the list is gone") {
    withClient { client =>
      for {
        _       <- client.rPush("list-pop", "a", "b", "c", "d")
        head    <- client.lPop[String, String]("list-pop")
        tail    <- client.rPop[String, String]("list-pop")
        twoHead <- client.lPopCount[String, String]("list-pop", 2L)
        drained <- client.lPopCount[String, String]("list-pop", 2L)
        none    <- client.lPop[String, String]("list-pop")
      } yield {
        assertEquals(head, Some("a"))
        assertEquals(tail, Some("d"))
        assertEquals(twoHead, Vector("b", "c"))
        assertEquals(drained, Vector.empty[String])
        assertEquals(none, None)
      }
    }
  }

  test("LSET LINSERT LREM and LTRIM edit the list in place") {
    withClient { client =>
      for {
        _        <- client.rPush("list-edit", "a", "b", "b", "c")
        _        <- client.lSet("list-edit", 0L, "A")
        inserted <- client.lInsert("list-edit", InsertPosition.Before, "c", "x")
        noPivot  <- client.lInsert("list-edit", InsertPosition.After, "zzz", "y")
        removed  <- client.lRem("list-edit", 0L, "b")
        _        <- client.lTrim("list-edit", 0L, 1L)
        all      <- client.lRange[String, String]("list-edit", 0L, -1L)
      } yield {
        assertEquals(inserted, 5L)
        assertEquals(noPivot, -1L)
        assertEquals(removed, 2L)
        assertEquals(all, Vector("A", "x"))
      }
    }
  }

  test("LPOS finds the first match, all matches, and reports None when absent") {
    withClient { client =>
      for {
        _     <- client.rPush("list-pos", "a", "b", "a", "c", "a")
        first <- client.lPos("list-pos", "a")
        last  <- client.lPos("list-pos", "a", rank = Some(-1L))
        every <- client.lPosCount("list-pos", "a", 0L)
        none  <- client.lPos("list-pos", "zzz")
      } yield {
        assertEquals(first, Some(0L))
        assertEquals(last, Some(4L))
        assertEquals(every, Vector(0L, 2L, 4L))
        assertEquals(none, None)
      }
    }
  }

  test("LMOVE shifts an element between ends and LMPOP pops from the first non-empty key") {
    withClient { client =>
      for {
        _       <- client.rPush("list-src", "a", "b", "c")
        moved   <- client.lMove[String, String]("list-src", "list-dst", ListSide.Left, ListSide.Right)
        dst     <- client.lRange[String, String]("list-dst", 0L, -1L)
        popped  <- client.lMpop[String, String]("list-empty", "list-src")(ListSide.Left, count = Some(2L))
        emptyOk <- client.lMpop[String, String]("list-empty")(ListSide.Left)
      } yield {
        assertEquals(moved, Some("a"))
        assertEquals(dst, Vector("a"))
        assertEquals(popped, Some(("list-src", Vector("b", "c"))))
        assertEquals(emptyOk, None)
      }
    }
  }

  test("BLPOP and BRPOP return a present element and time out to None on an empty key") {
    withClient { client =>
      for {
        _     <- client.rPush("blpop-data", "a", "b")
        head  <- client.blPop[String, String]("blpop-data")(BlockTimeout.After(1.second))
        tail  <- client.brPop[String, String]("blpop-data")(BlockTimeout.After(1.second))
        empty <- client.blPop[String, String]("blpop-empty")(BlockTimeout.After(100.millis))
      } yield {
        assertEquals(head, Some(("blpop-data", "a")))
        assertEquals(tail, Some(("blpop-data", "b")))
        assertEquals(empty, None)
      }
    }
  }

  test("BLMOVE and BLMPOP move and pop, timing out to None when nothing is available") {
    withClient { client =>
      for {
        _      <- client.rPush("blmove-src", "a", "b")
        moved  <- client.blMove[String, String]("blmove-src", "blmove-dst", ListSide.Left, ListSide.Right, BlockTimeout.After(1.second))
        dst    <- client.lRange[String, String]("blmove-dst", 0L, -1L)
        popped <- client.blMpop[String, String]("blmpop-empty", "blmove-src")(ListSide.Left, BlockTimeout.After(1.second), count = Some(2L))
        none   <- client.blMpop[String, String]("blmpop-empty")(ListSide.Left, BlockTimeout.After(100.millis))
      } yield {
        assertEquals(moved, Some("a"))
        assertEquals(dst, Vector("a"))
        assertEquals(popped, Some(("blmove-src", Vector("b"))))
        assertEquals(none, None)
      }
    }
  }

  test("a blocking command does not stall ordinary commands on the multiplexed connection") {
    withClient { client =>
      CIO
        .zip(
          client.blPop[String, String]("nonstall-queue")(BlockTimeout.After(5.seconds)),
          for {
            pong <- client.ping()
            _    <- client.rPush("nonstall-queue", "payload")
          } yield pong
        )
        .map { case (popped, pong) =>
          assertEquals(popped, Some(("nonstall-queue", "payload")))
          assertEquals(pong, "PONG")
        }
    }
  }
}

class RedisListsSuite extends ListsSuite(Images.redis)

class ValkeyListsSuite extends ListsSuite(Images.valkey)

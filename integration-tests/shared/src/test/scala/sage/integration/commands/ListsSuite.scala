package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.{BlockTimeout, InsertPosition, ListSide}
import sage.integration.BothServersSuite

class ListsSuite extends BothServersSuite {

  clientTest("RPUSH and LPUSH build a list that LRANGE LLEN and LINDEX read") { client =>
    client.rPush("list-build", "b", "c") >>
      client.lPush("list-build", "a") >>
      client.lRange[String]("list-build", 0L, -1L).is(Vector("a", "b", "c")) >>
      client.lLen("list-build").is(3L) >>
      client.lIndex[String]("list-build", 1L).is(Some("b"))
  }

  clientTest("LPUSHX and RPUSHX only extend an existing list") { client =>
    client.rPushX("list-x-missing", "v").is(0L) >>
      client.rPush("list-x", "a") >>
      client.rPushX("list-x", "b").is(2L) >>
      client.lPushX("list-x", "z").is(3L) >>
      client.lRange[String]("list-x", 0L, -1L).is(Vector("z", "a", "b"))
  }

  clientTest("LPOP and RPOP pop one or several elements, empty when the list is gone") { client =>
    client.rPush("list-pop", "a", "b", "c", "d") >>
      client.lPop[String]("list-pop").is(Some("a")) >>
      client.rPop[String]("list-pop").is(Some("d")) >>
      client.lPopCount[String]("list-pop", 2L).is(Vector("b", "c")) >>
      client.lPopCount[String]("list-pop", 2L).is(Vector.empty[String]) >>
      client.lPop[String]("list-pop").is(None)
  }

  clientTest("RPOP with a count pops several elements from the tail in pop order") { client =>
    client.rPush("list-rpopc", "a", "b", "c", "d") >>
      client.rPopCount[String]("list-rpopc", 2L).is(Vector("d", "c")) >>
      client.lRange[String]("list-rpopc", 0L, -1L).is(Vector("a", "b"))
  }

  clientTest("LSET LINSERT LREM and LTRIM edit the list in place") { client =>
    client.rPush("list-edit", "a", "b", "b", "c") >>
      client.lSet("list-edit", 0L, "A") >>
      client.lInsert("list-edit", InsertPosition.Before, "c", "x").is(5L) >>
      client.lInsert("list-edit", InsertPosition.After, "zzz", "y").is(-1L) >>
      client.lRem("list-edit", 0L, "b").is(2L) >>
      client.lTrim("list-edit", 0L, 1L) >>
      client.lRange[String]("list-edit", 0L, -1L).is(Vector("A", "x"))
  }

  clientTest("LPOS finds the first match, all matches, and reports None when absent") { client =>
    client.rPush("list-pos", "a", "b", "a", "c", "a") >>
      client.lPos("list-pos", "a").is(Some(0L)) >>
      client.lPos("list-pos", "a", rank = Some(-1L)).is(Some(4L)) >>
      client.lPosCount("list-pos", "a", 0L).is(Vector(0L, 2L, 4L)) >>
      client.lPos("list-pos", "zzz").is(None)
  }

  clientTest("LMOVE shifts an element between ends and LMPOP pops from the first non-empty key") { client =>
    client.rPush("list-src", "a", "b", "c") >>
      client.lMove[String]("list-src", "list-dst", ListSide.Left, ListSide.Right).is(Some("a")) >>
      client.lRange[String]("list-dst", 0L, -1L).is(Vector("a")) >>
      client.lMpop[String]("list-empty", "list-src")(ListSide.Left, count = Some(2L)).is(Some(("list-src", Vector("b", "c")))) >>
      client.lMpop[String]("list-empty")(ListSide.Left).is(None)
  }

  clientTest("BLPOP and BRPOP return a present element and time out to None on an empty key") { client =>
    client.rPush("blpop-data", "a", "b") >>
      client.blPop[String]("blpop-data")(BlockTimeout.After(1.second)).is(Some(("blpop-data", "a"))) >>
      client.brPop[String]("blpop-data")(BlockTimeout.After(1.second)).is(Some(("blpop-data", "b"))) >>
      client.blPop[String]("blpop-empty")(BlockTimeout.After(100.millis)).is(None)
  }

  clientTest("BLMOVE and BLMPOP move and pop, timing out to None when nothing is available") { client =>
    client.rPush("blmove-src", "a", "b") >>
      client.blMove[String]("blmove-src", "blmove-dst", ListSide.Left, ListSide.Right, BlockTimeout.After(1.second)).is(Some("a")) >>
      client.lRange[String]("blmove-dst", 0L, -1L).is(Vector("a")) >>
      client
        .blMpop[String]("blmpop-empty", "blmove-src")(ListSide.Left, BlockTimeout.After(1.second), count = Some(2L))
        .is(Some(("blmove-src", Vector("b")))) >>
      client.blMpop[String]("blmpop-empty")(ListSide.Left, BlockTimeout.After(100.millis)).is(None)
  }

  clientTest("a blocking command does not stall ordinary commands on the multiplexed connection") { client =>
    CIO
      .zip(
        client.blPop[String]("nonstall-queue")(BlockTimeout.After(5.seconds)),
        for {
          pong <- client.ping()
          _    <- client.rPush("nonstall-queue", "payload")
        } yield pong
      )
      .is((Some(("nonstall-queue", "payload")), "PONG"))
  }
}

package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.*
import sage.integration.BothServersSuite

class StreamsSuite extends BothServersSuite {

  clientTest("XADD XLEN XRANGE and XREVRANGE round-trip entries, preserving field order") { client =>
    client.xAdd("s-range", XAddId.Explicit(StreamId(1L, 0L)))(("a", "1"), ("b", "2")).is(StreamId(1L, 0L)) >>
      client.xAdd("s-range", XAddId.Explicit(StreamId(2L, 0L)))(("c", "3")).is(StreamId(2L, 0L)) >>
      client.xLen("s-range").is(2L) >>
      client
        .xRange[String, String]("s-range")
        .is(Vector(StreamEntry(StreamId(1L, 0L), Vector("a" -> "1", "b" -> "2")), StreamEntry(StreamId(2L, 0L), Vector("c" -> "3")))) >>
      client.xRevRange[String, String]("s-range").map(_.map(_.id)).is(Vector(StreamId(2L, 0L), StreamId(1L, 0L))) >>
      client.xRange[String, String]("s-range", StreamRangeId.Exclusive(StreamId(1L, 0L))).map(_.map(_.id)).is(Vector(StreamId(2L, 0L)))
  }

  clientTest("XADD with * generates increasing ids, NOMKSTREAM declines a missing stream, and XDEL removes") { client =>
    for {
      _ <- client.xAddNoMkStream("s-del-missing")(("f", "v")).is(None)
      a <- client.xAdd("s-del")(("f", "1"))
      b <- client.xAdd("s-del")(("f", "2"))
      _ <- client.xDel("s-del")(a).is(1L)
      _ <- client.xLen("s-del").is(1L)
    } yield assert(b > a)
  }

  clientTest("XTRIM MAXLEN caps the stream length") { client =>
    client.xAdd("s-trim", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xAdd("s-trim", XAddId.Explicit(StreamId(2L, 0L)))(("f", "2")) >>
      client.xAdd("s-trim", XAddId.Explicit(StreamId(3L, 0L)))(("f", "3")) >>
      client.xTrim("s-trim", Trimming.Exact(TrimThreshold.MaxLen(1L))).is(2L) >>
      client.xLen("s-trim").is(1L)
  }

  clientTest("XINFO STREAM reports length and the last generated id") { client =>
    for {
      _    <- client.xAdd("s-info", XAddId.Explicit(StreamId(1L, 0L)))(("f", "v"))
      _    <- client.xAdd("s-info", XAddId.Explicit(StreamId(5L, 0L)))(("g", "w"))
      info <- client.xInfoStream[String, String]("s-info")
    } yield {
      assertEquals(info.length, 2L)
      assertEquals(info.lastGeneratedId, StreamId(5L, 0L))
      assertEquals(info.firstEntry, Some(StreamEntry(StreamId(1L, 0L), Vector("f" -> "v"))))
    }
  }

  clientTest("a consumer group reads new entries, acknowledges them, and reports an empty PEL") { client =>
    client.xAdd("s-grp", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xAdd("s-grp", XAddId.Explicit(StreamId(2L, 0L)))(("f", "2")) >>
      client.xGroupCreate("s-grp", "g", GroupStartId.At(StreamId(0L, 0L))) >>
      client
        .xReadGroup[String, String]("g", "c1")(("s-grp", GroupReadId.New))(count = Some(10L))
        .is(Vector("s-grp" -> Vector(StreamEntry(StreamId(1L, 0L), Vector("f" -> "1")), StreamEntry(StreamId(2L, 0L), Vector("f" -> "2"))))) >>
      client.xPending("s-grp", "g").map(_.total).is(2L) >>
      client.xAck("s-grp", "g")(StreamId(1L, 0L), StreamId(2L, 0L)).is(2L) >>
      client.xPending("s-grp", "g").map(_.total).is(0L) >>
      client.xInfoGroups("s-grp").map(_.map(_.name)).is(Vector("g"))
  }

  clientTest("XCLAIM and XAUTOCLAIM transfer pending entries to another consumer") { client =>
    client.xAdd("s-claim", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xGroupCreate("s-claim", "g", GroupStartId.At(StreamId(0L, 0L))) >>
      client.xReadGroup[String, String]("g", "c1")(("s-claim", GroupReadId.New))() >>
      client
        .xClaim[String, String]("s-claim", "g", "c2", Duration.Zero)(StreamId(1L, 0L))()
        .is(Vector(StreamEntry(StreamId(1L, 0L), Vector("f" -> "1")))) >>
      client.xAutoClaim[String, String]("s-claim", "g", "c3", Duration.Zero).map(_.entries.map(_.id)).is(Vector(StreamId(1L, 0L))) >>
      client.xPendingExtended("s-claim", "g").map(_.map(_.consumer)).is(Vector("c3"))
  }

  clientTest("XGROUP CREATECONSUMER/DELCONSUMER/SETID/DESTROY and XSETID manage a group and the stream's last id") { client =>
    client.xAdd("s-mgmt", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xGroupCreate("s-mgmt", "g", GroupStartId.At(StreamId(0L, 0L))) >>
      client.xGroupCreateConsumer("s-mgmt", "g", "c1").is(true) >>
      client.xGroupCreateConsumer("s-mgmt", "g", "c1").is(false) >>
      client.xGroupDelConsumer("s-mgmt", "g", "c1").is(0L) >>
      client.xGroupSetId("s-mgmt", "g", GroupStartId.At(StreamId(1L, 0L))) >>
      client.xSetId("s-mgmt", GroupStartId.At(StreamId(5L, 0L))) >>
      client.xInfoStream[String, String]("s-mgmt").map(_.lastGeneratedId).is(StreamId(5L, 0L)) >>
      client.xGroupDestroy("s-mgmt", "g").is(true) >>
      client.xInfoGroups("s-mgmt").satisfies(_.isEmpty)
  }

  clientTest("XCLAIM and XAUTOCLAIM JUSTID transfer pending ids without the payload") { client =>
    client.xAdd("s-justid", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xAdd("s-justid", XAddId.Explicit(StreamId(2L, 0L)))(("f", "2")) >>
      client.xGroupCreate("s-justid", "g", GroupStartId.At(StreamId(0L, 0L))) >>
      client.xReadGroup[String, String]("g", "c1")(("s-justid", GroupReadId.New))() >>
      client.xClaimJustId("s-justid", "g", "c2", Duration.Zero)(StreamId(1L, 0L))().is(Vector(StreamId(1L, 0L))) >>
      client.xAutoClaimJustId("s-justid", "g", "c3", Duration.Zero).map(_.claimed).is(Vector(StreamId(1L, 0L), StreamId(2L, 0L)))
  }

  clientTest("XINFO STREAM FULL decodes the group PEL after a consumer has read but not acked") { client =>
    for {
      _    <- client.xAdd("s-full", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
      _    <- client.xGroupCreate("s-full", "g", GroupStartId.At(StreamId(0L, 0L)))
      _    <- client.xReadGroup[String, String]("g", "c1")(("s-full", GroupReadId.New))()
      full <- client.xInfoStreamFull[String, String]("s-full")
    } yield {
      assertEquals(full.length, 1L)
      assertEquals(
        full.groups.map(g => (g.name, g.pending.map(p => (p.id, p.consumer)), g.consumers.map(c => (c.name, c.pending.map(_.id))))),
        Vector(("g", Vector((StreamId(1L, 0L), Some("c1"))), Vector(("c1", Vector(StreamId(1L, 0L))))))
      )
    }
  }

  clientTest("XREAD with BLOCK does not stall ordinary commands on the multiplexed connection") { client =>
    client.xAdd("s-block", XAddId.Explicit(StreamId(1L, 0L)))(("f", "0")) >>
      CIO
        .zip(
          client.xRead[String, String](("s-block", ReadId.After(StreamId(1L, 0L))))(block = Some(BlockTimeout.After(5.seconds))),
          for {
            pong <- client.ping()
            _    <- client.xAdd("s-block", XAddId.Explicit(StreamId(2L, 0L)))(("f", "1"))
          } yield pong
        )
        .is((Vector("s-block" -> Vector(StreamEntry(StreamId(2L, 0L), Vector("f" -> "1")))), "PONG"))
  }

  // XCFGSET, XDELEX, XACKDEL and XNACK exist only on Redis.
  redisTest("XCFGSET sets per-stream idempotent-message-processing config") { client =>
    client.xAdd("sx-cfg", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xCfgSet("sx-cfg", idmpDuration = Some(1.hour), idmpMaxSize = Some(100L)).is(()) >>
      client.xCfgSet("sx-cfg", idmpMaxSize = Some(50L)).is(())
  }

  redisTest("XDELEX reports per-id deletion status") { client =>
    client.xAdd("sx-delex", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xAdd("sx-delex", XAddId.Explicit(StreamId(2L, 0L)))(("f", "2")) >>
      client.xDelEx("sx-delex")(StreamId(1L, 0L), StreamId(9L, 0L)).is(Vector(StreamEntryDeletion.Deleted, StreamEntryDeletion.NotFound)) >>
      client.xLen("sx-delex").is(1L)
  }

  redisTest("XACKDEL acknowledges and deletes in one step") { client =>
    client.xAdd("sx-ackdel", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xGroupCreate("sx-ackdel", "g", GroupStartId.At(StreamId(0L, 0L))) >>
      client.xReadGroup[String, String]("g", "c1")(("sx-ackdel", GroupReadId.New))() >>
      client.xAckDel("sx-ackdel", "g")(StreamId(1L, 0L)).is(Vector(StreamEntryDeletion.Deleted)) >>
      client.xLen("sx-ackdel").is(0L) >>
      client.xPending("sx-ackdel", "g").map(_.total).is(0L)
  }

  redisTest("XNACK releases a pending entry back to the group") { client =>
    client.xAdd("sx-nack", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1")) >>
      client.xGroupCreate("sx-nack", "g", GroupStartId.At(StreamId(0L, 0L))) >>
      client.xReadGroup[String, String]("g", "c1")(("sx-nack", GroupReadId.New))() >>
      client.xNack("sx-nack", "g", NackMode.Fail)(StreamId(1L, 0L))().is(1L) >>
      client.xPending("sx-nack", "g").map(_.total).is(1L)
  }
}

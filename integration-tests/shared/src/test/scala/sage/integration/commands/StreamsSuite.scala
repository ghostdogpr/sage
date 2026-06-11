package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.*
import sage.integration.{Images, ServerSuite}

abstract class StreamsSuite(image: String) extends ServerSuite(image) {

  test("XADD XLEN XRANGE and XREVRANGE round-trip entries, preserving field order") {
    withClient { client =>
      for {
        id1 <- client.xAdd("s-range", XAddId.Explicit(StreamId(1L, 0L)))(("a", "1"), ("b", "2"))
        id2 <- client.xAdd("s-range", XAddId.Explicit(StreamId(2L, 0L)))(("c", "3"))
        len <- client.xLen("s-range")
        all <- client.xRange[String, String, String]("s-range")
        rev <- client.xRevRange[String, String, String]("s-range")
        one <- client.xRange[String, String, String]("s-range", StreamRangeId.Exclusive(StreamId(1L, 0L)))
      } yield {
        assertEquals(id1, StreamId(1L, 0L))
        assertEquals(id2, StreamId(2L, 0L))
        assertEquals(len, 2L)
        assertEquals(all, Vector(StreamEntry(StreamId(1L, 0L), Vector("a" -> "1", "b" -> "2")), StreamEntry(StreamId(2L, 0L), Vector("c" -> "3"))))
        assertEquals(rev.map(_.id), Vector(StreamId(2L, 0L), StreamId(1L, 0L)))
        assertEquals(one.map(_.id), Vector(StreamId(2L, 0L)))
      }
    }
  }

  test("XADD with * generates increasing ids, NOMKSTREAM declines a missing stream, and XDEL removes") {
    withClient { client =>
      for {
        absent <- client.xAddNoMkStream("s-del-missing")(("f", "v"))
        a      <- client.xAdd("s-del")(("f", "1"))
        b      <- client.xAdd("s-del")(("f", "2"))
        del    <- client.xDel("s-del")(a)
        len    <- client.xLen("s-del")
      } yield {
        assertEquals(absent, None)
        assert(b > a)
        assertEquals(del, 1L)
        assertEquals(len, 1L)
      }
    }
  }

  test("XTRIM MAXLEN caps the stream length") {
    withClient { client =>
      for {
        _   <- client.xAdd("s-trim", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
        _   <- client.xAdd("s-trim", XAddId.Explicit(StreamId(2L, 0L)))(("f", "2"))
        _   <- client.xAdd("s-trim", XAddId.Explicit(StreamId(3L, 0L)))(("f", "3"))
        cut <- client.xTrim("s-trim", Trimming.Exact(TrimThreshold.MaxLen(1L)))
        len <- client.xLen("s-trim")
      } yield {
        assertEquals(cut, 2L)
        assertEquals(len, 1L)
      }
    }
  }

  test("XINFO STREAM reports length and the last generated id") {
    withClient { client =>
      for {
        _    <- client.xAdd("s-info", XAddId.Explicit(StreamId(1L, 0L)))(("f", "v"))
        _    <- client.xAdd("s-info", XAddId.Explicit(StreamId(5L, 0L)))(("g", "w"))
        info <- client.xInfoStream[String, String, String]("s-info")
      } yield {
        assertEquals(info.length, 2L)
        assertEquals(info.lastGeneratedId, StreamId(5L, 0L))
        assertEquals(info.firstEntry, Some(StreamEntry(StreamId(1L, 0L), Vector("f" -> "v"))))
      }
    }
  }

  test("a consumer group reads new entries, acknowledges them, and reports an empty PEL") {
    withClient { client =>
      for {
        _       <- client.xAdd("s-grp", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
        _       <- client.xAdd("s-grp", XAddId.Explicit(StreamId(2L, 0L)))(("f", "2"))
        _       <- client.xGroupCreate("s-grp", "g", GroupStartId.At(StreamId(0L, 0L)))
        read    <- client.xReadGroup[String, String, String]("g", "c1")(("s-grp", GroupReadId.New))(count = Some(10L))
        pending <- client.xPending("s-grp", "g")
        acked   <- client.xAck("s-grp", "g")(StreamId(1L, 0L), StreamId(2L, 0L))
        drained <- client.xPending("s-grp", "g")
        groups  <- client.xInfoGroups("s-grp")
      } yield {
        assertEquals(
          read,
          Vector("s-grp" -> Vector(StreamEntry(StreamId(1L, 0L), Vector("f" -> "1")), StreamEntry(StreamId(2L, 0L), Vector("f" -> "2"))))
        )
        assertEquals(pending.total, 2L)
        assertEquals(acked, 2L)
        assertEquals(drained.total, 0L)
        assertEquals(groups.map(_.name), Vector("g"))
      }
    }
  }

  test("XCLAIM and XAUTOCLAIM transfer pending entries to another consumer") {
    withClient { client =>
      for {
        _       <- client.xAdd("s-claim", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
        _       <- client.xGroupCreate("s-claim", "g", GroupStartId.At(StreamId(0L, 0L)))
        _       <- client.xReadGroup[String, String, String]("g", "c1")(("s-claim", GroupReadId.New))()
        claimed <- client.xClaim[String, String, String]("s-claim", "g", "c2", Duration.Zero)(StreamId(1L, 0L))()
        auto    <- client.xAutoClaim[String, String, String]("s-claim", "g", "c3", Duration.Zero)
        owners  <- client.xPendingExtended("s-claim", "g")
      } yield {
        assertEquals(claimed, Vector(StreamEntry(StreamId(1L, 0L), Vector("f" -> "1"))))
        assertEquals(auto.entries.map(_.id), Vector(StreamId(1L, 0L)))
        assertEquals(owners.map(_.consumer), Vector("c3"))
      }
    }
  }

  test("XINFO STREAM FULL decodes the group PEL after a consumer has read but not acked") {
    withClient { client =>
      for {
        _    <- client.xAdd("s-full", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
        _    <- client.xGroupCreate("s-full", "g", GroupStartId.At(StreamId(0L, 0L)))
        _    <- client.xReadGroup[String, String, String]("g", "c1")(("s-full", GroupReadId.New))()
        full <- client.xInfoStreamFull[String, String, String]("s-full")
      } yield {
        assertEquals(full.length, 1L)
        assertEquals(full.groups.map(_.name), Vector("g"))
        assertEquals(full.groups.head.pending.map(_.id), Vector(StreamId(1L, 0L)))
        assertEquals(full.groups.head.pending.head.consumer, Some("c1"))
        assertEquals(full.groups.head.consumers.head.pending.map(_.id), Vector(StreamId(1L, 0L)))
      }
    }
  }

  test("XREAD with BLOCK does not stall ordinary commands on the multiplexed connection") {
    withClient { client =>
      for {
        _   <- client.xAdd("s-block", XAddId.Explicit(StreamId(1L, 0L)))(("f", "0"))
        out <- CIO.zip(
                 client.xRead[String, String, String](("s-block", ReadId.After(StreamId(1L, 0L))))(block = Some(BlockTimeout.After(5.seconds))),
                 for {
                   pong <- client.ping()
                   _    <- client.xAdd("s-block", XAddId.Explicit(StreamId(2L, 0L)))(("f", "1"))
                 } yield pong
               )
      } yield {
        val (read, pong) = out
        assertEquals(pong, "PONG")
        assertEquals(read, Vector("s-block" -> Vector(StreamEntry(StreamId(2L, 0L), Vector("f" -> "1")))))
      }
    }
  }
}

class RedisStreamsSuite extends StreamsSuite(Images.redis)

class ValkeyStreamsSuite extends StreamsSuite(Images.valkey)

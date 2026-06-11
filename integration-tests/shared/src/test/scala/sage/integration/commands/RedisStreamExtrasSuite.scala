package sage.integration.commands

import scala.concurrent.duration.*

import kyo.compat.*

import sage.commands.*
import sage.integration.{Images, ServerSuite}

/**
  * XDELEX/XACKDEL (8.2), XNACK (8.8) and XCFGSET are Redis-only stream commands absent in Valkey, so they have no cross-server counterpart
  * (ADR-0026).
  */
class RedisStreamExtrasSuite extends ServerSuite(Images.redis) {

  test("XCFGSET sets per-stream idempotent-message-processing config") {
    withClient { client =>
      for {
        _    <- client.xAdd("sx-cfg", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
        both <- client.xCfgSet("sx-cfg", idmpDuration = Some(1.hour), idmpMaxSize = Some(100L))
        one  <- client.xCfgSet("sx-cfg", idmpMaxSize = Some(50L))
      } yield {
        assertEquals(both, ())
        assertEquals(one, ())
      }
    }
  }

  test("XDELEX reports per-id deletion status") {
    withClient { client =>
      for {
        _      <- client.xAdd("sx-delex", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
        _      <- client.xAdd("sx-delex", XAddId.Explicit(StreamId(2L, 0L)))(("f", "2"))
        status <- client.xDelEx("sx-delex")(StreamId(1L, 0L), StreamId(9L, 0L))
        len    <- client.xLen("sx-delex")
      } yield {
        assertEquals(status, Vector(StreamEntryDeletion.Deleted, StreamEntryDeletion.NotFound))
        assertEquals(len, 1L)
      }
    }
  }

  test("XACKDEL acknowledges and deletes in one step") {
    withClient { client =>
      for {
        _      <- client.xAdd("sx-ackdel", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
        _      <- client.xGroupCreate("sx-ackdel", "g", GroupStartId.At(StreamId(0L, 0L)))
        _      <- client.xReadGroup[String, String, String]("g", "c1")(("sx-ackdel", GroupReadId.New))()
        status <- client.xAckDel("sx-ackdel", "g")(StreamId(1L, 0L))
        len    <- client.xLen("sx-ackdel")
        pend   <- client.xPending("sx-ackdel", "g")
      } yield {
        assertEquals(status, Vector(StreamEntryDeletion.Deleted))
        assertEquals(len, 0L)
        assertEquals(pend.total, 0L)
      }
    }
  }

  test("XNACK releases a pending entry back to the group") {
    withClient { client =>
      for {
        _        <- client.xAdd("sx-nack", XAddId.Explicit(StreamId(1L, 0L)))(("f", "1"))
        _        <- client.xGroupCreate("sx-nack", "g", GroupStartId.At(StreamId(0L, 0L)))
        _        <- client.xReadGroup[String, String, String]("g", "c1")(("sx-nack", GroupReadId.New))()
        released <- client.xNack("sx-nack", "g", NackMode.Fail)(StreamId(1L, 0L))()
        pend     <- client.xPending("sx-nack", "g")
      } yield {
        assertEquals(released, 1L)
        assertEquals(pend.total, 1L)
      }
    }
  }
}

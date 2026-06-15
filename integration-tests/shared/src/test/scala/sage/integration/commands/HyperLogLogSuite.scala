package sage.integration.commands

import kyo.compat.*

import sage.integration.{Images, ServerSuite}

abstract class HyperLogLogSuite(image: String) extends ServerSuite(image) {

  test("PFADD reports change and PFCOUNT estimates cardinality") {
    withClient { client =>
      for {
        first <- client.pfAdd("hll", "a", "b", "c")
        again <- client.pfAdd("hll", "a")
        count <- client.pfCount("hll")
        empty <- client.pfAdd[String]("hll-empty")
      } yield {
        assertEquals(first, true)
        assertEquals(again, false)
        assertEquals(count, 3L)
        assertEquals(empty, true)
      }
    }
  }

  test("PFMERGE unions HyperLogLogs and PFCOUNT spans multiple keys") {
    withClient { client =>
      for {
        _      <- client.pfAdd("hll-a", "a", "b", "c")
        _      <- client.pfAdd("hll-b", "c", "d", "e")
        _      <- client.pfMerge("hll-merged", "hll-a", "hll-b")
        merged <- client.pfCount("hll-merged")
        both   <- client.pfCount("hll-a", "hll-b")
      } yield {
        assertEquals(merged, 5L)
        assertEquals(both, 5L)
      }
    }
  }
}

class RedisHyperLogLogSuite extends HyperLogLogSuite(Images.redis)

class ValkeyHyperLogLogSuite extends HyperLogLogSuite(Images.valkey)

package sage.integration.commands

import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.integration.{Images, ServerSuite}

abstract class PubsubSuite(image: String) extends ServerSuite(image) {

  // a subscribe blocks until the server confirms it, but UNSUBSCRIBE is fire-and-forget, so let it propagate before re-checking PUBSUB
  private def settle: CIO[Unit] = CIO.blocking(Thread.sleep(300))

  test("PUBLISH delivers to a channel subscriber; PUBSUB introspection reflects the subscription") {
    withClient { client =>
      for {
        sub      <- client.subscribeChannels[String]("news")
        channels <- client.pubsubChannels()
        numSub   <- client.pubsubNumSub("news")
        numPat   <- client.pubsubNumPat
        received <- client.publish("news", "hello")
        first    <- sub.next
        _        <- client.publish("news", "world")
        second   <- sub.next
        _        <- sub.close
      } yield {
        assert(channels.contains("news"), channels)
        assertEquals(numSub, Map("news" -> 1L))
        assertEquals(numPat, 0L)
        assertEquals(received, 1L)
        assertEquals(first, Some(Message("news", "hello")))
        assertEquals(second, Some(Message("news", "world")))
      }
    }
  }

  test("PSUBSCRIBE matches by pattern, naming the pattern and the concrete channel; PUBSUB NUMPAT counts it") {
    withClient { client =>
      for {
        sub    <- client.subscribePatterns[String]("news.*")
        numPat <- client.pubsubNumPat
        _      <- client.publish("news.sports", "goal")
        msg    <- sub.next
        _      <- sub.close
      } yield {
        assertEquals(numPat, 1L)
        assertEquals(msg, Some(PatternMessage("news.*", "news.sports", "goal")))
      }
    }
  }

  test("SSUBSCRIBE delivers a sharded message; SPUBLISH returns the receiver count; PUBSUB SHARDCHANNELS reflects it") {
    withClient { client =>
      for {
        sub      <- client.subscribeShardChannels[String]("orders")
        channels <- client.pubsubShardChannels()
        numSub   <- client.pubsubShardNumSub("orders")
        received <- client.sPublish("orders", "placed")
        first    <- sub.next
        _        <- sub.close
      } yield {
        assert(channels.contains("orders"), channels)
        assertEquals(numSub, Map("orders" -> 1L))
        assertEquals(received, 1L)
        assertEquals(first, Some(Message("orders", "placed")))
      }
    }
  }

  test("closing the last subscriber unsubscribes on the server") {
    withClient { client =>
      for {
        sub      <- client.subscribeChannels[String]("bye")
        active   <- client.pubsubChannels()
        _        <- sub.close
        _        <- settle
        inactive <- client.pubsubChannels()
      } yield {
        assert(active.contains("bye"), active)
        assert(!inactive.contains("bye"), inactive)
      }
    }
  }
}

class RedisPubsubSuite extends PubsubSuite(Images.redis)

class ValkeyPubsubSuite extends PubsubSuite(Images.valkey)

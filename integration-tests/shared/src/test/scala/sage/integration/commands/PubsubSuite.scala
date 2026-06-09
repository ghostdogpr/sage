package sage.integration.commands

import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.integration.{Images, ServerSuite}

abstract class PubsubSuite(image: String) extends ServerSuite(image) {

  // SUBSCRIBE rides the Subscription Connection while PUBLISH rides the Multiplexed one, so let the subscription register before publishing
  private def settle: CIO[Unit] = CIO.blocking(Thread.sleep(300))

  test("PUBLISH delivers to a channel subscriber; PUBSUB introspection reflects the subscription") {
    withClient { client =>
      for {
        sub      <- client.subscribeChannels[String]("news")
        _        <- settle
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
        _      <- settle
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

  test("closing the last subscriber unsubscribes on the server") {
    withClient { client =>
      for {
        sub      <- client.subscribeChannels[String]("bye")
        _        <- settle
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

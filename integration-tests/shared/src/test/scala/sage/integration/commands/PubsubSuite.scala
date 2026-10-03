package sage.integration.commands

import sage.{Message, PatternMessage}
import sage.integration.{BothServersSuite, Eventually}

class PubsubSuite extends BothServersSuite {

  clientTest("PUBLISH delivers to a channel subscriber; PUBSUB introspection reflects the subscription") { client =>
    withSubscription(client.subscribeChannels[String]("news")) { sub =>
      client.pubsubChannels().satisfies(_.contains("news")) >>
        client.pubsubNumSub("news").is(Map("news" -> 1L)) >>
        client.pubsubNumPat.is(0L) >>
        client.publish("news", "hello").is(1L) >>
        sub.next.is(Some(Message("news", "hello"))) >>
        client.publish("news", "world") >>
        sub.next.is(Some(Message("news", "world")))
    }
  }

  clientTest("PSUBSCRIBE matches by pattern, naming the pattern and the concrete channel; PUBSUB NUMPAT counts it") { client =>
    withSubscription(client.subscribePatterns[String]("news.*")) { sub =>
      client.pubsubNumPat.is(1L) >> client.publish("news.sports", "goal") >> sub.next.is(Some(PatternMessage("news.*", "news.sports", "goal")))
    }
  }

  clientTest("SSUBSCRIBE delivers a sharded message; SPUBLISH returns the receiver count; PUBSUB SHARDCHANNELS reflects it") { client =>
    withSubscription(client.subscribeShardChannels[String]("orders")) { sub =>
      client.pubsubShardChannels().satisfies(_.contains("orders")) >>
        client.pubsubShardNumSub("orders").is(Map("orders" -> 1L)) >>
        client.sPublish("orders", "placed").is(1L) >>
        sub.next.is(Some(Message("orders", "placed")))
    }
  }

  clientTest("closing the last subscriber unsubscribes on the server") { client =>
    withSubscription(client.subscribeChannels[String]("bye"))(_ => client.pubsubChannels().satisfies(_.contains("bye"))) >>
      // a subscribe blocks until the server confirms it, but UNSUBSCRIBE is fire-and-forget, so poll until it propagates
      Eventually(50)(client.pubsubChannels().satisfies(!_.contains("bye")))
  }
}

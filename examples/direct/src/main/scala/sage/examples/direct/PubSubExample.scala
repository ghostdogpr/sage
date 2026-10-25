package sage.examples.direct

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.util.Using

import sage.*
import sage.backend.*

/**
  * Classic channel pub/sub. `subscribe` returns a blocking iterator, and closing it unsubscribes. `onMessage` calls a function for each message
  * on a background virtual thread until the returned handle is closed. The cluster example covers sharded pub/sub.
  */
object PubSubExample {

  def run(client: SageClient): Unit = {
    // the subscription is confirmed when subscribe returns, so the messages published next are delivered
    val messages = Using.resource(client.subscribe[String]("news")) { news =>
      (1 to 3).foreach(i => client.publish("news", s"item-$i"))
      news.take(3).map(_.payload).toList
    }
    println(s"received=$messages")

    val alerts = new LinkedBlockingQueue[String]()
    Using.resource(client.onMessage[String]("alerts")(message => alerts.put(message.payload))) { _ =>
      client.publish("alerts", "disk almost full")
      println(s"alert=${alerts.poll(5, TimeUnit.SECONDS)}")
    }
  }
}

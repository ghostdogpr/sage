package sage.examples.ox

import ox.Ox

import sage.*
import sage.backend.*

/**
  * Classic channel pub/sub with an Ox `Flow`. Ending the flow unsubscribes. The cluster example covers sharded pub/sub.
  */
object PubSubExample {

  def run(client: SageClient)(using Ox): Unit = {
    // wait for the subscription to be confirmed before publishing.
    val news     = client.subscribeScoped[String]("news")
    (1 to 3).foreach { i =>
      client.publish("news", s"item-$i")
    }
    val messages = news.take(3).runToList()
    println(s"received=${messages.map(_.payload)}")
  }
}

package sage.examples.ox

import ox.Ox

import sage.*
import sage.backend.*

/**
  * Classic channel pub/sub surfaced as a native Ox `Flow`; ending the flow unsubscribes. Sharded pub/sub is shown in the cluster spotlight,
  * where it belongs.
  */
object PubSubExample {

  def run(client: SageClient)(using Ox): Unit = {
    // subscribeScoped confirms before returning, so the publishes below can't outrun the registration
    val news     = client.subscribeScoped[String]("news")
    (1 to 3).foreach { i =>
      val _ = client.publish("news", s"item-$i")
    }
    val messages = news.take(3).runToList()
    println(s"received=${messages.map(_.payload)}")
  }
}

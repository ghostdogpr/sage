package sage.examples.ox

import ox.{fork, Ox}

import sage.*
import sage.ox.*

/**
  * Classic channel pub/sub surfaced as a native Ox `Flow`. The subscriber runs in a forked concurrency scope; ending the flow unsubscribes.
  * Sharded pub/sub is shown in the cluster spotlight, where it belongs.
  */
object PubSubExample {

  def run(client: SageClient)(using Ox): Unit = {
    val collector = fork(client.subscribe[String]("news").take(3).runToList())
    Thread.sleep(300) // let SUBSCRIBE register before publishing
    (1 to 3).foreach { i =>
      val _ = client.publish("news", s"item-$i")
    }
    val messages = collector.join()
    println(s"received=${messages.map(_.payload)}")
  }
}

package sage.examples.kyo

import kyo.*

import sage.*
import sage.backend.*

/**
  * Classic channel pub/sub with a Kyo `Stream`. The cluster example covers sharded pub/sub.
  */
object PubSubExample {

  def run(client: SageClient): Unit < (Scope & Abort[Throwable] & Async) =
    for {
      stream <- client.subscribeScoped[String]("news")
      _      <- Kyo.foreachDiscard(1 to 3)(i => client.publish("news", s"item-$i"))
      chunk  <- stream.take(3).run
      _      <- Console.printLine(s"received=${chunk.toList.map(_.payload)}")
    } yield ()
}

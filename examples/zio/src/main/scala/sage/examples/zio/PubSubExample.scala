package sage.examples.zio

import zio.*

import sage.*
import sage.zio.*

/**
  * Classic channel pub/sub surfaced as a native `ZStream`. The subscriber is forked first; closing the stream's scope unsubscribes. Sharded
  * pub/sub (`sSubscribe`/`sPublish`) is shown in the cluster spotlight, where it belongs.
  */
object PubSubExample {

  val run: ZIO[SageClient, Throwable, Unit] =
    ZIO.serviceWithZIO[SageClient] { client =>
      for {
        subscriber <- client.subscribe[String]("news").take(3).runCollect.fork
        _          <- ZIO.sleep(Duration.fromMillis(300)) // let SUBSCRIBE register before publishing
        _          <- ZIO.foreachDiscard(1 to 3)(i => client.publish("news", s"item-$i"))
        messages   <- subscriber.join
        _          <- Console.printLine(s"received=${messages.map(_.payload).toList}")
      } yield ()
    }
}

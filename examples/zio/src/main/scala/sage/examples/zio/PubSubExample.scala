package sage.examples.zio

import zio.*

import sage.*
import sage.backend.*

/**
  * Classic channel pub/sub with a `ZStream`. Closing the surrounding scope unsubscribes. The cluster example covers sharded pub/sub with
  * `sSubscribe` and `sPublish`.
  */
object PubSubExample {

  val run: ZIO[SageClient, Throwable, Unit] =
    ZIO.serviceWithZIO[SageClient] { client =>
      ZIO.scoped {
        for {
          stream   <- client.subscribeScoped[String]("news")
          _        <- ZIO.foreachDiscard(1 to 3)(i => client.publish("news", s"item-$i"))
          messages <- stream.take(3).runCollect
          _        <- Console.printLine(s"received=${messages.map(_.payload).toList}")
        } yield ()
      }
    }
}

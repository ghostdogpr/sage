package sage.examples.ce

import cats.effect.IO
import cats.syntax.all.*

import sage.*
import sage.backend.*

/**
  * Classic channel pub/sub with an fs2 `Stream`. Releasing the resource unsubscribes. The cluster example covers sharded pub/sub.
  */
object PubSubExample {

  def run(client: SageClient): IO[Unit] =
    client.subscribeResource[String]("news").use { stream =>
      for {
        _        <- (1 to 3).toList.traverse_(i => client.publish("news", s"item-$i"))
        messages <- stream.take(3).compile.toVector
        _        <- IO.println(s"received=${messages.map(_.payload).toList}")
      } yield ()
    }
}

package sage.examples.ce

import scala.concurrent.duration.*

import cats.effect.IO
import cats.syntax.all.*

import sage.*
import sage.ce.*

/**
  * Classic channel pub/sub surfaced as a native fs2 `Stream`. The subscriber is started first; ending the stream cancels the subscription.
  * Sharded pub/sub is shown in the cluster spotlight, where it belongs.
  */
object PubSubExample {

  def run(client: SageClient): IO[Unit] =
    for {
      subscriber <- client.subscribe[String]("news").take(3).compile.toVector.start
      _          <- IO.sleep(300.millis) // let SUBSCRIBE register before publishing
      _          <- (1 to 3).toList.traverse_(i => client.publish("news", s"item-$i"))
      messages   <- subscriber.joinWithNever
      _          <- IO.println(s"received=${messages.map(_.payload).toList}")
    } yield ()
}

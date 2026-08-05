package sage.examples.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.{Materializer, SystemMaterializer}
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import sage.*
import sage.backend.*

/**
  * Classic channel pub/sub with a Pekko Streams `Source`. The materialized `Future[Done]` completes when the subscription is confirmed.
  * Waiting for it before publishing avoids missing the first messages. Cancelling the stream unsubscribes. The cluster example covers
  * sharded pub/sub.
  */
object PubSubExample {

  def run(client: SageClient)(using system: ActorSystem[?], ec: ExecutionContext): Future[Unit] = {
    given Materializer        = SystemMaterializer(system).materializer
    val (confirmed, received) =
      client.subscribe[String]("news").take(3).toMat(Sink.seq)(Keep.both).run()
    for {
      _        <- confirmed
      _        <- Future.traverse(1 to 3)(i => client.publish("news", s"item-$i"))
      messages <- received
    } yield println(s"received=${messages.map(_.payload).toList}")
  }
}

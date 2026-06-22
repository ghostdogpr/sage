package sage.examples.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import sage.*
import sage.backend.*

/**
  * Classic channel pub/sub surfaced as a native Pekko Streams `Source`. The materialized `Future[Done]` resolves once the SUBSCRIBE is
  * confirmed, so the publishes below cannot race the registration; cancelling the stream (here, after `take(3)`) unsubscribes. Sharded
  * pub/sub is shown in the cluster spotlight, where it belongs.
  */
object PubSubExample {

  def run(client: SageClient)(using system: ActorSystem[?], ec: ExecutionContext): Future[Unit] = {
    given Materializer        = Materializer(system)
    val (confirmed, received) =
      client.subscribe[String]("news").take(3).toMat(Sink.seq)(Keep.both).run()
    for {
      _        <- confirmed
      _        <- Future.traverse(1 to 3)(i => client.publish("news", s"item-$i"))
      messages <- received
    } yield println(s"received=${messages.map(_.payload).toList}")
  }
}

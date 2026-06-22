package sage.examples.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import sage.*
import sage.backend.*

/**
  * A Pipeline is an applicative composition of Commands sent in one round-trip, yielding a typed tuple. `pipelineAttempt` keeps each
  * position's failure separate instead of failing the whole call.
  */
object PipelinesExample {

  def run(client: SageClient)(using ExecutionContext): Future[Unit] =
    for {
      _       <- client.set("pipe:a", "x")
      _       <- client.set("pipe:n", 10)
      tuple   <- client.pipeline((Commands.get[String, String]("pipe:a"), Commands.incrBy("pipe:n", 5)))
      _       <- client.set("pipe:str", "hello")
      // INCR on a non-numeric string fails only at its own position; the GET still succeeds
      attempt <- client.pipelineAttempt((Commands.get[String, String]("pipe:str"), Commands.incr("pipe:str")))
    } yield println(s"tuple=$tuple attempt=$attempt")
}

package sage.examples.ox

import ox.Ox

import sage.*
import sage.ox.*

/**
  * A Pipeline is an applicative composition of Commands sent in one round-trip, yielding a typed tuple. `pipelineAttempt` keeps each
  * position's failure separate instead of failing the whole call.
  */
object PipelinesExample {

  def run(client: SageClient)(using Ox): Unit = {
    val _       = client.set("pipe:a", "x")
    val _       = client.set("pipe:n", 10)
    val tuple   = client.pipeline((Commands.get[String, String]("pipe:a"), Commands.incrBy("pipe:n", 5)).pipeline)
    val _       = client.set("pipe:str", "hello")
    // INCR on a non-numeric string fails only at its own position; the GET still succeeds
    val attempt = client.pipelineAttempt((Commands.get[String, String]("pipe:str"), Commands.incr("pipe:str")).pipeline)
    println(s"tuple=$tuple attempt=$attempt")
  }
}

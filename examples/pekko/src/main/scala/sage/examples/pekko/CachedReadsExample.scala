package sage.examples.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.*

import sage.*
import sage.backend.*

/**
  * A Cached Read opts in to client-side caching per call: the first read fetches and caches, the second is served from the local cache until
  * a server invalidation push or the TTL evicts it.
  */
object CachedReadsExample {

  def run(client: SageClient)(using ExecutionContext): Future[Unit] =
    for {
      _      <- client.set("cached:key", "v1")
      first  <- client.cached(Commands.get[String, String]("cached:key"), 1.minute) // fetch + cache
      second <- client.cached(Commands.get[String, String]("cached:key"), 1.minute) // local hit
    } yield println(s"first=$first second=$second")
}

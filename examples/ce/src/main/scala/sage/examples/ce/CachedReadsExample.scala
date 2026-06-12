package sage.examples.ce

import scala.concurrent.duration.*

import cats.effect.IO

import sage.*
import sage.ce.*

/**
  * A Cached Read opts in to client-side caching per call: the first read fetches and caches, the second is served from the local cache until
  * a server invalidation push or the TTL evicts it.
  */
object CachedReadsExample {

  def run(client: SageClient): IO[Unit] =
    for {
      _      <- client.set("cached:key", "v1")
      first  <- client.cached(Commands.get[String, String]("cached:key"), 1.minute) // fetch + cache
      second <- client.cached(Commands.get[String, String]("cached:key"), 1.minute) // local hit
      _      <- IO.println(s"first=$first second=$second")
    } yield ()
}

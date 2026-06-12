package sage.examples.kyo

import kyo.*

import sage.*
import sage.kyo.*

/**
  * A Cached Read opts in to client-side caching per call: the first read fetches and caches, the second is served from the local cache until
  * a server invalidation push or the TTL evicts it. The Kyo-native `cached` takes a `kyo.Duration`.
  */
object CachedReadsExample {

  def run(client: SageClient): Unit < (Abort[Throwable] & Async) =
    for {
      _      <- client.set("cached:key", "v1")
      first  <- client.cached(Commands.get[String, String]("cached:key"), 1.minute) // fetch + cache
      second <- client.cached(Commands.get[String, String]("cached:key"), 1.minute) // local hit
      _      <- Console.printLine(s"first=$first second=$second")
    } yield ()
}

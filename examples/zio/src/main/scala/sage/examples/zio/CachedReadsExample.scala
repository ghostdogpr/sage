package sage.examples.zio

import zio.*

import sage.*
import sage.zio.*

/**
  * A Cached Read opts in to client-side caching per call: the first read fetches and caches, the second is served from the local cache until
  * a server invalidation push or the TTL evicts it. The ZIO-native `cached` takes a `zio.Duration`.
  */
object CachedReadsExample {

  val run: ZIO[SageClient, Throwable, Unit] =
    ZIO.serviceWithZIO[SageClient] { client =>
      for {
        _      <- client.set("cached:key", "v1")
        first  <- client.cached(Commands.get[String, String]("cached:key"), 1.minute) // fetch + cache
        second <- client.cached(Commands.get[String, String]("cached:key"), 1.minute) // local hit
        _      <- Console.printLine(s"first=$first second=$second")
      } yield ()
    }
}

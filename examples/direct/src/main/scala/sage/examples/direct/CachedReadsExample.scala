package sage.examples.direct

import scala.concurrent.duration.*

import sage.*
import sage.backend.*

/**
  * A Cached Read opts in to client-side caching per call: the first read fetches and caches, the second is served from the local cache until
  * a server invalidation push or the TTL evicts it.
  */
object CachedReadsExample {

  def run(client: SageClient): Unit = {
    client.set("cached:key", "v1")
    val first  = client.cached(Commands.get[String, String]("cached:key"), 1.minute) // fetch + cache
    val second = client.cached(Commands.get[String, String]("cached:key"), 1.minute) // local hit
    println(s"first=$first second=$second")
  }
}

package sage.examples.ox

import scala.concurrent.duration.*

import ox.Ox

import sage.*
import sage.ox.*

/**
  * A Cached Read opts in to client-side caching per call: the first read fetches and caches, the second is served from the local cache until
  * a server invalidation push or the TTL evicts it.
  */
object CachedReadsExample {

  def run(client: SageClient)(using Ox): Unit = {
    val _      = client.set("cached:key", "v1")
    val first  = client.cached(Commands.get[String, String]("cached:key"), 1.minute) // fetch + cache
    val second = client.cached(Commands.get[String, String]("cached:key"), 1.minute) // local hit
    println(s"first=$first second=$second")
  }
}

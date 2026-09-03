package sage.examples.ox

import scala.concurrent.duration.*

import ox.Ox

import sage.backend.*

/**
  * Serializes a counter update across processes that use the same lock namespace and key.
  */
object LockExample {
  def run(client: SageClient)(using Ox): Unit = {
    val locks = client.lock[String](namespace = "example-locks")
    val next  = locks.withLock("counter", waitTimeout = 2.seconds) {
      val current = client.get[Int]("example:locked-counter")
      val next    = current.getOrElse(0) + 1
      client.set("example:locked-counter", next)
      next
    }
    println(s"Updated counter to $next")
  }
}

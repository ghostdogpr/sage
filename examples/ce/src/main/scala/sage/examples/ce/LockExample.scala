package sage.examples.ce

import scala.concurrent.duration.*

import cats.effect.IO

import sage.backend.*

/**
  * Serializes a counter update across processes that use the same lock namespace and key.
  */
object LockExample {
  def run(client: SageClient): IO[Unit] = {
    val locks = client.lock[String](namespace = "example-locks")
    for {
      next <- locks.withLock("counter", waitTimeout = 2.seconds) {
                for {
                  current <- client.get[Int]("example:locked-counter")
                  next     = current.getOrElse(0) + 1
                  _       <- client.set("example:locked-counter", next)
                } yield next
              }
      _    <- IO.println(s"Updated counter to $next")
    } yield ()
  }
}

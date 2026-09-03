package sage.examples.zio

import scala.concurrent.duration.*

import zio.*

import sage.backend.*

/**
  * Serializes a counter update across processes that use the same lock namespace and key.
  */
object LockExample {
  val run: ZIO[SageClient, Throwable, Unit] = ZIO.serviceWithZIO[SageClient] { client =>
    val locks = client.lock[String](namespace = "example-locks")
    for {
      next <- locks.withLock("counter", waitTimeout = 2.seconds) {
                for {
                  current <- client.get[Int]("example:locked-counter")
                  next     = current.getOrElse(0) + 1
                  _       <- client.set("example:locked-counter", next)
                } yield next
              }
      _    <- Console.printLine(s"Updated counter to $next")
    } yield ()
  }
}

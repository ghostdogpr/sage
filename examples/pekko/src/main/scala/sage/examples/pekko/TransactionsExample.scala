package sage.examples.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import sage.*
import sage.backend.*

/**
  * A WATCH-guarded MULTI/EXEC transaction on one leased Dedicated Connection: read inside the scope, decide, then `exec` a Pipeline
  * atomically. A `None` result means a watched key changed before EXEC, the normal optimistic-concurrency retry signal, not a failure.
  */
object TransactionsExample {

  def run(client: SageClient)(using ExecutionContext): Future[Unit] =
    for {
      _      <- client.set("tx:n", 1)
      result <- client.transaction { tx =>
                  for {
                    _   <- tx.watch("tx:n")
                    _   <- tx.get[Int]("tx:n")
                    res <- tx.exec((Commands.incr("tx:n"), Commands.incrBy("tx:n", 4)))
                  } yield res
                }
    } yield println(s"transaction result=$result")
}

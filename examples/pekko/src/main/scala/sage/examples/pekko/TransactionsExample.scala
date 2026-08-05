package sage.examples.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import sage.*
import sage.backend.*

/**
  * Runs a WATCH-guarded MULTI/EXEC transaction on one leased dedicated connection. It reads inside the transaction scope, decides what to do,
  * then executes a Pipeline atomically. If a watched key changes before EXEC, the transaction returns `None` and the caller should retry.
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

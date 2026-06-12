package sage.examples.kyo

import kyo.*

import sage.*
import sage.kyo.*

/**
  * A WATCH-guarded MULTI/EXEC transaction on one leased Dedicated Connection: read inside the scope, decide, then `exec` a Pipeline
  * atomically. A `None` result means a watched key changed before EXEC — the normal optimistic-concurrency retry signal, not a failure.
  */
object TransactionsExample {

  def run(client: SageClient): Unit < (Abort[Throwable] & Async) =
    for {
      _      <- client.set("tx:n", 1)
      result <- client.transaction { tx =>
                  for {
                    _   <- tx.watch("tx:n")
                    _   <- tx.get[String, Int]("tx:n")
                    res <- tx.exec((Commands.incr("tx:n"), Commands.incrBy("tx:n", 4)).pipeline)
                  } yield res
                }
      _      <- Console.printLine(s"transaction result=$result")
    } yield ()
}

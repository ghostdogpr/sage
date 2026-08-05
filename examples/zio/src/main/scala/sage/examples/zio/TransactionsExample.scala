package sage.examples.zio

import zio.*

import sage.*
import sage.backend.*

/**
  * Runs a WATCH-guarded MULTI/EXEC transaction on one leased dedicated connection. It reads inside the transaction scope, decides what to do,
  * then executes a Pipeline atomically. If a watched key changes before EXEC, the transaction returns `None` and the caller should retry.
  */
object TransactionsExample {

  val run: ZIO[SageClient, Throwable, Unit] =
    ZIO.serviceWithZIO[SageClient] { client =>
      for {
        _      <- client.set("tx:n", 1)
        result <- client.transaction { tx =>
                    for {
                      _   <- tx.watch("tx:n")
                      _   <- tx.get[Int]("tx:n")
                      res <- tx.exec((Commands.incr("tx:n"), Commands.incrBy("tx:n", 4)))
                    } yield res
                  }
        _      <- Console.printLine(s"transaction result=$result")
      } yield ()
    }
}

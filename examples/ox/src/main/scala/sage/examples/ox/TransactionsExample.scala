package sage.examples.ox

import ox.Ox

import sage.*
import sage.backend.*

/**
  * Runs a WATCH-guarded MULTI/EXEC transaction on one leased dedicated connection. It reads inside the transaction scope, decides what to do,
  * then executes a Pipeline atomically. If a watched key changes before EXEC, the transaction returns `None` and the caller should retry.
  */
object TransactionsExample {

  def run(client: SageClient)(using Ox): Unit = {
    client.set("tx:n", 1)
    val result = client.transaction { tx =>
      tx.watch("tx:n")
      tx.get[Int]("tx:n")
      tx.exec((Commands.incr("tx:n"), Commands.incrBy("tx:n", 4)))
    }
    println(s"transaction result=$result")
  }
}

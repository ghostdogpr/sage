package sage.examples.ox

import ox.Ox

import sage.*
import sage.ox.*

/**
  * A WATCH-guarded MULTI/EXEC transaction on one leased Dedicated Connection: read inside the scope, decide, then `exec` a Pipeline
  * atomically. A `None` result means a watched key changed before EXEC — the normal optimistic-concurrency retry signal, not a failure.
  */
object TransactionsExample {

  def run(client: SageClient)(using Ox): Unit = {
    val _      = client.set("tx:n", 1)
    val result = client.transaction { tx =>
      val _ = tx.watch("tx:n")
      val _ = tx.get[String, Int]("tx:n")
      tx.exec((Commands.incr("tx:n"), Commands.incrBy("tx:n", 4)).pipeline)
    }
    println(s"transaction result=$result")
  }
}

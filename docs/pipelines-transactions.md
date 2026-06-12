# Pipelines & transactions

Both group several commands together, but they answer different needs. A **pipeline** is about throughput: many commands, one round-trip, no atomicity. A **transaction** is about atomicity: the grouped commands run as a unit, optionally guarded against concurrent change.

## Pipelines

A pipeline is an applicative composition of `Command` values, sent in one round-trip and decoded into a typed tuple. There is no atomicity: other clients' commands may interleave, and in a cluster the pipeline is split and routed per key, then reassembled in order.

::: code-group

```scala [Ox]
client.set("pipe:a", "x")
client.set("pipe:n", 10)
val tuple = client.pipeline(
  (
    Commands.get[String, String]("pipe:a"),
    Commands.incrBy("pipe:n", 5)
  ).pipeline
)
// tuple: (Option[String], Long)
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _     <- client.set("pipe:a", "x")
  _     <- client.set("pipe:n", 10)
  tuple <- client.pipeline(
             (
               Commands.get[String, String]("pipe:a"),
               Commands.incrBy("pipe:n", 5)
             ).pipeline
           )
} yield tuple // (Option[String], Long)
```

:::

By default a pipeline fails as a whole if any position fails. Use `pipelineAttempt` to keep each position's outcome separate, so one failing command does not sink the others:

::: code-group

```scala [Ox]
client.set("pipe:str", "hello")
// INCR on a non-numeric string fails only at its own position;
// the GET still succeeds
val attempt = client.pipelineAttempt(
  (
    Commands.get[String, String]("pipe:str"),
    Commands.incr("pipe:str")
  ).pipeline
)
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _       <- client.set("pipe:str", "hello")
  // INCR on a non-numeric string fails only at its own position;
  // the GET still succeeds
  attempt <- client.pipelineAttempt(
               (
                 Commands.get[String, String]("pipe:str"),
                 Commands.incr("pipe:str")
               ).pipeline
             )
} yield attempt
```

:::

## Transactions

A transaction runs a pipeline atomically via `MULTI`/`EXEC` on a leased dedicated connection. Open one with `transaction { tx => … }`: inside the scope you may `watch` keys, run ordinary reads (`tx.get`, `tx.run`, …), decide, and then `exec` a pipeline (or abandon the scope to discard it).

`exec` returns an `Option`. A `None` means a watched key changed before `EXEC`, so the transaction did not run. That is the normal optimistic-concurrency outcome you retry, not a failure:

::: code-group

```scala [Ox]
client.set("tx:n", 1)
val result = client.transaction { tx =>
  tx.watch("tx:n")
  tx.get[String, Int]("tx:n")
  tx.exec(
    (Commands.incr("tx:n"), Commands.incrBy("tx:n", 4)).pipeline
  )
}
// result: Some((2, 6)), or None if "tx:n" changed before EXEC
```

```scala [ZIO · Cats Effect · Kyo]
for {
  _      <- client.set("tx:n", 1)
  result <- client.transaction { tx =>
              for {
                _   <- tx.watch("tx:n")
                _   <- tx.get[String, Int]("tx:n")
                res <- tx.exec(
                         (
                           Commands.incr("tx:n"),
                           Commands.incrBy("tx:n", 4)
                         ).pipeline
                       )
              } yield res
            }
} yield result // Some((2, 6)), or None if "tx:n" changed
```

:::

A few rules follow from how Redis transactions work:

- **Reads inside the scope must be ordinary commands.** A blocking command is rejected rather than parking the lease.
- **A queueing-phase rejection discards the whole transaction**, so nothing runs.
- **An execution-phase error leaves the other commands committed.** Redis does not roll back, so those errors surface per position, like a pipeline.
- **In a cluster, every key in the transaction must hash to one slot** (use a [hash tag](/configuration) to force that). A pipeline has no such restriction.

## Which to use

Reach for a **pipeline** when the commands are independent and you only want fewer round-trips. Reach for a **transaction** when you need read-decide-commit on one connection, or all-or-nothing execution guarded by `WATCH`.

# Pipelines & transactions

Pipelines and transactions both group several commands, but they serve different purposes. A **pipeline** improves throughput by sending many commands in one round trip, without atomicity. A **transaction** runs the grouped commands as a unit and can protect them from concurrent changes.

## Pipelines

A pipeline sends several `Command` values in one round trip and returns their results as a typed tuple. The commands are not atomic, so commands from other clients may run between them. In a cluster, Sage routes each command by key and restores the original result order. [Cross-slot commands](/configuration#supported-cross-slot-commands) work the same way inside and outside a pipeline.

::: code-group

```scala [Ox]
client.set("pipe:a", "x")
client.set("pipe:n", 10)
val tuple = client.pipeline(
  (
    Commands.get[String, String]("pipe:a"),
    Commands.incrBy("pipe:n", 5)
  )
)
// tuple: (Option[String], Long)
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _     <- client.set("pipe:a", "x")
  _     <- client.set("pipe:n", 10)
  tuple <- client.pipeline(
             (
               Commands.get[String, String]("pipe:a"),
               Commands.incrBy("pipe:n", 5)
             )
           )
} yield tuple // (Option[String], Long)
```

:::

A tuple gives a fixed-size result whose elements can have different types. When commands are built dynamically and share a result type, pass a `Seq[Command[A]]` instead and get back a `Vector[A]` in the same order. An empty `Seq` returns an empty result without contacting the server:

```scala
val ids = List("a", "b", "c")
client.pipeline(ids.map(id => Commands.get[String, String](id))) // F[Vector[Option[String]]]
```

By default, a pipeline fails as a whole if any command fails. Use `pipelineAttempt` to return each command's result separately:

::: code-group

```scala [Ox]
client.set("pipe:str", "hello")
// INCR on a non-numeric string fails only at its own position;
// the GET still succeeds
val attempt = client.pipelineAttempt(
  (
    Commands.get[String, String]("pipe:str"),
    Commands.incr("pipe:str")
  )
)
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _       <- client.set("pipe:str", "hello")
  // INCR on a non-numeric string fails only at its own position;
  // the GET still succeeds
  attempt <- client.pipelineAttempt(
               (
                 Commands.get[String, String]("pipe:str"),
                 Commands.incr("pipe:str")
               )
             )
} yield attempt
```

:::

In a cluster, a pipeline creates a separate batch for each node. It cannot include a [command that runs on every master](/configuration#commands-that-run-on-every-master). Sage rejects these commands before sending the pipeline; run them on the client directly.

## Transactions

A transaction runs a pipeline atomically with `MULTI`/`EXEC` on a temporary dedicated connection. Open one with `transaction { tx => … }`. Inside the scope you can `watch` keys, run ordinary reads (`tx.get`, `tx.run`, …), decide what to do, and then call `exec` with a pipeline. Leaving the scope without calling `exec` discards the transaction.

`exec` returns an `Option`. If a watched key changed before `EXEC`, the transaction does not run and returns `None`. This is an expected result that you can retry:

::: code-group

```scala [Ox]
client.set("tx:n", 1)
val result = client.transaction { tx =>
  tx.watch("tx:n")
  tx.get[Int]("tx:n")
  tx.exec(
    (Commands.incr("tx:n"), Commands.incrBy("tx:n", 4))
  )
}
// result: Some((2, 6)), or None if "tx:n" changed before EXEC
```

```scala [ZIO · Cats Effect · Kyo · Pekko]
for {
  _      <- client.set("tx:n", 1)
  result <- client.transaction { tx =>
              for {
                _   <- tx.watch("tx:n")
                _   <- tx.get[Int]("tx:n")
                res <- tx.exec(
                         (
                           Commands.incr("tx:n"),
                           Commands.incrBy("tx:n", 4)
                         )
                       )
              } yield res
            }
} yield result // Some((2, 6)), or None if "tx:n" changed
```

:::

A few rules follow from how Redis transactions work:

- **Reads inside the scope must be ordinary commands.** A blocking command is rejected rather than parking the lease.
- **A queueing-phase rejection discards the whole transaction**, so nothing runs.
- **An execution-phase error leaves the other commands committed.** Redis does not roll back. As with a pipeline, the error is reported for the individual command.
- **In a cluster, every key in the transaction must hash to one slot** (use a [hash tag](/configuration#hash-tags) to force that). A pipeline has no such restriction for commands with documented cross-slot support.
- **In a cluster, bound your retries.** A transaction never follows a redirect, since that would break atomicity, so you retry the whole block yourself, exactly as a `WATCH` abort already requires. While a slot is migrating no retry can commit until the migration finalizes, so retry with backoff and a ceiling rather than in a tight loop.

## Which to use

Use a **pipeline** when the commands are independent and you want fewer round trips. Use a **transaction** when the commands must run as a unit or when you need `WATCH` to protect them from concurrent changes.

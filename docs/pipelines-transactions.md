# Pipelines & transactions

<!--
WRITER NOTES — replace this body with real content.
Ground snippets in: examples/*/PipelinesExample.scala and TransactionsExample.scala.

Purpose: the two ways to group commands, and how they differ. The whole point of
this page is to make the Pipeline-vs-Transaction distinction crisp.

Sections to cover (grounded in CONTEXT.md glossary):

1. Pipeline — applicative composition of Commands sent in ONE round-trip, yielding a
   typed tuple. NOT a transaction — no atomicity. In a cluster a Pipeline may be split
   across nodes. Show building + executing, with the typed-tuple result.

2. Transaction — a Pipeline executed atomically via MULTI/EXEC on a Dedicated
   Connection, opened with `transaction { tx => … }` (the Transaction Scope).
   - Optional WATCH guard: a watched key changing before EXEC aborts the txn — a normal
     optimistic-concurrency outcome the caller RETRIES, not a failure.
   - Within the scope: watch, run reads via tx.get / tx.run, then exec a Pipeline (or
     discard). Reads must be ordinary commands; blocking commands are rejected.
   - Failure model: queueing-phase rejection discards the whole txn (nothing runs);
     execution-phase error leaves other commands committed (Redis does not roll back),
     surfaced per-position like a Pipeline.

3. When to use which — quick guidance: Pipeline for throughput (no atomicity needed),
   Transaction for read-decide-commit on one connection / optimistic concurrency.

Avoid calling a Pipeline a "batch" or a Transaction a "batch".
-->

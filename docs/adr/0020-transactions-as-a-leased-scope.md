# Transactions as a leased scope, abort as an outcome not an error

A Transaction leases one Dedicated Connection for a caller-supplied block — `transaction { scope => … }` — rather than executing a fixed `(watchKeys, Pipeline)` bundle. The bundled form cannot express correct optimistic concurrency: `WATCH` guards a key only against changes *after* the watch, so a real read-modify-write must `WATCH`, then read, then decide, then `EXEC`, all on the one connection `WATCH`'s state lives on. A single bundled call leaves no room to read-and-decide between `WATCH` and `MULTI`, so its guard has a time-of-check/time-of-use gap. ADR-0016 already deferred transactions to "a separate leased-connection API"; this is that API. `TransactionScope[F]` exposes `watch`/`run`/`exec`/`execAttempt`/`discard`; `transaction` is a fourth `Client` primitive alongside `run`/`pipeline`/`pipelineAttempt` (a fake now implements four).

`exec` returns `F[Option[Out]]`: `None` is a `WATCH`-abort. Abort is normal optimistic-concurrency control flow the caller retries, not a failure, so it stays out of the sealed `SageException` hierarchy (consistent with ADR-0019's reluctance to grow it) — an `Option` the retry loop pattern-matches, not an exception it must catch.

The two server-side error phases map to different places, reflecting that Redis does not roll back:

- **Queueing phase** — a command rejected after `MULTI` (unknown command, arity) flags the transaction dirty; `EXEC` replies `EXECABORT` and nothing runs. This fails the effect with a new sealed case, `SageException.TransactionDiscarded`, carrying the offending reply.
- **Execution phase** — every command queued, `EXEC` ran them all, and an individual command erred in place. The transaction committed; the error is per-position, exactly the Pipeline model (`exec` strict-collapses on the first; `execAttempt` yields `Left` slots). `Reply.run` already lets errors nested in an `EXEC` array reach decoders.

## Considered Options

- **Leased scope (chosen)** — the only shape that supports interactive read-modify-write on the connection `WATCH` requires.
- **Bundled `transaction(watch)(pipeline)`** — simplest, one round-trip, but cannot read between `WATCH` and `MULTI`; its guard has a TOCTOU gap, failing PRD story 11.
- **Structured RMW combinator** `transaction(watch)(read)(build)` — captures the one-read loop without leaking a handle, but can't express multi-step interactive transactions.
- **Abort as a sealed exception** — uniform with other failures, but conflates expected control flow with errors and forces catch-style retry.

## Consequences

- The lease is bracketed around the block and released on success, effect failure, and interruption. Connection hygiene is tracked, not reset: `exec`/`discard` clear all `WATCH`/`MULTI` state server-side, so a clean exit returns the connection to the idle pool; a scope abandoned with watches still armed (read, decided not to proceed, never `exec`/`discard`/`unwatch`) is discarded via the pool's existing `scheduleClose` path rather than recycled. No `UNWATCH` reset round-trip, and no `UNWATCH` command is needed in core. Quiescence is counted from *submit* time, not write time: the transport queues writes asynchronously, so recycling on "no pending replies" alone could hand back a connection whose `MULTI`…`EXEC` is still queued (e.g. an interrupted transaction) and let it execute under the next borrower — the connection counts work in-flight the moment a command is submitted.
- `exec` pipelines `MULTI` + the body + `EXEC` in one round-trip; the read-phase `run` calls are separate awaited round-trips. `DedicatedConnection` therefore needs a batched submit (a single aggregate callback over `MULTI` … `EXEC`) alongside its existing single `submit`.
- `tx.run` accepts blocking commands in the read phase: the connection is exclusively held, so a blocking read stalls only this scope's own connection. `exec` still rejects blocking commands through the Pipeline's existing guard.
- Acquisition is gated on client liveness and bounded by `acquireTimeout`, identical to the blocking-command path (ADR-0017); on `close`, an in-flight transaction is force-closed → `ConnectionLost(true)`.
- The handle can be captured and used after the block returns; this is unsupported and rejected at runtime — the finalizer sets a `released` flag the scope checks before every operation, failing with an `IllegalStateException` (a programmer error, outside the sealed hierarchy) rather than touching a recycled connection.
- An empty pipeline is a true no-op that never touches the socket *only when no keys are watched*; with watches armed, `exec` still issues `MULTI`/`EXEC` so a concurrent change to a watched key is observed as an abort (`None`) rather than reported as a vacuous success.
- Standalone only this slice. Like the Pipeline (ADR-0019), the body retains each command's `keyIndices` so a later cluster slice can enforce single-slot routing without reshaping the types.

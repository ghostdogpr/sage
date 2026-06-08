package sage.client.internal

import sage.codec.KeyCodec
import sage.commands.{Command, Pipeline}

/**
  * The handle opened by `transaction { scope => … }`: one leased Dedicated Connection, held for the whole block. Reads (`watch`, `run`)
  * execute immediately on that connection — the one `WATCH` requires — so a caller can read, decide, and only then `exec` a Pipeline as an
  * atomic `MULTI`/`EXEC`. `exec` returns `None` when a watched key changed before `EXEC` (an aborted optimistic-concurrency attempt the
  * caller retries, not a failure); `execAttempt` mirrors a Pipeline's per-position results. A queueing-phase rejection fails the effect with
  * `TransactionDiscarded` (nothing ran), distinct from an execution-phase error, which leaves the other commands committed.
  */
trait TransactionScope[F[_]] {

  /**
    * Watches keys for the duration of the scope; a later `exec` aborts if any changed.
    */
  def watch[K: KeyCodec](key: K, rest: K*): F[Unit]

  /**
    * Runs a command immediately on the leased connection — a read in the optimistic-concurrency loop, before any `exec`.
    */
  def run[A](command: Command[A]): F[A]

  /**
    * Executes the Pipeline atomically. `None` means a watched key changed and the transaction aborted.
    */
  def exec[Out, R](pipeline: Pipeline[Out, R]): F[Option[Out]]

  /**
    * Like `exec`, but yields the per-position results (each slot a `Right`/`Left`) on commit. `None` still means aborted.
    */
  def execAttempt[Out, R](pipeline: Pipeline[Out, R]): F[Option[R]]

  /**
    * Abandons the scope without committing, clearing any watched keys so the connection can be recycled (issues `UNWATCH`).
    */
  def discard: F[Unit]
}

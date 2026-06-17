package sage.client.internal

import sage.codec.KeyCodec
import sage.commands.{Attempt, Command, Pipeline}

/**
  * The handle opened by `transaction { tx => … }`: one leased Dedicated Connection, held for the whole block. Reads run immediately on that
  * connection via the inherited command surface (`tx.get`, `tx.run`, …) — the reads `WATCH` requires — so a caller can read, decide, and only
  * then `exec` a Pipeline atomically as a `MULTI`/`EXEC`. `exec` returns `None` when a watched key changed before `EXEC` (an aborted
  * optimistic-concurrency attempt the caller retries, not a failure); `execAttempt` mirrors a Pipeline's per-position results. A queueing-phase
  * rejection fails the effect with `TransactionDiscarded` (nothing ran), distinct from an execution-phase error, which leaves the others committed.
  */
trait TransactionScope[F[_], K] extends CommandRunner[F, K] {

  /**
    * Watches keys for the duration of the scope; a later `exec` aborts if any changed.
    */
  def watch[K: KeyCodec](key: K, rest: K*): F[Unit]

  private[sage] def exec[Out, R](pipeline: Pipeline[Out, R]): F[Option[Out]]

  private[sage] def execAttempt[Out, R](pipeline: Pipeline[Out, R]): F[Option[R]]

  /**
    * Executes a fixed-arity batch of commands atomically (`MULTI`/`EXEC`), yielding a result tuple that mirrors the argument tuple
    * element-for-element. `None` means a watched key changed and the transaction aborted.
    */
  def exec[T <: NonEmptyTuple](commands: T)(using Tuple.IsMappedBy[Command][T]): F[Option[Tuple.InverseMap[T, Command]]] =
    exec(Pipeline.fromTuple(commands))

  /**
    * Executes a dynamic, homogeneous batch of commands atomically (`MULTI`/`EXEC`), yielding one result per command in order. `None` means
    * a watched key changed and the transaction aborted.
    */
  def exec[A](commands: Seq[Command[A]]): F[Option[Vector[A]]] =
    exec(Pipeline.sequence(commands))

  /**
    * Like the tuple [[exec]], but yields the per-position results (each slot a `Right`/`Left`) on commit. `None` still means aborted.
    */
  def execAttempt[T <: NonEmptyTuple](commands: T)(using Tuple.IsMappedBy[Command][T]): F[Option[Tuple.Map[Tuple.InverseMap[T, Command], Attempt]]] =
    execAttempt(Pipeline.fromTuple(commands))

  /**
    * Like the `Seq` [[exec]], but yields the per-position results (each slot a `Right`/`Left`) on commit. `None` still means aborted.
    */
  def execAttempt[A](commands: Seq[Command[A]]): F[Option[Vector[Attempt[A]]]] =
    execAttempt(Pipeline.sequence(commands))

  /**
    * Abandons the scope without committing, clearing any watched keys so the connection can be recycled (issues `UNWATCH`).
    */
  def discard: F[Unit]

  /**
    * Re-types the scope's command surface to another key type, keeping the leased connection and the transaction's `watch`/`exec`/`discard`.
    * Lets a transaction read or write a different key type without leaving the scope (`tx.as[Array[Byte]].get(k)`).
    */
  override def as[K2](using KeyCodec[K2]): TransactionScope[F, K2] = {
    val self = this
    new TransactionScope[F, K2] {
      def run[A](command: Command[A]): F[A]                                           = self.run(command)
      def watch[K0: KeyCodec](key: K0, rest: K0*): F[Unit]                            = self.watch(key, rest*)
      private[sage] def exec[Out, R](pipeline: Pipeline[Out, R]): F[Option[Out]]      = self.exec(pipeline)
      private[sage] def execAttempt[Out, R](pipeline: Pipeline[Out, R]): F[Option[R]] = self.execAttempt(pipeline)
      def discard: F[Unit]                                                            = self.discard
    }
  }
}

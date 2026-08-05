package sage.client.internal

import sage.codec.KeyCodec
import sage.commands.{Attempt, Command, Pipeline}

/**
  * The handle passed to `transaction { tx => … }`. It uses at most one dedicated connection. Once acquired, the connection is retained until
  * the block ends. Commands such as `tx.get` run immediately, which allows a caller to read watched values before building a pipeline for
  * `exec`. `exec` sends the pipeline with
  * `MULTI`/`EXEC` and returns `None` if a watched key changed. `execAttempt` preserves errors for individual commands. If the server rejects
  * a command while queuing, the effect fails with `TransactionDiscarded` and nothing runs. Errors returned during execution affect only
  * their commands; the rest remain committed.
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
    * Returns a view that uses another key type while keeping the leased connection and the same `watch`, `exec`, and `discard` operations.
    * For example, `tx.as[Array[Byte]].get(k)` reads a binary key without leaving the transaction.
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

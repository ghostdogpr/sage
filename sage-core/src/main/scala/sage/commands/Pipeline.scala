package sage.commands

import scala.util.boundary

import sage.SageException

/**
  * The result for one pipeline or transaction position. `Right` contains a successful value, and `Left` contains that position's error. This is the element type of
  * the `*Attempt` result shapes (`Vector[Attempt[A]]` for a homogeneous batch, a tuple of `Attempt`s for a fixed-arity one).
  */
type Attempt[A] = Either[SageException, A]

/**
  * Assembles the commands passed to `pipeline` and `exec`. The runtime batches commands by target connection. A cluster pipeline normally
  * sends one batch per target node and routes any command whose target cannot be resolved individually. Each command produces one typed
  * result. A pipeline does not provide transaction atomicity. The public methods accept either a tuple of commands with different result types or a
  * `Seq[Command[A]]` with one result type. The runtime decodes positions independently into a `Vector[Either[SageException, Any]]`, then
  * `finish` converts it to `R`: the strict factories fail with the first error, and the `*Attempt` factories keep an [[Attempt]] for each
  * command. The internal `Any` values do not appear in the public result.
  */
final private[sage] class Pipeline[R] private (
  /**
    * The composed commands, in send order.
    */
  val commands: Vector[Command[?]],
  val finish: Vector[Attempt[Any]] => Attempt[R]
)

private[sage] object Pipeline {

  /**
    * A dynamic, homogeneous pipeline. An empty sequence is a no-op that yields an empty result without touching the socket.
    */
  def sequence[A](commands: Seq[Command[A]]): Pipeline[Vector[A]] = strict(commands.toVector, _.asInstanceOf[Vector[A]])

  def sequenceAttempt[A](commands: Seq[Command[A]]): Pipeline[Vector[Attempt[A]]] =
    new Pipeline(commands.toVector, results => Right(results.asInstanceOf[Vector[Attempt[A]]]))

  /**
    * A fixed-arity pipeline from a tuple of `Command`s, whose result tuple mirrors it element-for-element. `(get, incr)` yields
    * `Pipeline[(Option[V], Long)]`, and `fromTupleAttempt` yields `Pipeline[(Attempt[Option[V]], Attempt[Long])]`.
    */
  def fromTuple[T <: NonEmptyTuple](commands: T)(using Tuple.IsMappedBy[Command][T]): Pipeline[Tuple.InverseMap[T, Command]] =
    strict(listed(commands), tuple)

  def fromTupleAttempt[T <: NonEmptyTuple](
    commands: T
  )(using Tuple.IsMappedBy[Command][T]): Pipeline[Tuple.Map[Tuple.InverseMap[T, Command], Attempt]] =
    new Pipeline(listed(commands), results => Right(tuple(results)))

  private def listed(commands: NonEmptyTuple): Vector[Command[?]] = commands.toList.asInstanceOf[List[Command[?]]].toVector

  private def tuple[R](values: Vector[Any]): R = Tuple.fromArray(values.toArray).asInstanceOf[R]

  private def strict[R](commands: Vector[Command[?]], assemble: Vector[Any] => R): Pipeline[R] =
    new Pipeline(commands, results => boundary(Right(assemble(results.map(_.fold(error => boundary.break(Left(error)), identity))))))
}

package sage.commands

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
  * `Seq[Command[A]]` with one result type. `Out` contains the all-success result, while `Results` contains an [[Attempt]] for each command.
  * The runtime decodes positions independently into a `Vector[Either[SageException, Any]]`, then converts it to `Out` or `Results`. The
  * internal `Any` values do not appear in the public result.
  */
final private[sage] class Pipeline[Out, Results] private[commands] (
  /**
    * The composed commands, in send order.
    */
  val commands: Vector[Command[?]],
  private[sage] val toOut: Vector[Any] => Out,
  private[sage] val toResults: Vector[Either[SageException, Any]] => Results
)

private[sage] object Pipeline {

  /**
    * A dynamic, homogeneous pipeline. An empty sequence is a no-op that yields an empty result without touching the socket.
    */
  def sequence[A](commands: Seq[Command[A]]): Pipeline[Vector[A], Vector[Attempt[A]]] =
    new Pipeline(
      commands.toVector,
      values => values.asInstanceOf[Vector[A]],
      results => results.asInstanceOf[Vector[Attempt[A]]]
    )

  /**
    * A fixed-arity pipeline from a tuple of `Command`s, whose result tuple mirrors it element-for-element. `(get, incr)` yields
    * `Pipeline[(Option[V], Long), (Attempt[Option[V]], Attempt[Long])]`.
    */
  def fromTuple[T <: NonEmptyTuple](
    commands: T
  )(using Tuple.IsMappedBy[Command][T]): Pipeline[Tuple.InverseMap[T, Command], Tuple.Map[Tuple.InverseMap[T, Command], Attempt]] = {
    val cmds = commands.toList.asInstanceOf[List[Command[?]]].toVector
    new Pipeline(
      cmds,
      values => Tuple.fromArray(values.toArray).asInstanceOf[Tuple.InverseMap[T, Command]],
      results => Tuple.fromArray(results.toArray).asInstanceOf[Tuple.Map[Tuple.InverseMap[T, Command], Attempt]]
    )
  }
}

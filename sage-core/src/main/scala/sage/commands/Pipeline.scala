package sage.commands

import sage.SageException

/**
  * One pipeline (or transaction) position's outcome: `Right` on success, `Left` carrying the per-position error. This is the element type of
  * the `*Attempt` result shapes (`Vector[Attempt[A]]` for a homogeneous batch, a tuple of `Attempt`s for a fixed-arity one).
  */
type Attempt[A] = Either[SageException, A]

/**
  * The internal assembler behind the `pipeline`/`exec` command-batch sugar: an applicative composition of Commands sent in one round-trip,
  * yielding one typed result per command. Not a transaction, so no atomicity. Callers never name this type; they hand `pipeline`/`exec` a tuple
  * of Commands (fixed arity, heterogeneous results) or a `Seq[Command[A]]` (dynamic arity, homogeneous), and the runtime builds one of these.
  * `Out` is the all-success shape; `Results` is the per-position shape, each slot an [[Attempt]]. The runtime decodes every position
  * independently into a `Vector[Either[SageException, Any]]`, then either collapses it to `Out` (strict) or shapes it to `Results` (attempt);
  * the `Any` is confined to the assemblers below and never reaches a caller.
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

package sage.commands

import sage.SageException

/**
  * An applicative composition of Commands sent in one round-trip, yielding one typed result per command. Not a transaction — no
  * atomicity. `Out` is the all-success shape; `Results` is the per-position shape, each slot an `Either[SageException, _]`. The runtime
  * decodes every position independently into a `Vector[Either[SageException, Any]]`, then either collapses it to `Out` (strict) or shapes
  * it to `Results` (attempt); the `Any` is confined to the assemblers below and never reaches a caller.
  */
final class Pipeline[Out, Results] private[commands] (
  val commands: Vector[Command[?]],
  private[sage] val toOut: Vector[Any] => Out,
  private[sage] val toResults: Vector[Either[SageException, Any]] => Results
)

object Pipeline {

  type Attempt[A] = Either[SageException, A]

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
    * Tuple syntax: a tuple of `Command`s composes into a pipeline whose result tuple mirrors it element-for-element. `(get, incr).pipeline`
    * yields `Pipeline[(Option[V], Long), (Attempt[Option[V]], Attempt[Long])]`.
    */
  extension [T <: NonEmptyTuple](commands: T) {
    def pipeline(using Tuple.IsMappedBy[Command][T]): Pipeline[Tuple.InverseMap[T, Command], Tuple.Map[Tuple.InverseMap[T, Command], Attempt]] = {
      val cmds = commands.toList.asInstanceOf[List[Command[?]]].toVector
      new Pipeline(
        cmds,
        values => Tuple.fromArray(values.toArray).asInstanceOf[Tuple.InverseMap[T, Command]],
        results => Tuple.fromArray(results.toArray).asInstanceOf[Tuple.Map[Tuple.InverseMap[T, Command], Attempt]]
      )
    }
  }
}

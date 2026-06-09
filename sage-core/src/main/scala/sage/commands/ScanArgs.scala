package sage.commands

import sage.Bytes

private[commands] object ScanArgs {

  val Match: Bytes = Bytes.utf8("MATCH")
  val Count: Bytes = Bytes.utf8("COUNT")

  def options(pattern: Option[String], count: Option[Long]): Vector[Bytes] =
    pattern.toVector.flatMap(p => Vector(Match, Bytes.utf8(p))) ++
      count.toVector.flatMap(n => Vector(Count, Bytes.utf8(n.toString)))
}

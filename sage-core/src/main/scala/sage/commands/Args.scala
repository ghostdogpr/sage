package sage.commands

import sage.Bytes
import sage.codec.{Doubles, KeyCodec, Primitives, ValueCodec}

// argument keywords and option encoders shared by the command families
private[commands] object Args {

  val Match: Bytes      = Bytes.utf8("MATCH")
  val Count: Bytes      = Bytes.utf8("COUNT")
  val LimitWord: Bytes  = Bytes.utf8("LIMIT")
  val Nx: Bytes         = Bytes.utf8("NX")
  val Xx: Bytes         = Bytes.utf8("XX")
  val Gt: Bytes         = Bytes.utf8("GT")
  val Lt: Bytes         = Bytes.utf8("LT")
  val Ch: Bytes         = Bytes.utf8("CH")
  val Get: Bytes        = Bytes.utf8("GET")
  val Replace: Bytes    = Bytes.utf8("REPLACE")
  val Rev: Bytes        = Bytes.utf8("REV")
  val Min: Bytes        = Bytes.utf8("MIN")
  val Max: Bytes        = Bytes.utf8("MAX")
  val Desc: Bytes       = Bytes.utf8("DESC")
  val WithValues: Bytes = Bytes.utf8("WITHVALUES")

  def long(value: Long): Bytes = Primitives.encodeLong(value)

  def double(value: Double): Bytes = Bytes.utf8(Doubles.format(value))

  // the keyword of each case of a fieldless enum is its upper-cased name
  def keywords[E <: scala.reflect.Enum](values: Array[E]): E => Bytes = {
    val words = values.toVector.map(value => Bytes.utf8(value.toString.toUpperCase(java.util.Locale.ROOT)))
    value => words(value.ordinal)
  }

  def pairs[A, B](entries: Vector[(A, B)])(using a: KeyCodec[A], b: ValueCodec[B]): Vector[Bytes] =
    entries.flatMap { case (x, y) => Vector(a.encode(x), b.encode(y)) }

  def keyThen[K, A](key: K, first: A, rest: Seq[A])(encode: A => Bytes)(using keyCodec: KeyCodec[K]): Vector[Bytes] = {
    val args = Vector.newBuilder[Bytes]
    args.sizeHint(rest.length + 2)
    args += keyCodec.encode(key) += encode(first)
    rest.foreach(a => args += encode(a))
    args.result()
  }

  def flag(on: Boolean, word: Bytes): Vector[Bytes] = if (on) Vector(word) else Vector.empty

  def opt[A](word: Bytes, value: Option[A])(encode: A => Bytes): Vector[Bytes] =
    value match {
      case Some(a) => Vector(word, encode(a))
      case None    => Vector.empty
    }

  def optLong(word: Bytes, value: Option[Long]): Vector[Bytes] = opt(word, value)(long)

  def optText(word: Bytes, value: Option[String]): Vector[Bytes] = opt(word, value)(Bytes.utf8)

  def limit(value: Option[Limit]): Vector[Bytes] =
    value match {
      case Some(l) => Vector(LimitWord, Args.long(l.offset), Args.long(l.count))
      case None    => Vector.empty
    }

  def scanOptions(pattern: Option[String], count: Option[Long]): Vector[Bytes] = optText(Match, pattern) ++ optLong(Count, count)
}

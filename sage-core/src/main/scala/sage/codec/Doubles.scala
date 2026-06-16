package sage.codec

/**
  * The canonical RESP wire form for doubles: infinities and NaN are spelled `inf`/`-inf`/`nan` the way Redis writes them. The
  * single source of truth shared by the value codec, the score/range/coordinate encoders, and the bulk-string reply decoders.
  * The RESP3 parser keeps its own wire-level grammar by design — it is not a codec.
  */
private[sage] object Doubles {

  def format(value: Double): String =
    special(value == Double.PositiveInfinity, value == Double.NegativeInfinity, value.isNaN).getOrElse(value.toString)

  def formatFloat(value: Float): String =
    special(value == Float.PositiveInfinity, value == Float.NegativeInfinity, value.isNaN).getOrElse(value.toString)

  def parse(text: String): Option[Double] =
    parseWith(text)(Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN, _.toDoubleOption)

  def parseFloat(text: String): Option[Float] =
    parseWith(text)(Float.PositiveInfinity, Float.NegativeInfinity, Float.NaN, _.toFloatOption)

  private def special(posInf: Boolean, negInf: Boolean, nan: Boolean): Option[String] =
    if (posInf) Some("inf") else if (negInf) Some("-inf") else if (nan) Some("nan") else None

  private def parseWith[A](text: String)(posInf: A, negInf: A, nan: A, fallback: String => Option[A]): Option[A] =
    text match {
      case "inf" | "+inf" => Some(posInf)
      case "-inf"         => Some(negInf)
      case "nan"          => Some(nan)
      case other          => fallback(other)
    }
}

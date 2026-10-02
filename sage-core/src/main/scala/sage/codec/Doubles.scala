package sage.codec

/**
  * The canonical RESP wire form for doubles: infinities and NaN are spelled `inf`/`-inf`/`nan` the way Redis writes them. The value codec,
  * score, range, and coordinate encoders, and bulk-string reply decoders all use this format. The RESP3 parser keeps its own wire-level
  * grammar by design because it is not a codec.
  */
private[sage] object Doubles {

  def format(value: Double): String =
    if (value == Double.PositiveInfinity) "inf" else if (value == Double.NegativeInfinity) "-inf" else if (value.isNaN) "nan" else value.toString

  def formatFloat(value: Float): String = if (value.isNaN || value.isInfinite) format(value.toDouble) else value.toString

  def parse(text: String): Option[Double] =
    parseWith(text)(Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN, _.toDoubleOption)

  def parseFloat(text: String): Option[Float] =
    parseWith(text)(Float.PositiveInfinity, Float.NegativeInfinity, Float.NaN, _.toFloatOption)

  private def parseWith[A](text: String)(posInf: A, negInf: A, nan: A, fallback: String => Option[A]): Option[A] =
    text match {
      case "inf" | "+inf" => Some(posInf)
      case "-inf"         => Some(negInf)
      case "nan"          => Some(nan)
      case other          => fallback(other)
    }
}

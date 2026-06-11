package sage.codec

/**
  * The canonical RESP wire form for doubles: infinities and NaN are spelled `inf`/`-inf`/`nan` the way Redis writes and accepts
  * them, never Java's `Infinity`/`NaN`. The single source of truth shared by the value codec, the score/range/coordinate
  * encoders, and the bulk-string reply decoders. The RESP3 parser keeps its own wire-level grammar by design — it is not a codec.
  */
private[sage] object Doubles {

  def format(value: Double): String =
    if (value == Double.PositiveInfinity) "inf"
    else if (value == Double.NegativeInfinity) "-inf"
    else if (value.isNaN) "nan"
    else value.toString

  def parse(text: String): Option[Double] =
    text match {
      case "inf" | "+inf" => Some(Double.PositiveInfinity)
      case "-inf"         => Some(Double.NegativeInfinity)
      case "nan"          => Some(Double.NaN)
      case other          => other.toDoubleOption
    }

  def formatFloat(value: Float): String =
    if (value == Float.PositiveInfinity) "inf"
    else if (value == Float.NegativeInfinity) "-inf"
    else if (value.isNaN) "nan"
    else value.toString

  def parseFloat(text: String): Option[Float] =
    text match {
      case "inf" | "+inf" => Some(Float.PositiveInfinity)
      case "-inf"         => Some(Float.NegativeInfinity)
      case "nan"          => Some(Float.NaN)
      case other          => other.toFloatOption
    }
}

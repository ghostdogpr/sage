package sage.protocol

import sage.Bytes

/**
  * A single RESP3 protocol value. Aggregates preserve wire order and duplicate keys; byte-carrying cases define content equality, so `==`
  * is structural on every Frame. A sealed trait rather than an enum because those cases must override `equals`/`hashCode`.
  */
sealed trait Frame

object Frame {

  final case class SimpleString(value: String) extends Frame

  final case class SimpleError(value: String) extends Frame

  final case class Integer(value: Long) extends Frame

  final case class BulkString(value: Bytes) extends Frame {

    override def equals(that: Any): Boolean =
      that match {
        case BulkString(other) => value.sameBytes(other)
        case _                 => false
      }

    override def hashCode(): Int = value.contentHashCode

    override def toString: String = s"BulkString(${value.asUtf8String})"
  }

  final case class Array(elements: Vector[Frame]) extends Frame

  case object Null extends Frame

  final case class Bool(value: Boolean) extends Frame

  final case class Double(value: scala.Double) extends Frame

  final case class BigNumber(value: BigInt) extends Frame

  final case class BulkError(value: Bytes) extends Frame {

    override def equals(that: Any): Boolean =
      that match {
        case BulkError(other) => value.sameBytes(other)
        case _                => false
      }

    override def hashCode(): Int = value.contentHashCode

    override def toString: String = s"BulkError(${value.asUtf8String})"
  }

  final case class VerbatimString(format: String, value: Bytes) extends Frame {

    override def equals(that: Any): Boolean =
      that match {
        case VerbatimString(otherFormat, otherValue) => format == otherFormat && value.sameBytes(otherValue)
        case _                                       => false
      }

    override def hashCode(): Int = format.hashCode * 31 + value.contentHashCode

    override def toString: String = s"VerbatimString($format, ${value.asUtf8String})"
  }

  final case class Map(entries: Vector[(Frame, Frame)]) extends Frame

  final case class Set(elements: Vector[Frame]) extends Frame

  final case class Attribute(entries: Vector[(Frame, Frame)]) extends Frame

  final case class Push(elements: Vector[Frame]) extends Frame

  def describe(frame: Frame): String =
    frame match {
      case SimpleString(value)           => s"simple string '$value'"
      case SimpleError(value)            => s"simple error '$value'"
      case Integer(value)                => s"integer $value"
      case BulkString(value)             => s"bulk string (${value.length} bytes)"
      case Array(elements)               => s"array (${elements.length} elements)"
      case Null                          => "null"
      case Bool(value)                   => s"boolean $value"
      case Double(value)                 => s"double $value"
      case BigNumber(value)              => s"big number $value"
      case BulkError(value)              => s"bulk error (${value.length} bytes)"
      case VerbatimString(format, value) => s"verbatim string '$format' (${value.length} bytes)"
      case Map(entries)                  => s"map (${entries.length} entries)"
      case Set(elements)                 => s"set (${elements.length} elements)"
      case Attribute(entries)            => s"attribute (${entries.length} entries)"
      case Push(elements)                => s"push (${elements.length} elements)"
    }
}

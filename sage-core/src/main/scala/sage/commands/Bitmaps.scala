package sage.commands

import sage.Bytes
import sage.codec.{KeyCodec, Primitives}
import sage.commands.Args.Get

/**
  * Whether a `BITCOUNT`/`BITPOS` range is measured in `Byte`s or `Bit`s.
  */
enum BitUnit {
  case Byte, Bit
}

/**
  * An inclusive `[start, end]` range for `BITCOUNT`, in [[BitUnit]]s; negative indices count from the end.
  */
final case class BitRange(start: Long, end: Long, unit: BitUnit = BitUnit.Byte)

/**
  * A `BITPOS` range, whose grammar is looser than [[BitRange]]: search `FromStart` (start only), or `Within` an explicit
  * `[start, end]`; a unit is permitted only with an end, which this type enforces.
  */
enum BitPosRange {
  case FromStart(start: Long)
  case Within(start: Long, end: Long, unit: BitUnit = BitUnit.Byte)
}

/**
  * A `BITFIELD` integer type. `Signed` represents `i<bits>` from 1 to 64 bits. `Unsigned` represents `u<bits>` from 1 to 63 bits.
  */
enum BitFieldType {
  case Signed(bits: Int)
  case Unsigned(bits: Int)
}

/**
  * A `BITFIELD` offset. `Absolute` stores a bit offset. `TypeWidth(n)` represents the wire form `#n`, or `n` multiplied by the type width.
  */
enum BitFieldOffset {
  case Absolute(value: Long)
  case TypeWidth(factor: Long)
}

/**
  * How `BITFIELD` arithmetic handles overflow: `Wrap` around, `Sat`urate at the type's bound, or `Fail` (no write).
  */
enum BitFieldOverflow {
  case Wrap, Sat, Fail
}

/**
  * One operation in a `BITFIELD` command. `Overflow` produces no reply element and sets the mode for the following `Set` and `IncrBy`
  * operations. The read-only `BITFIELD_RO` command accepts only `Get`.
  */
enum BitFieldOp {
  case Get(fieldType: BitFieldType, offset: BitFieldOffset)
  case Set(fieldType: BitFieldType, offset: BitFieldOffset, value: Long)
  case IncrBy(fieldType: BitFieldType, offset: BitFieldOffset, increment: Long)
  case Overflow(behavior: BitFieldOverflow)
}

private[sage] object Bitmaps {

  private val And          = Bytes.utf8("AND")
  private val Or           = Bytes.utf8("OR")
  private val Xor          = Bytes.utf8("XOR")
  private val Not          = Bytes.utf8("NOT")
  private val SetWord      = Bytes.utf8("SET")
  private val IncrByWord   = Bytes.utf8("INCRBY")
  private val OverflowWord = Bytes.utf8("OVERFLOW")
  private val overflowArg  = Args.keywords(BitFieldOverflow.values)
  private val unitArg      = Args.keywords(BitUnit.values)

  def setBit[K](key: K, offset: Long, value: Boolean)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command("SETBIT", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(offset), Primitives.encodeBoolean(value)), Decode.flag)

  def getBit[K](key: K, offset: Long)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command.read("GETBIT", Command.FirstKey, Vector(keyCodec.encode(key), Args.long(offset)), Decode.flag)

  def bitCount[K](key: K, range: Option[BitRange] = None)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("BITCOUNT", Command.FirstKey, keyCodec.encode(key) +: rangeArgs(range), Decode.long)

  def bitPos[K](key: K, bit: Boolean, range: Option[BitPosRange] = None)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("BITPOS", Command.FirstKey, Vector(keyCodec.encode(key), Primitives.encodeBoolean(bit)) ++ posRangeArgs(range), Decode.long)

  def bitOpAnd[K](destination: K, first: K, rest: K*)(using KeyCodec[K]): Command[Long] = bitOp(And, destination, first +: rest.toVector)

  def bitOpOr[K](destination: K, first: K, rest: K*)(using KeyCodec[K]): Command[Long] = bitOp(Or, destination, first +: rest.toVector)

  def bitOpXor[K](destination: K, first: K, rest: K*)(using KeyCodec[K]): Command[Long] = bitOp(Xor, destination, first +: rest.toVector)

  def bitOpNot[K](destination: K, source: K)(using KeyCodec[K]): Command[Long] = bitOp(Not, destination, Vector(source))

  def bitField[K](key: K, first: BitFieldOp, rest: BitFieldOp*)(using keyCodec: KeyCodec[K]): Command[Vector[Option[Long]]] =
    Command("BITFIELD", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).flatMap(opArgs), Decode.vector(Decode.optionalLong))

  def bitFieldRo[K](key: K, first: BitFieldOp.Get, rest: BitFieldOp.Get*)(using keyCodec: KeyCodec[K]): Command[Vector[Long]] =
    Command.read("BITFIELD_RO", Command.FirstKey, keyCodec.encode(key) +: (first +: rest.toVector).flatMap(opArgs), Decode.vector(Decode.long))

  private def bitOp[K](op: Bytes, destination: K, sources: Vector[K])(using keyCodec: KeyCodec[K]): Command[Long] = {
    val keys = (destination +: sources).map(keyCodec.encode)
    Command("BITOP", Vector.tabulate(keys.size)(_ + 1), op +: keys, Decode.long)
  }

  private def rangeArgs(range: Option[BitRange]): Vector[Bytes] =
    range.toVector.flatMap(r => Vector(Args.long(r.start), Args.long(r.end), unitArg(r.unit)))

  private def posRangeArgs(range: Option[BitPosRange]): Vector[Bytes] =
    range match {
      case None                                       => Vector.empty
      case Some(BitPosRange.FromStart(start))         => Vector(Args.long(start))
      case Some(BitPosRange.Within(start, end, unit)) => Vector(Args.long(start), Args.long(end), unitArg(unit))
    }

  private def opArgs(op: BitFieldOp): Vector[Bytes] =
    op match {
      case BitFieldOp.Get(fieldType, offset)          => Vector(Get, typeArg(fieldType), offsetArg(offset))
      case BitFieldOp.Set(fieldType, offset, value)   => Vector(SetWord, typeArg(fieldType), offsetArg(offset), Args.long(value))
      case BitFieldOp.IncrBy(fieldType, offset, incr) => Vector(IncrByWord, typeArg(fieldType), offsetArg(offset), Args.long(incr))
      case BitFieldOp.Overflow(behavior)              => Vector(OverflowWord, overflowArg(behavior))
    }

  private def typeArg(fieldType: BitFieldType): Bytes =
    fieldType match {
      case BitFieldType.Signed(bits)   => Bytes.utf8("i" + bits.toString)
      case BitFieldType.Unsigned(bits) => Bytes.utf8("u" + bits.toString)
    }

  private def offsetArg(offset: BitFieldOffset): Bytes =
    offset match {
      case BitFieldOffset.Absolute(value)   => Args.long(value)
      case BitFieldOffset.TypeWidth(factor) => Bytes.utf8("#" + factor.toString)
    }

}

package sage.commands

import sage.Bytes
import sage.codec.KeyCodec

enum BitUnit {
  case Byte, Bit
}

final case class BitRange(start: Long, end: Long, unit: BitUnit = BitUnit.Byte)

// BITPOS's grammar is looser than BITCOUNT's: end requires start, and a unit requires end
enum BitPosRange {
  case FromStart(start: Long)
  case Within(start: Long, end: Long, unit: BitUnit = BitUnit.Byte)
}

// signed i<bits> (1..64) or unsigned u<bits> (1..63)
enum BitFieldType {
  case Signed(bits: Int)
  case Unsigned(bits: Int)
}

// TypeWidth(n) is the wire's #n form: n * type-width bits in
enum BitFieldOffset {
  case Absolute(value: Long)
  case TypeWidth(factor: Long)
}

enum BitFieldOverflow {
  case Wrap, Sat, Fail
}

// Overflow yields no reply element; it sets the mode for later Set/IncrBy. Get is the only op BITFIELD_RO accepts (see bitFieldRo).
enum BitFieldOp {
  case Get(fieldType: BitFieldType, offset: BitFieldOffset)
  case Set(fieldType: BitFieldType, offset: BitFieldOffset, value: Long)
  case IncrBy(fieldType: BitFieldType, offset: BitFieldOffset, increment: Long)
  case Overflow(behavior: BitFieldOverflow)
}

private[sage] object Bitmaps {

  private val Zero         = Bytes.utf8("0")
  private val One          = Bytes.utf8("1")
  private val And          = Bytes.utf8("AND")
  private val Or           = Bytes.utf8("OR")
  private val Xor          = Bytes.utf8("XOR")
  private val Not          = Bytes.utf8("NOT")
  private val GetWord      = Bytes.utf8("GET")
  private val SetWord      = Bytes.utf8("SET")
  private val IncrByWord   = Bytes.utf8("INCRBY")
  private val OverflowWord = Bytes.utf8("OVERFLOW")
  private val WrapWord     = Bytes.utf8("WRAP")
  private val SatWord      = Bytes.utf8("SAT")
  private val FailWord     = Bytes.utf8("FAIL")
  private val ByteWord     = Bytes.utf8("BYTE")
  private val BitWord      = Bytes.utf8("BIT")

  def setBit[K](key: K, offset: Long, value: Boolean)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command("SETBIT", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(offset.toString), bitToken(value)), Decode.flag)

  def getBit[K](key: K, offset: Long)(using keyCodec: KeyCodec[K]): Command[Boolean] =
    Command.read("GETBIT", Command.FirstKey, Vector(keyCodec.encode(key), Bytes.utf8(offset.toString)), Decode.flag)

  def bitCount[K](key: K, range: Option[BitRange] = None)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("BITCOUNT", Command.FirstKey, keyCodec.encode(key) +: rangeArgs(range), Decode.long)

  def bitPos[K](key: K, bit: Boolean, range: Option[BitPosRange] = None)(using keyCodec: KeyCodec[K]): Command[Long] =
    Command.read("BITPOS", Command.FirstKey, Vector(keyCodec.encode(key), bitToken(bit)) ++ posRangeArgs(range), Decode.long)

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
    range.toVector.flatMap(r => Vector(Bytes.utf8(r.start.toString), Bytes.utf8(r.end.toString), unitArg(r.unit)))

  private def posRangeArgs(range: Option[BitPosRange]): Vector[Bytes] =
    range match {
      case None                                       => Vector.empty
      case Some(BitPosRange.FromStart(start))         => Vector(Bytes.utf8(start.toString))
      case Some(BitPosRange.Within(start, end, unit)) => Vector(Bytes.utf8(start.toString), Bytes.utf8(end.toString), unitArg(unit))
    }

  private def opArgs(op: BitFieldOp): Vector[Bytes] =
    op match {
      case BitFieldOp.Get(fieldType, offset)          => Vector(GetWord, typeArg(fieldType), offsetArg(offset))
      case BitFieldOp.Set(fieldType, offset, value)   => Vector(SetWord, typeArg(fieldType), offsetArg(offset), Bytes.utf8(value.toString))
      case BitFieldOp.IncrBy(fieldType, offset, incr) => Vector(IncrByWord, typeArg(fieldType), offsetArg(offset), Bytes.utf8(incr.toString))
      case BitFieldOp.Overflow(behavior)              => Vector(OverflowWord, overflowArg(behavior))
    }

  private def typeArg(fieldType: BitFieldType): Bytes =
    fieldType match {
      case BitFieldType.Signed(bits)   => Bytes.utf8("i" + bits.toString)
      case BitFieldType.Unsigned(bits) => Bytes.utf8("u" + bits.toString)
    }

  private def offsetArg(offset: BitFieldOffset): Bytes =
    offset match {
      case BitFieldOffset.Absolute(value)   => Bytes.utf8(value.toString)
      case BitFieldOffset.TypeWidth(factor) => Bytes.utf8("#" + factor.toString)
    }

  private def overflowArg(behavior: BitFieldOverflow): Bytes =
    behavior match {
      case BitFieldOverflow.Wrap => WrapWord
      case BitFieldOverflow.Sat  => SatWord
      case BitFieldOverflow.Fail => FailWord
    }

  private def unitArg(unit: BitUnit): Bytes =
    unit match {
      case BitUnit.Byte => ByteWord
      case BitUnit.Bit  => BitWord
    }

  private def bitToken(value: Boolean): Bytes = if (value) One else Zero
}

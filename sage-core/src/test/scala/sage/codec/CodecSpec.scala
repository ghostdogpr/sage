package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

class CodecSpec extends munit.FunSuite {

  private def assertValueRoundTrip[A](value: A)(using codec: ValueCodec[A]): Unit =
    assertEquals(codec.decode(codec.encode(value)), Right(value))

  private def assertKeyRoundTrip[A](value: A)(using codec: KeyCodec[A]): Unit =
    assertEquals(codec.decode(codec.encode(value)), Right(value))

  private val strings = List("", "héllo", "wörld", "line\nbreak")
  private val ints    = List(0, 42, -7, Int.MaxValue, Int.MinValue)
  private val longs   = List(0L, 42L, -7L, Long.MaxValue, Long.MinValue)
  private val doubles = List(0.0, 1.5, -2.25, Double.MaxValue, Double.MinValue, Double.PositiveInfinity, Double.NegativeInfinity)
  private val floats  = List(0.0f, 1.5f, -2.25f, Float.MaxValue, Float.MinValue, Float.PositiveInfinity, Float.NegativeInfinity)

  test("String codecs round-trip") {
    strings.foreach { value =>
      assertValueRoundTrip(value)
      assertKeyRoundTrip(value)
    }
  }

  test("Int codecs round-trip") {
    ints.foreach { value =>
      assertValueRoundTrip(value)
      assertKeyRoundTrip(value)
    }
  }

  test("Long codecs round-trip") {
    longs.foreach { value =>
      assertValueRoundTrip(value)
      assertKeyRoundTrip(value)
    }
  }

  test("Double ValueCodec round-trips") {
    doubles.foreach(assertValueRoundTrip(_))
  }

  test("Float ValueCodec round-trips") {
    floats.foreach(assertValueRoundTrip(_))
  }

  test("encode writes canonical wire bytes") {
    assert(summon[ValueCodec[String]].encode("foo").sameBytes(Bytes.utf8("foo")))
    assert(summon[KeyCodec[String]].encode("foo").sameBytes(Bytes.utf8("foo")))
    assert(summon[ValueCodec[Int]].encode(42).sameBytes(Bytes.utf8("42")))
    assert(summon[KeyCodec[Long]].encode(-7L).sameBytes(Bytes.utf8("-7")))
    assert(summon[ValueCodec[Double]].encode(1.5).sameBytes(Bytes.utf8("1.5")))
    assert(summon[ValueCodec[Float]].encode(1.5f).sameBytes(Bytes.utf8("1.5")))
  }

  test("Boolean ValueCodec writes 1 and 0") {
    val codec = summon[ValueCodec[Boolean]]
    assert(codec.encode(true).sameBytes(Bytes.utf8("1")))
    assert(codec.encode(false).sameBytes(Bytes.utf8("0")))
    assertValueRoundTrip(true)
    assertValueRoundTrip(false)
  }

  test("Boolean decode rejects anything but 1 and 0") {
    val codec = summon[ValueCodec[Boolean]]
    assertEquals(codec.decode(Bytes.utf8("true")), Left(DecodeError("boolean (1 or 0)", "'true'")))
    assertEquals(codec.decode(Bytes.utf8("")), Left(DecodeError("boolean (1 or 0)", "''")))
  }

  test("numeric decode failure carries expected and actual") {
    assertEquals(summon[ValueCodec[Int]].decode(Bytes.utf8("abc")), Left(DecodeError("Int", "'abc'")))
    assertEquals(summon[ValueCodec[Int]].decode(Bytes.utf8("1.0")), Left(DecodeError("Int", "'1.0'")))
    assertEquals(summon[ValueCodec[Long]].decode(Bytes.utf8("9223372036854775808")), Left(DecodeError("Long", "'9223372036854775808'")))
  }

  test("String decode rejects invalid UTF-8") {
    val invalid = Bytes.fromArray(Array(0xff.toByte, 0xfe.toByte))
    summon[ValueCodec[String]].decode(invalid) match {
      case Left(DecodeError(expected, _)) => assertEquals(expected, "UTF-8 string")
      case other                          => fail(s"expected a DecodeError, got $other")
    }
  }

  test("control characters are escaped in DecodeError") {
    assertEquals(summon[ValueCodec[Int]].decode(Bytes.utf8("1\n2")), Left(DecodeError("Int", "'1\\n2'")))
    assertEquals(summon[ValueCodec[Int]].decode(Bytes.utf8("a\u0000b")), Left(DecodeError("Int", "'a\\u0000b'")))
  }

  test("long payloads are truncated in DecodeError with a byte count") {
    val payload = "x" * 100
    assertEquals(summon[ValueCodec[Int]].decode(Bytes.utf8(payload)), Left(DecodeError("Int", s"'${"x" * 64}…' (100 bytes)")))
  }

  test("truncation never splits a surrogate pair or an escape sequence") {
    val emojiPayload   = "x" * 63 + "😀" + "y" * 30
    assertEquals(
      summon[ValueCodec[Int]].decode(Bytes.utf8(emojiPayload)),
      Left(DecodeError("Int", s"'${"x" * 63}😀…' (97 bytes)"))
    )
    val controlPayload = "x" * 63 + "\u0000" + "y" * 30
    assertEquals(
      summon[ValueCodec[Int]].decode(Bytes.utf8(controlPayload)),
      Left(DecodeError("Int", s"'${"x" * 63}\\u0000…' (94 bytes)"))
    )
  }

  test("Bytes codecs are the identity") {
    val data = Bytes.utf8("raw")
    assert(summon[ValueCodec[Bytes]].encode(data).sameBytes(data))
    assertEquals(summon[ValueCodec[Bytes]].decode(data).map(_.sameBytes(data)), Right(true))
    assert(summon[KeyCodec[Bytes]].encode(data).sameBytes(data))
    assertEquals(summon[KeyCodec[Bytes]].decode(data).map(_.sameBytes(data)), Right(true))
  }

  test("Array[Byte] codec copies on encode and decode") {
    val codec   = summon[ValueCodec[Array[Byte]]]
    val input   = Array[Byte](1, 2, 3)
    val encoded = codec.encode(input)
    input(0) = 9
    assert(encoded.sameBytes(Bytes.fromArray(Array[Byte](1, 2, 3))))
    val decoded = codec.decode(encoded).fold(error => fail(error.getMessage), identity)
    decoded(0) = 9
    assert(encoded.sameBytes(Bytes.fromArray(Array[Byte](1, 2, 3))))
  }

  test("Array[Byte] codecs round-trip") {
    val input = Array[Byte](1, 2, 3, -128, 127)
    assertEquals(summon[ValueCodec[Array[Byte]]].decode(summon[ValueCodec[Array[Byte]]].encode(input)).map(_.toList), Right(input.toList))
    assertEquals(summon[KeyCodec[Array[Byte]]].decode(summon[KeyCodec[Array[Byte]]].encode(input)).map(_.toList), Right(input.toList))
  }

  test("no KeyCodec for Double, Float, or Boolean") {
    assert(compileErrors("summon[sage.codec.KeyCodec[Double]]").nonEmpty)
    assert(compileErrors("summon[sage.codec.KeyCodec[Float]]").nonEmpty)
    assert(compileErrors("summon[sage.codec.KeyCodec[Boolean]]").nonEmpty)
  }
}

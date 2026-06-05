package sage.codec

import sage.Bytes

class CodecSpec extends munit.FunSuite {

  test("String KeyCodec round-trips") {
    val codec = summon[KeyCodec[String]]
    assertEquals(codec.decode(codec.encode("héllo")), Right("héllo"))
    assert(codec.encode("foo").sameBytes(Bytes.utf8("foo")))
  }

  test("String ValueCodec round-trips") {
    val codec = summon[ValueCodec[String]]
    assertEquals(codec.decode(codec.encode("wörld")), Right("wörld"))
    assert(codec.encode("bar").sameBytes(Bytes.utf8("bar")))
  }
}

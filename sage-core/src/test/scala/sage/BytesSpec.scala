package sage

class BytesSpec extends munit.FunSuite {

  test("sameBytes is content equality") {
    val a = Bytes.utf8("hello")
    val b = Bytes.utf8("hello")
    val c = Bytes.utf8("world")
    assert(a.sameBytes(b))
    assert(!a.sameBytes(c))
  }

  test("contentHashCode is consistent with sameBytes") {
    assertEquals(Bytes.utf8("hello").contentHashCode, Bytes.utf8("hello").contentHashCode)
  }

  test("utf8 round-trips through asUtf8String") {
    assertEquals(Bytes.utf8("héllo wörld").asUtf8String, "héllo wörld")
  }

  test("fromArray copies defensively") {
    val source = "abc".getBytes
    val bytes  = Bytes.fromArray(source)
    source(0) = 'z'.toByte
    assertEquals(bytes.asUtf8String, "abc")
  }

  test("toArray returns a copy") {
    val bytes = Bytes.utf8("abc")
    val out   = bytes.toArray
    out(0) = 'z'.toByte
    assertEquals(bytes.asUtf8String, "abc")
  }

  test("wrap and toIArray preserve content") {
    val bytes = Bytes.wrap(IArray[Byte](1, 2, 3))
    assertEquals(bytes.length, 3)
    assertEquals(bytes.toIArray.toList, List[Byte](1, 2, 3))
  }

  test("empty has length zero") {
    assertEquals(Bytes.empty.length, 0)
  }
}

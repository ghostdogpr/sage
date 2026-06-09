package sage.cluster

import sage.Bytes

class SlotSpec extends munit.FunSuite {

  test("CRC16 matches the published XMODEM check value") {
    val bytes = "123456789".getBytes
    assertEquals(Crc16.of(bytes, 0, bytes.length), 0x31c3)
  }

  test("known keys hash to their documented slots") {
    assertEquals(Slot.of(Bytes.utf8("foo")).value, 12182)
    assertEquals(Slot.of(Bytes.utf8("bar")).value, 5061)
  }

  test("a hash tag hashes only the bytes between the first braces") {
    assertEquals(Slot.of(Bytes.utf8("{foo}")), Slot.of(Bytes.utf8("foo")))
    assertEquals(Slot.of(Bytes.utf8("{foo}.bar")), Slot.of(Bytes.utf8("foo")))
    assertEquals(Slot.of(Bytes.utf8("{user1000}.following")), Slot.of(Bytes.utf8("{user1000}.followers")))
  }

  test("an empty or unclosed tag hashes the whole key") {
    assertEquals(Slot.of(Bytes.utf8("{}foo")), Slot.of(Bytes.utf8("{}foo")))
    assertNotEquals(Slot.of(Bytes.utf8("{}foo")), Slot.of(Bytes.utf8("foo")))
    assertNotEquals(Slot.of(Bytes.utf8("foo{")), Slot.of(Bytes.utf8("foo")))
    // first '{' has no following '}' before the next content; "{" then "}" empty -> whole key
    assertNotEquals(Slot.of(Bytes.utf8("foo{}{bar}")), Slot.of(Bytes.utf8("bar")))
  }

  test("only the first non-empty tag is used") {
    assertEquals(Slot.of(Bytes.utf8("{first}{second}")), Slot.of(Bytes.utf8("first")))
  }

  test("of stays within range and at is None out-of-range") {
    assert(Slot.of(Bytes.utf8("anything")).value >= 0)
    assert(Slot.of(Bytes.utf8("anything")).value < Slot.Count)
    assertEquals(Slot.at(0).map(_.value), Some(0))
    assertEquals(Slot.at(Slot.Count), None)
    assertEquals(Slot.at(-1), None)
  }
}

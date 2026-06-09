package sage.cluster

class RedirectSpec extends munit.FunSuite {

  test("MOVED parses into a permanent redirect") {
    assertEquals(
      Redirect.parse("MOVED 3999 127.0.0.1:6381"),
      Some(Redirect(RedirectKind.Moved, Slot.unsafe(3999), Node("127.0.0.1", 6381)))
    )
  }

  test("ASK parses into a one-shot redirect") {
    assertEquals(
      Redirect.parse("ASK 3999 127.0.0.1:6381"),
      Some(Redirect(RedirectKind.Ask, Slot.unsafe(3999), Node("127.0.0.1", 6381)))
    )
  }

  test("an empty host is preserved for the runtime to resolve") {
    assertEquals(Redirect.parse("MOVED 3999 :6381"), Some(Redirect(RedirectKind.Moved, Slot.unsafe(3999), Node("", 6381))))
  }

  test("an IPv6 host keeps its colons, port taken after the last") {
    assertEquals(
      Redirect.parse("MOVED 1 2001:db8::1:6379"),
      Some(Redirect(RedirectKind.Moved, Slot.unsafe(1), Node("2001:db8::1", 6379)))
    )
  }

  test("a non-redirect error is not a redirect") {
    assertEquals(Redirect.parse("WRONGTYPE Operation against a key holding the wrong kind of value"), None)
    assertEquals(Redirect.parse("ERR unknown command"), None)
  }

  test("malformed redirects parse to None") {
    assertEquals(Redirect.parse("MOVED 3999"), None)
    assertEquals(Redirect.parse("MOVED notaslot 127.0.0.1:6381"), None)
    assertEquals(Redirect.parse("MOVED 3999 127.0.0.1:notaport"), None)
    assertEquals(Redirect.parse("MOVED 99999 127.0.0.1:6381"), None)
  }

  test("an out-of-range port is rejected") {
    assertEquals(Redirect.parse("MOVED 1 127.0.0.1:0"), None)
    assertEquals(Redirect.parse("MOVED 1 127.0.0.1:-1"), None)
    assertEquals(Redirect.parse("MOVED 1 127.0.0.1:70000"), None)
  }
}

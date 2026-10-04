package sage.cluster

class RedirectSpec extends munit.FunSuite {

  test("MOVED parses into a permanent redirect") {
    assertEquals(
      Redirect.parse(RedirectKind.Moved, "3999 127.0.0.1:6381"),
      Some(Redirect(RedirectKind.Moved, Node("127.0.0.1", 6381)))
    )
  }

  test("ASK parses into a one-shot redirect") {
    assertEquals(
      Redirect.parse(RedirectKind.Ask, "3999 127.0.0.1:6381"),
      Some(Redirect(RedirectKind.Ask, Node("127.0.0.1", 6381)))
    )
  }

  test("an empty host targets the node that sent the redirect") {
    val from = Node("10.0.0.5", 7000)
    assertEquals(Redirect.parse(RedirectKind.Moved, "3999 :6381").map(_.target(from)), Some(Node("10.0.0.5", 6381)))
    assertEquals(Redirect.parse(RedirectKind.Moved, "3999 127.0.0.1:6381").map(_.target(from)), Some(Node("127.0.0.1", 6381)))
  }

  test("an IPv6 host keeps its colons, port taken after the last") {
    assertEquals(
      Redirect.parse(RedirectKind.Moved, "1 2001:db8::1:6379"),
      Some(Redirect(RedirectKind.Moved, Node("2001:db8::1", 6379)))
    )
  }

  test("malformed redirects parse to None") {
    assertEquals(Redirect.parse(RedirectKind.Moved, "3999"), None)
    assertEquals(Redirect.parse(RedirectKind.Moved, "3999 127.0.0.1:notaport"), None)
  }

  test("an out-of-range port is rejected") {
    assertEquals(Redirect.parse(RedirectKind.Moved, "1 127.0.0.1:0"), None)
    assertEquals(Redirect.parse(RedirectKind.Moved, "1 127.0.0.1:-1"), None)
    assertEquals(Redirect.parse(RedirectKind.Moved, "1 127.0.0.1:70000"), None)
  }
}

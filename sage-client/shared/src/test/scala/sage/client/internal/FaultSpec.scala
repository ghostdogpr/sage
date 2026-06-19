package sage.client.internal

import sage.SageException.{ConnectionLost, NotConnected, ServerError}
import sage.cluster.{Node, Redirect, RedirectKind, Slot}

class FaultSpec extends munit.FunSuite {

  test("a MOVED reply categorizes as Redirected carrying the parsed redirect") {
    assertEquals(
      Fault.categorize(ServerError("MOVED", "3999 127.0.0.1:6379")),
      Fault.Redirected(Redirect(RedirectKind.Moved, Slot.at(3999).get, Node("127.0.0.1", 6379)))
    )
  }

  test("an ASK reply categorizes as Redirected") {
    assertEquals(
      Fault.categorize(ServerError("ASK", "42 127.0.0.1:7000")),
      Fault.Redirected(Redirect(RedirectKind.Ask, Slot.at(42).get, Node("127.0.0.1", 7000)))
    )
  }

  test("a READONLY reply categorizes as Demoted") {
    assertEquals(Fault.categorize(ServerError("READONLY", "You can't write against a read only replica.")), Fault.Demoted)
  }

  test("any other ServerError categorizes as Fatal") {
    assertEquals(Fault.categorize(ServerError("WRONGTYPE", "Operation against a key holding the wrong kind of value")), Fault.Fatal)
    assertEquals(Fault.categorize(ServerError("ERR", "foo bar")), Fault.Fatal)
    assertEquals(Fault.categorize(ServerError("ERR", "")), Fault.Fatal)
    // a redirect-shaped triple whose first token is not exactly MOVED/ASK is not a redirect
    assertEquals(Fault.categorize(ServerError("MOVE", "3999 127.0.0.1:6379")), Fault.Fatal)
  }

  test("NotConnected categorizes as a provably-unexecuted loss") {
    assertEquals(Fault.categorize(NotConnected()), Fault.Lost(mayHaveExecuted = false))
  }

  test("ConnectionLost carries through its mayHaveExecuted flag") {
    assertEquals(Fault.categorize(ConnectionLost(mayHaveExecuted = false)), Fault.Lost(mayHaveExecuted = false))
    assertEquals(Fault.categorize(ConnectionLost(mayHaveExecuted = true)), Fault.Lost(mayHaveExecuted = true))
  }

  test("an unrelated throwable categorizes as Fatal") {
    assertEquals(Fault.categorize(new RuntimeException("boom")), Fault.Fatal)
  }
}

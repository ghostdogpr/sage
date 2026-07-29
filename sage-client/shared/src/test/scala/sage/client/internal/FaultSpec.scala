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

  test("a TRYAGAIN reply categorizes as TryAgain") {
    assertEquals(Fault.categorize(ServerError("TRYAGAIN", "Multiple keys request during rehashing of slot")), Fault.TryAgain)
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

  test("an ownership or connection change forces a refresh past the throttle window") {
    val moved = Fault.Redirected(Redirect(RedirectKind.Moved, Slot.at(1).get, Node("a", 6379)))
    assertEquals(moved.refreshPolicy, RefreshPolicy.Forced)
    assertEquals(Fault.Demoted.refreshPolicy, RefreshPolicy.Forced)
    assertEquals(Fault.Lost(mayHaveExecuted = false).refreshPolicy, RefreshPolicy.Forced)
    assertEquals(Fault.Lost(mayHaveExecuted = true).refreshPolicy, RefreshPolicy.Forced)
  }

  test("an ASK refreshes throttled: it leaves slot ownership unchanged") {
    val ask = Fault.Redirected(Redirect(RedirectKind.Ask, Slot.at(1).get, Node("a", 6379)))
    assertEquals(ask.refreshPolicy, RefreshPolicy.Throttled)
  }

  test("a data error and a TryAgain refresh nothing: the slot mapping stays valid") {
    assertEquals(Fault.TryAgain.refreshPolicy, RefreshPolicy.Skip)
    assertEquals(Fault.Fatal.refreshPolicy, RefreshPolicy.Skip)
  }

  test("the policies order weakest to strongest, so mixed faults can take the strongest") {
    assert(RefreshPolicy.Skip.ordinal < RefreshPolicy.Throttled.ordinal)
    assert(RefreshPolicy.Throttled.ordinal < RefreshPolicy.Forced.ordinal)
  }
}

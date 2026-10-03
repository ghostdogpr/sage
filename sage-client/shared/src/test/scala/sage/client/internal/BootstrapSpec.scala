package sage.client.internal

import sage.SageException.{ConnectionLost, ServerError}
import sage.client.{AuthConfig, SageConfig}
import sage.commands.Connection
import sage.protocol.Frame

class BootstrapSpec extends munit.FunSuite {

  private def lines(auth: Option[AuthConfig], database: Int, clientName: Option[String]): Vector[String] =
    Bootstrap
      .commands(SageConfig(auth = auth, database = database, clientName = clientName))
      .map(c => (c.name +: c.args.map(_.asUtf8String)).mkString(" "))

  test("the default bootstrap is HELLO then library identification, no SELECT") {
    val cmds = lines(None, 0, None)
    assertEquals(cmds.head, "HELLO 3")
    assert(cmds.contains("CLIENT SETINFO LIB-NAME sage"), cmds.toString)
    assert(cmds.exists(_.startsWith("CLIENT SETINFO LIB-VER ")), cmds.toString)
    assert(!cmds.exists(_.startsWith("SELECT")), cmds.toString)
    assert(!cmds.exists(_.contains("SETNAME")), cmds.toString)
  }

  test("clientName adds CLIENT SETNAME") {
    assert(lines(None, 0, Some("my-app")).contains("CLIENT SETNAME my-app"))
  }

  test("a non-zero database appends SELECT last; zero adds none") {
    assertEquals(lines(None, 3, None).last, "SELECT 3")
    assert(!lines(None, 0, None).exists(_.startsWith("SELECT")))
  }

  test("HELLO carries AUTH when credentials are configured") {
    val first = Bootstrap.commands(SageConfig(auth = Some(AuthConfig("pw", "alice")))).head
    assertEquals(first.name, "HELLO")
    assert(first.args.map(_.asUtf8String).containsSlice(Vector("AUTH", "alice", "pw")))
  }

  // a dedicated connection whose transport answers each written command through `reply`; the second value reports whether it was closed
  private def connection(reply: (String, FakeTransport) => Seq[Frame]): (DedicatedConnection, () => Boolean) = {
    var transport: FakeTransport                        = null
    val factory: MultiplexedConnection.TransportFactory = (onFrame, onClosed) => {
      transport = new FakeTransport(onFrame, onClosed, payload => reply(payload.asUtf8String, transport))
      transport
    }
    (new DedicatedConnection(factory, new ManualScheduler), () => transport.closeCount > 0)
  }

  private def setupReply(setInfo: FakeTransport => Seq[Frame]): (String, FakeTransport) => Seq[Frame] = (payload, transport) =>
    if (payload.contains("HELLO")) Seq(Replies.hello) else if (payload.contains("SETINFO")) setInfo(transport) else Seq(Replies.ok)

  test("a CLIENT SETINFO error does not abort setup, so a pre-7.2 server still connects") {
    val (conn, closed) = connection(setupReply(_ => Seq(Frame.SimpleError("ERR Unknown subcommand or wrong number of arguments for 'SETINFO'."))))
    conn.handshake(Bootstrap.commands(SageConfig()), 1000)
    assert(!closed(), "connection must stay open when only library identification fails")
  }

  test("a step returns a server error reply without closing, so a server that denies tracking still connects (ADR-0045)") {
    val (conn, closed) = connection((_, _) => Seq(Frame.SimpleError("ERR This instance has cluster support disabled")))
    conn.start()
    val result         = conn.step(Connection.clientTrackingOnOptin, 1000)
    assert(!closed(), "connection must stay open when only tracking is denied")
    assertEquals(result, Some(ServerError("ERR", "This instance has cluster support disabled")))
  }

  test("a CLIENT SETINFO connection loss is NOT tolerated: it closes and throws") {
    val (conn, closed) = connection(setupReply { transport =>
      transport.close()
      Nil
    })
    val thrown         = intercept[ConnectionLost](conn.handshake(Bootstrap.commands(SageConfig()), 1000))
    assertEquals(thrown.mayHaveExecuted, false)
    assert(closed(), "a broken connection must be discarded even on a best-effort command")
  }

  test("a load-bearing command error aborts setup and closes the connection") {
    val (conn, closed) = connection((_, _) => Seq(Frame.SimpleError("NOAUTH Authentication required.")))
    val thrown         = intercept[ServerError](conn.handshake(Vector(Connection.hello(None)), 1000))
    assertEquals(thrown, ServerError("NOAUTH", "Authentication required."))
    assert(closed(), "connection must be closed when a load-bearing command fails")
  }
}

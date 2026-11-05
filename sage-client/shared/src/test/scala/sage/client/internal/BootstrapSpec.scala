package sage.client.internal

import scala.collection.mutable

import sage.SageException.{ConnectionLost, ServerError, UnsupportedServer}
import sage.client.{AuthConfig, RespProtocol, SageConfig}
import sage.commands.Connection
import sage.protocol.Frame

class BootstrapSpec extends munit.FunSuite {

  private def line(command: sage.commands.Command[?]): String = (command.name +: command.args.map(_.asUtf8String)).mkString(" ")

  private def lines(auth: Option[AuthConfig], database: Int, clientName: Option[String]): Vector[String] =
    Bootstrap(SageConfig(auth = auth, database = database, clientName = clientName)).setup.map(line)

  test("the default bootstrap is HELLO then library identification, no SELECT") {
    val cmds = lines(None, 0, None)
    assertEquals(line(Bootstrap(SageConfig()).hello), "HELLO 3")
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
    assertEquals(line(Bootstrap(SageConfig(auth = Some(AuthConfig("pw", "alice")))).hello), "HELLO 3 AUTH alice pw")
  }

  test("RESP2 authenticates the default user with AUTH <password> and a named user with AUTH <username> <password>") {
    assertEquals(Bootstrap(SageConfig(auth = Some(AuthConfig("pw")))).resp2Auth.map(line), Some("AUTH pw"))
    assertEquals(Bootstrap(SageConfig(auth = Some(AuthConfig("pw", "alice")))).resp2Auth.map(line), Some("AUTH alice pw"))
    assertEquals(Bootstrap(SageConfig()).resp2Auth, None)
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
    conn.handshake(Bootstrap(SageConfig()), 1000): Unit
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
    val thrown         = intercept[ConnectionLost](conn.handshake(Bootstrap(SageConfig()), 1000))
    assertEquals(thrown.mayHaveExecuted, false)
    assert(closed(), "a broken connection must be discarded even on a best-effort command")
  }

  test("a load-bearing command error aborts setup and closes the connection") {
    val (conn, closed) = connection((_, _) => Seq(Frame.SimpleError("NOAUTH Authentication required.")))
    val thrown         = intercept[ServerError](conn.handshake(Replies.helloOnly, 1000))
    assertEquals(thrown, ServerError("NOAUTH", "Authentication required."))
    assert(closed(), "connection must be closed when a load-bearing command fails")
  }

  // answers HELLO with `hello`, AUTH with `auth` and anything else with OK, and records each command; the setup writes one command at a time
  private def recording(hello: Frame, auth: Frame = Replies.ok): (DedicatedConnection, mutable.ArrayBuffer[String], () => Boolean) = {
    val written        = mutable.ArrayBuffer.empty[String]
    val (conn, closed) = connection { (payload, _) =>
      val tokens = payload.split("\r\n").toVector
      val words  = tokens.drop(2).grouped(2).map(_.head).toVector
      written += words.mkString(" ")
      words.head match {
        case "HELLO" => Seq(hello)
        case "AUTH"  => Seq(auth)
        case _       => Seq(Replies.ok)
      }
    }
    (conn, written, closed)
  }

  private val noProto        = Frame.SimpleError("NOPROTO unsupported protocol version")
  private val unknownCommand = Frame.SimpleError("ERR unknown command 'HELLO'")
  private val credentials    = Some(AuthConfig("pw", "alice"))

  test("under Auto, a server that answers HELLO 3 with NOPROTO is used with RESP2 and authenticated with AUTH") {
    val (conn, written, closed) = recording(noProto)
    val protocol                = conn.handshake(Bootstrap(SageConfig(auth = credentials)), 1000)
    assertEquals((protocol, conn.isResp2, closed()), (RespProtocol.Resp2, true, false))
    assertEquals(written.take(3).toVector, Vector("HELLO 3 AUTH alice pw", "AUTH alice pw", "CLIENT SETINFO LIB-NAME sage"))
  }

  test("under Auto, a server that does not know HELLO is used with RESP2, and the default user authenticates with AUTH <password>") {
    val (conn, written, closed) = recording(unknownCommand)
    val protocol                = conn.handshake(Bootstrap(SageConfig(auth = Some(AuthConfig("pw")))), 1000)
    assertEquals((protocol, closed()), (RespProtocol.Resp2, false))
    assertEquals(written.take(2).toVector, Vector("HELLO 3 AUTH default pw", "AUTH pw"))
  }

  test("under Auto, a RESP2 fallback without credentials sends no AUTH") {
    val (conn, written, _) = recording(unknownCommand)
    assertEquals(conn.handshake(Bootstrap(SageConfig()), 1000), RespProtocol.Resp2)
    assert(!written.exists(_.startsWith("AUTH")), written.toString)
  }

  test("a server that accepts HELLO 3 is used with RESP3 under Auto") {
    val (conn, written, _) = recording(Replies.hello)
    assertEquals((conn.handshake(Bootstrap(SageConfig(auth = credentials)), 1000), conn.isResp2), (RespProtocol.Resp3, false))
    assert(!written.exists(_.startsWith("AUTH")), written.toString)
  }

  test("under Resp3, a server that rejects HELLO 3 fails with UnsupportedServer and the connection closes") {
    Seq(noProto, unknownCommand).foreach { rejection =>
      val (conn, written, closed) = recording(rejection)
      val thrown                  = intercept[UnsupportedServer](conn.handshake(Bootstrap(SageConfig(protocol = RespProtocol.Resp3)), 1000))
      assert(thrown.getMessage.contains("RespProtocol.Auto"), thrown.getMessage)
      assert(closed(), "connection must be closed when the server rejects HELLO 3")
      assertEquals(written.toVector, Vector("HELLO 3"))
    }
  }

  private val proto2Hello = Frame.Array(Vector(Replies.bulk("server"), Replies.bulk("proxy"), Replies.bulk("proto"), Frame.Integer(2)))

  test("under Auto, a server that accepts HELLO 3 but keeps protocol 2 is used with RESP2, already authenticated by HELLO") {
    val (conn, written, _) = recording(proto2Hello)
    assertEquals(conn.handshake(Bootstrap(SageConfig(auth = credentials)), 1000), RespProtocol.Resp2)
    assert(!written.exists(_.startsWith("AUTH")), written.toString)
  }

  test("under Resp3, a server that accepts HELLO 3 but keeps protocol 2 fails with UnsupportedServer") {
    val (conn, _, closed) = recording(proto2Hello)
    val thrown            = intercept[UnsupportedServer](conn.handshake(Bootstrap(SageConfig(protocol = RespProtocol.Resp3)), 1000))
    assert(thrown.getMessage.contains("protocol 2"), thrown.getMessage)
    assert(closed())
  }

  test("under Resp2, the setup skips HELLO and authenticates with AUTH") {
    val (conn, written, _) = recording(Replies.hello)
    assertEquals(conn.handshake(Bootstrap(SageConfig(auth = credentials, protocol = RespProtocol.Resp2)), 1000), RespProtocol.Resp2)
    assertEquals(written.head, "AUTH alice pw")
    assert(!written.exists(_.startsWith("HELLO")), written.toString)
  }

  test("a wrong password in HELLO 3 fails the setup without falling back to RESP2") {
    val wrongPass               = Frame.SimpleError("WRONGPASS invalid username-password pair or user is disabled.")
    val (conn, written, closed) = recording(wrongPass)
    val thrown                  = intercept[ServerError](conn.handshake(Bootstrap(SageConfig(auth = credentials)), 1000))
    assertEquals(thrown.code, "WRONGPASS")
    assert(closed(), "connection must be closed when authentication fails")
    assertEquals(written.toVector, Vector("HELLO 3 AUTH alice pw"))
  }

  test("a wrong password in the RESP2 AUTH fails the setup") {
    val (conn, _, closed) = recording(unknownCommand, auth = Frame.SimpleError("ERR invalid password"))
    val thrown            = intercept[ServerError](conn.handshake(Bootstrap(SageConfig(auth = Some(AuthConfig("pw")))), 1000))
    assertEquals(thrown, ServerError("ERR", "invalid password"))
    assert(closed(), "connection must be closed when authentication fails")
  }
}

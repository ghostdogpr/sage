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
    conn.handshake(Bootstrap(SageConfig()), 1000)
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

  test("on RESP2 without credentials, a NOAUTH reply to CLIENT SETINFO aborts setup, as HELLO 3's does on RESP3") {
    val (conn, closed) = connection((_, _) => Seq(Frame.SimpleError("NOAUTH Authentication required.")))
    intercept[ServerError](conn.handshake(Bootstrap(SageConfig(protocol = RespProtocol.Resp2)), 1000))
    assert(closed(), "connection must be closed when the server requires authentication")
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
  // Redis 5 echoes the first arguments, which include the credentials
  private val unknownCommand = Frame.SimpleError("ERR unknown command `HELLO`, with args beginning with: `3`, `AUTH`, `alice`, `pw`, ")
  private val proto2Hello    = Frame.Array(Vector(Replies.bulk("server"), Replies.bulk("proxy"), Replies.bulk("proto"), Frame.Integer(2)))
  private val credentials    = Some(AuthConfig("pw", "alice"))

  test("the setup negotiates the protocol, and authenticates with AUTH only when HELLO 3 was skipped or rejected") {
    Seq(
      (RespProtocol.Auto, noProto, credentials, true, Vector("HELLO 3 AUTH alice pw", "AUTH alice pw", "CLIENT SETINFO LIB-NAME sage")),
      (RespProtocol.Auto, unknownCommand, Some(AuthConfig("pw")), true, Vector("HELLO 3 AUTH default pw", "AUTH pw", "CLIENT SETINFO LIB-NAME sage")),
      (RespProtocol.Auto, unknownCommand, None, true, Vector("HELLO 3", "CLIENT SETINFO LIB-NAME sage")),
      (RespProtocol.Auto, Replies.hello, credentials, false, Vector("HELLO 3 AUTH alice pw", "CLIENT SETINFO LIB-NAME sage")),
      (RespProtocol.Auto, proto2Hello, credentials, true, Vector("HELLO 3 AUTH alice pw", "CLIENT SETINFO LIB-NAME sage")),
      (RespProtocol.Resp2, Replies.hello, credentials, true, Vector("AUTH alice pw", "CLIENT SETINFO LIB-NAME sage"))
    ).foreach { case (protocol, hello, auth, resp2, expected) =>
      val (conn, written, closed) = recording(hello)
      conn.handshake(Bootstrap(SageConfig(auth = auth, protocol = protocol)), 1000)
      assertEquals((conn.isResp2, closed(), written.take(expected.size).toVector), (resp2, false, expected))
    }
  }

  test("under Resp3, a server that rejects HELLO 3 or keeps protocol 2 fails with UnsupportedServer and the connection closes") {
    Seq(noProto -> "(NOPROTO)", unknownCommand -> "(ERR)", proto2Hello -> "protocol 2").foreach { case (hello, reason) =>
      val (conn, written, closed) = recording(hello)
      val thrown                  =
        intercept[UnsupportedServer](conn.handshake(Bootstrap(SageConfig(auth = credentials, protocol = RespProtocol.Resp3)), 1000))
      val message                 = thrown.getMessage
      assert(message.contains(reason) && message.contains("RespProtocol.Auto") && !message.contains("pw"), message)
      assert(closed(), "connection must be closed when the server rejects HELLO 3")
      assertEquals(written.toVector, Vector("HELLO 3 AUTH alice pw"))
    }
  }

  test("a wrong password in the RESP2 AUTH fails the setup") {
    val (conn, _, closed) = recording(unknownCommand, auth = Frame.SimpleError("ERR invalid password"))
    val thrown            = intercept[ServerError](conn.handshake(Bootstrap(SageConfig(auth = Some(AuthConfig("pw")))), 1000))
    assertEquals(thrown, ServerError("ERR", "invalid password"))
    assert(closed(), "connection must be closed when authentication fails")
  }
}

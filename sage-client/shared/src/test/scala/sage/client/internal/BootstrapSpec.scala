package sage.client.internal

import scala.util.{Failure, Success}

import sage.SageException.{ConnectionLost, ServerError}
import sage.client.AuthConfig
import sage.commands.Connection

class BootstrapSpec extends munit.FunSuite {

  private def lines(auth: Option[AuthConfig], database: Int, clientName: Option[String]): Vector[String] =
    Bootstrap.commands(auth, database, clientName).map(c => (c.name +: c.args.map(_.asUtf8String)).mkString(" "))

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
    val first = Bootstrap.commands(Some(AuthConfig("pw", "alice")), 0, None).head
    assertEquals(first.name, "HELLO")
    assert(first.args.map(_.asUtf8String).containsSlice(Vector("AUTH", "alice", "pw")))
  }

  test("a CLIENT SETINFO error does not abort setup, so a pre-7.2 server still connects") {
    val unknown = Failure(ServerError("ERR", "Unknown subcommand or wrong number of arguments for 'SETINFO'."))
    var closed  = false
    Bootstrap.run(
      Bootstrap.commands(None, 0, None),
      connectTimeoutMillis = 1000,
      submit = (c, cb) => cb(if (c.name == "CLIENT" && c.args.head.asUtf8String == "SETINFO") unknown else Success(())),
      close = () => closed = true
    )
    assert(!closed, "connection must stay open when only library identification fails")
  }

  test("a CLIENT TRACKING error is tolerated and reported, so a server that denies tracking still connects (ADR-0045)") {
    val denied    = Failure(ServerError("ERR", "This instance has cluster support disabled"))
    var closed    = false
    var tolerated = Vector.empty[String]
    Bootstrap.run(
      Bootstrap.commands(None, 0, None) :+ Connection.clientTrackingOnOptin,
      connectTimeoutMillis = 1000,
      submit = (c, cb) => cb(if (c.name == "CLIENT" && c.args.head.asUtf8String == "TRACKING") denied else Success(())),
      close = () => closed = true,
      onTolerated = c => tolerated = tolerated :+ s"${c.name} ${c.args.head.asUtf8String}"
    )
    assert(!closed, "connection must stay open when only tracking is denied")
    assertEquals(tolerated, Vector("CLIENT TRACKING"))
  }

  test("a CLIENT SETINFO connection loss is NOT tolerated: it closes and throws") {
    var closed = false
    val thrown = intercept[ConnectionLost] {
      Bootstrap.run(
        Bootstrap.commands(None, 0, None),
        connectTimeoutMillis = 1000,
        submit = (c, cb) =>
          cb(
            if (c.name == "CLIENT" && c.args.head.asUtf8String == "SETINFO") Failure(ConnectionLost(mayHaveExecuted = false))
            else Success(())
          ),
        close = () => closed = true
      )
    }
    assertEquals(thrown.mayHaveExecuted, false)
    assert(closed, "a broken connection must be discarded even on a best-effort command")
  }

  test("a load-bearing command error aborts setup and closes the connection") {
    val error  = ServerError("NOAUTH", "Authentication required.")
    var closed = false
    val thrown = intercept[ServerError] {
      Bootstrap.run(Vector(Connection.hello(None)), 1000, (_, cb) => cb(Failure(error)), () => closed = true)
    }
    assertEquals(thrown, error)
    assert(closed, "connection must be closed when a load-bearing command fails")
  }
}

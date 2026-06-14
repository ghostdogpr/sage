package sage.client.internal

import sage.client.AuthConfig

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
}

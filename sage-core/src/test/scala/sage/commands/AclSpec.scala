package sage.commands

class AclSpec extends munit.FunSuite with BothProtocols {

  test("ACL GETUSER decodes the RESP2 flat arrays like the RESP3 maps and sets, and a missing user as None") {
    decodesAlike(
      Acl.aclGetUser("alice"),
      "*12\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*1\r\n$2\r\nab\r\n$8\r\ncommands\r\n$10\r\n-@all +get\r\n" +
        "$4\r\nkeys\r\n$3\r\n~k*\r\n$8\r\nchannels\r\n$0\r\n\r\n$9\r\nselectors\r\n*1\r\n*6\r\n$8\r\ncommands\r\n$10\r\n-@all +set\r\n" +
        "$4\r\nkeys\r\n$3\r\n~x*\r\n$8\r\nchannels\r\n$0\r\n\r\n",
      "%6\r\n$5\r\nflags\r\n~2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n$9\r\npasswords\r\n*1\r\n$2\r\nab\r\n$8\r\ncommands\r\n$10\r\n-@all +get\r\n" +
        "$4\r\nkeys\r\n$3\r\n~k*\r\n$8\r\nchannels\r\n$0\r\n\r\n$9\r\nselectors\r\n*1\r\n%3\r\n$8\r\ncommands\r\n$10\r\n-@all +set\r\n" +
        "$4\r\nkeys\r\n$3\r\n~x*\r\n$8\r\nchannels\r\n$0\r\n\r\n",
      Some(
        AclUser(
          flags = Vector("on", "sanitize-payload"),
          passwords = Vector("ab"),
          commands = "-@all +get",
          keys = "~k*",
          channels = "",
          selectors = Vector(Map("commands" -> "-@all +set", "keys" -> "~x*", "channels" -> ""))
        )
      )
    )
    decodesAlike(Acl.aclGetUser("nobody"), "$-1\r\n", "_\r\n", None)
  }

  test("ACL LOG decodes the RESP2 bulk-string age like the RESP3 double") {
    val fields =
      "$5\r\ncount\r\n:1\r\n$6\r\nreason\r\n$4\r\nauth\r\n$7\r\ncontext\r\n$8\r\ntoplevel\r\n$6\r\nobject\r\n$4\r\nAUTH\r\n" +
        "$8\r\nusername\r\n$9\r\nwronguser\r\n$11\r\nclient-info\r\n$4\r\nid=8\r\n$8\r\nentry-id\r\n:0\r\n"
    decodesAlike(
      Acl.aclLog(Some(1L)),
      "*1\r\n*16\r\n" + fields + "$11\r\nage-seconds\r\n$5\r\n2.125\r\n",
      "*1\r\n%8\r\n" + fields + "$11\r\nage-seconds\r\n,2.125\r\n",
      Vector(AclLogEntry(1L, "auth", "toplevel", "AUTH", "wronguser", 2.125, "id=8", 0L))
    )
  }
}

package sage.commands

import sage.protocol.Frames.wire

/**
  * Checks that a command decodes the RESP2 and RESP3 forms of the same server reply to the same result. The replies are raw wire text as
  * Redis or Valkey sent it after `HELLO 2` and `HELLO 3`.
  */
trait BothProtocols { this: munit.FunSuite =>

  protected def decodesAlike[A](command: Command[A], resp2: String, resp3: String, expected: A)(using munit.Location): Unit = {
    assertEquals(Reply.decode(command, wire(resp2)).toEither, Right(expected), "RESP2 reply")
    assertEquals(Reply.decode(command, wire(resp3)).toEither, Right(expected), "RESP3 reply")
  }
}

package sage.integration.commands

import scala.concurrent.duration.*

import sage.SageException.NotCacheable
import sage.commands.Commands
import sage.integration.BothServersSuite

class CachingSuite extends BothServersSuite {

  clientsTest("a repeated cached read is served locally and a server-side write evicts it via invalidation")(cachedReadIsInvalidated(_, _, "csc:key"))

  clientTest("cached rejects a non-read-only command with NotCacheable") { client =>
    failsWith[NotCacheable](client.cached(Commands.set[String, String]("csc:write", "v"), 1.minute))
  }
}

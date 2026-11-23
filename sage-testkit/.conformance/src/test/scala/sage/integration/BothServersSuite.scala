package sage.integration

import scala.annotation.unused

import kyo.compat.*
import munit.{Location, TestOptions}

import sage.client.SageConfig
import sage.client.inmemory.InMemoryClient
import sage.client.internal.Client
import sage.testkit.InMemoryServer

/**
  * Stands in for the integration tests' `BothServersSuite` so that their command suites run unchanged against the in-memory client. Every
  * test gets a new client with an empty keyspace. Tests written for only Redis or only Valkey run too, because the in-memory client aims
  * to behave like both.
  */
abstract class BothServersSuite extends ClientAssertions {

  protected def clientTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit =
    clientsTest(options)((client, _) => body(client))

  // two clients of one keyspace, like two connections to the same server
  protected def clientsTest(options: TestOptions)(body: (Client[CIO, String], Client[CIO, String]) => CIO[Any])(using Location): Unit =
    if (BothServersSuite.unsupported.exists(options.name.startsWith(_))) test(options.ignore)(())
    else
      test(options) {
        val server = InMemoryServer()
        val first  = InMemoryClient(server)
        val second = InMemoryClient(server)
        CIO.ensure(first.close.flatMap(_ => second.close))(body(first, second)).unsafeRun
      }

  // The container hooks below exist so that suites mixing server-level tests with ordinary ones compile. Tests that use them are
  // listed as unsupported, so their bodies never run.
  protected def serverTest(options: TestOptions)(@unused body: Nothing => CIO[Any])(using Location): Unit = test(options.ignore)(())

  protected def configOf(@unused server: Any): SageConfig = SageConfig()

  protected def connectAndUse[A](@unused config: SageConfig)(body: Client[CIO, String] => CIO[A]): CIO[A] = {
    val client = InMemoryClient()
    CIO.ensure(client.close)(body(client))
  }

  protected def redisTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit = clientTest(options)(body)

  protected def valkeyTest(options: TestOptions)(body: Client[CIO, String] => CIO[Any])(using Location): Unit = clientTest(options)(body)

  // only the ignored connection-loss test uses this, so it does not wait for anything
  protected def awaitCalls[A](commandStats: CIO[String], @unused command: String, @unused by: Long)(next: CIO[Unit] => CIO[A]): CIO[A] =
    commandStats.flatMap(_ => next(CIO.unit))
}

object BothServersSuite {

  /**
    * Tests of commands and options that the in-memory client does not implement, by the start of the test name. They are reported as
    * ignored and still run against Redis and Valkey in the integration tests.
    */
  val unsupported: Set[String] = Set(
    "GETRANGE SETRANGE STRLEN",
    "LCS finds",
    "DIGEST returns",
    "DELEX deletes",
    "MSETEX sets",
    "INCREX increments",
    "COPY copies",
    "EXPIRE conditions",
    "RANDOMKEY returns",
    "RENAME moves",
    "SORT orders",
    "SORT BY/GET",
    "SORT_RO reads",
    "MOVE relocates",
    "DUMP and RESTORE",
    "MIGRATE reports",
    "OBJECT exposes",
    "OBJECT FREQ",
    "DELIFEQ deletes",
    "HRANDFIELD returns",
    "HEXPIRE/HTTL/HPERSIST",
    "a field-TTL command",
    "HEXPIREAT pins",
    "HPTTL and HPEXPIRETIME",
    "HGETDEL returns",
    "HGETEX returns",
    "HSETEX sets",
    "LPUSHX and RPUSHX",
    "LPOS finds",
    "LMOVE shifts",
    "BLMOVE and BLMPOP",
    "SPOP and SRANDMEMBER",
    "SMOVE relocates",
    "SDIFF SINTER SUNION and their STORE forms",
    "ZRANGE BYLEX",
    "ZRANGESTORE copies",
    "ZPOPMIN ZPOPMAX and ZMPOP",
    "BZPOPMIN and BZMPOP",
    "ZRANDMEMBER draws",
    "ZUNION ZINTER",
    "ZREMRANGEBYRANK BYSCORE and BYLEX",
    // the in-memory client runs no Lua scripts, and its locks have no lease
    "automatic renewal extends the lease",
    "expired ownership cannot renew",
    "script cache flush",
    "the computed sha matches",
    "the native EVALSHA path recovers from NOSCRIPT",
    "the composable EVAL guard",
    "RateLimiterClient.command exposes",
    "accrues at the exact rate",
    "refills continuously",
    "a backward server clock",
    "a non-integer refill rate",
    "a clock rollback folds",
    "a large refill period",
    "reusing a namespace with alternating policies",
    "malformed bucket fields",
    "an already-full bucket",
    // the in-memory client has no connections to kill, and no INFO
    "a standalone lock survives connection loss",
    "closing the client releases its server connection"
  )
}

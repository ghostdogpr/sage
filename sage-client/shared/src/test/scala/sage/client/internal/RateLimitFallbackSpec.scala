package sage.client.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import kyo.compat.*

import sage.SageException.{InvalidArgument, ServerError}
import sage.commands.Command
import sage.ratelimit.{Decision, RateLimit, RateLimiter}

class RateLimitFallbackSpec extends munit.FunSuite {

  private given ExecutionContext = munitExecutionContext

  private def executor(limit: RateLimit = RateLimit.perSecond(10)) =
    new RateLimitExecutor[String](RateLimiter[String](limit))

  test("a NOSCRIPT loads the script once and retries the EVALSHA") {
    var loads    = 0
    var evalShas = 0
    val runner   = new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] = command.name match {
        case "EVALSHA" =>
          evalShas += 1
          if (loads == 0) CIO.fail(ServerError("NOSCRIPT", ""))
          else CIO.value(Decision.Allowed(3, 1.second).asInstanceOf[A])
        case "SCRIPT"  => loads += 1; CIO.value("sha".asInstanceOf[A])
        case other     => CIO.fail(ServerError("ERR", s"unexpected $other"))
      }
    }
    for {
      result <- executor().evalSha(runner, "k", 1, peek = false).unsafeRun
    } yield {
      assertEquals(result, Decision.Allowed(3, 1.second))
      assertEquals(loads, 1)
      assertEquals(evalShas, 2)
    }
  }

  test("a non-NOSCRIPT error propagates and never loads") {
    var loads  = 0
    val runner = new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] =
        if (command.name == "SCRIPT") { loads += 1; CIO.value("sha".asInstanceOf[A]) }
        else CIO.fail(ServerError("WRONGTYPE", "nope"))
    }
    for {
      failed <- executor().evalSha(runner, "k", 1, peek = false).unsafeRun.failed
    } yield {
      failed match {
        case e: ServerError => assertEquals(e.code, "WRONGTYPE")
        case other          => fail(s"expected ServerError, got $other")
      }
      assertEquals(loads, 0)
    }
  }

  test("a misconfigured policy fails through the effect without touching the runner") {
    var calls  = 0
    val runner = new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] = { calls += 1; CIO.fail(ServerError("ERR", "should not run")) }
    }
    val bad    = new RateLimitExecutor[String](RateLimiter[String](RateLimit(0, 1, 1.second)))
    for {
      failed <- bad.evalSha(runner, "k", 1, peek = false).unsafeRun.failed
    } yield {
      failed match {
        case e: InvalidArgument => assert(e.getMessage.contains("capacity must be > 0"), e.getMessage)
        case other              => fail(s"expected InvalidArgument, got $other")
      }
      assertEquals(calls, 0)
    }
  }
}

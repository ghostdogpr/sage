package sage.client.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.duration.*

import cats.effect.{IO, Outcome}
import kyo.compat.*

import sage.SageException.{LockLost, ServerError, TimedOut}
import sage.backend.SageClient
import sage.commands.Command
import sage.protocol.Frame

class CeLockCancellationSpec extends LockCancellationSpec {
  override protected def tryWithLock[A](commands: CommandRunner[CIO, String], lease: FiniteDuration)(body: CIO[A]): CIO[Option[A]] =
    CIO.lift(new SageClient.Lowered(new LockTestClient(commands)).lock[String](lease).tryWithLock("key")(body.lower))

  override protected def withLock[A](commands: CommandRunner[CIO, String], lease: FiniteDuration, wait: FiniteDuration)(body: CIO[A]): CIO[A] =
    CIO.lift(new SageClient.Lowered(new LockTestClient(commands)).lock[String](lease).withLock("key", wait)(body.lower))

  List("acquire", "renew", "release").foreach { stalled =>
    test(s"a delayed $stalled callback does not hold up the lock deadline") {
      val commands  = new CommandRunner[CIO, String] {
        def run[A](command: Command[A]): CIO[A] = CIO.async { callback =>
          val result = command.decode(Frame.Integer(1)).toTry
          if (command.args(4).asUtf8String == stalled) Scheduler.real.after(5.seconds)(callback(result))
          else callback(result)
        }
      }
      val failure   = ServerError("ERR", "body failed")
      val operation = stalled match {
        case "acquire" => withLock(commands, 300.millis, 50.millis)(CIO.fail(new AssertionError("body ran after acquisition timeout")))
        case "renew"   => tryWithLock(commands, 300.millis)(CIO.never).unit
        case _         => tryWithLock(commands, 300.millis)(CIO.fail(failure)).unit
      }
      val check     = for {
        started <- IO.monotonic
        result  <- operation.lower.attempt
        ended   <- IO.monotonic
      } yield {
        val error = result.swap.toOption.getOrElse(fail("expected the lock to fail"))
        stalled match {
          case "acquire" => assert(error.isInstanceOf[TimedOut], error.toString)
          case "renew"   => assert(error.isInstanceOf[LockLost], error.toString)
          case _         => assert(error eq failure)
        }
        assert(ended - started < 2.seconds, s"$stalled waited for its delayed callback: ${ended - started}")
      }
      CIO.lift(check).unsafeRun
    }
  }

  test("a body that cancels itself still releases the lock") {
    val released = new AtomicBoolean(false)
    val body     = CIO.lift(IO.canceled *> IO.never[Unit])
    val check    = tryWithLock(runner(released, new AtomicInteger(0), false), 300.millis)(body).lower.start.flatMap(_.join).map { outcome =>
      outcome match {
        case Outcome.Canceled() => ()
        case other              => fail(s"expected cancellation, got $other")
      }
      assert(released.get())
    }
    CIO.lift(check.timeout(2.seconds)).unsafeRun
  }
}

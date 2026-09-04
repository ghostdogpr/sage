package sage.client.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.control.ControlThrowable

import kyo.compat.*

import sage.backend.SageClient

class OxLockCancellationSpec extends LockCancellationSpec {
  private given ExecutionContext = munitExecutionContext

  override protected def tryWithLock[A](commands: CommandRunner[CIO, String], lease: FiniteDuration)(body: CIO[A]): CIO[Option[A]] =
    CIO.deferLift(new SageClient.Lowered(new LockTestClient(commands)).lock[String](lease).tryWithLock("key")(body.lower))

  override protected def withLock[A](commands: CommandRunner[CIO, String], lease: FiniteDuration, wait: FiniteDuration)(body: CIO[A]): CIO[A] =
    CIO.deferLift(new SageClient.Lowered(new LockTestClient(commands)).lock[String](lease).withLock("key", wait)(body.lower))

  test("a body control exception ends the scope and releases ownership") {
    val released = new AtomicBoolean(false)
    val renewals = new AtomicInteger(0)
    val failure  = new ControlThrowable {}
    CIO.deferLift {
      val caught =
        try {
          ox.timeoutOption(2.seconds) {
            tryWithLock(runner(released, renewals, false), 300.millis)(CIO.defer(throw failure)).lower
          }
          None
        } catch {
          case cause: ControlThrowable => Some(cause)
        }
      assertEquals(caught, Some(failure))
      assert(released.get())
    }.unsafeRun
  }
}

package sage.client.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.duration.*

import kyo.compat.*
import zio.ZIO

import sage.SageException
import sage.backend.SageClient

class ZioLockCancellationSpec extends LockCancellationSpec {
  override protected def tryWithLock[A](commands: CommandRunner[CIO, String], lease: FiniteDuration)(body: CIO[A]): CIO[Option[A]] =
    CIO.lift(new SageClient.Lowered(new LockTestClient(commands)).lock[String](lease).tryWithLock("key")(body.lower.refineToOrDie[SageException]))

  override protected def withLock[A](commands: CommandRunner[CIO, String], lease: FiniteDuration, wait: FiniteDuration)(body: CIO[A]): CIO[A] =
    CIO.lift(new SageClient.Lowered(new LockTestClient(commands)).lock[String](lease).withLock("key", wait)(body.lower.refineToOrDie[SageException]))

  test("a body defect ends the scope and releases ownership") {
    val released = new AtomicBoolean(false)
    val failure  = new IllegalStateException("body defect")
    val check    = tryWithLock(runner(released, new AtomicInteger(0), false), 300.millis)(CIO.lift(ZIO.die(failure))).lower.exit.map { exit =>
      assert(exit.causeOption.exists(_.defects.contains(failure)))
      assert(released.get())
    }
    CIO.lift(check.timeoutFail(new AssertionError("defect left the lock scope running"))(zio.Duration.fromSeconds(2))).unsafeRun
  }

  test("a body that interrupts itself ends the scope and releases ownership") {
    val released = new AtomicBoolean(false)
    val check    = tryWithLock(runner(released, new AtomicInteger(0), false), 300.millis)(CIO.lift(ZIO.interrupt)).lower.exit.map { exit =>
      assert(exit.isInterrupted)
      assert(released.get())
    }
    CIO.lift(check.timeoutFail(new AssertionError("interruption left the lock scope running"))(zio.Duration.fromSeconds(2))).unsafeRun
  }
}

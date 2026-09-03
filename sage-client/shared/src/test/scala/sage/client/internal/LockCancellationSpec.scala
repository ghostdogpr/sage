package sage.client.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import kyo.compat.*

import sage.{Message, PatternMessage}
import sage.SageException.{LockLost, ServerError}
import sage.codec.ValueCodec
import sage.commands.{Command, Pipeline}
import sage.protocol.Frame
import sage.ratelimit.Decision

abstract class LockCancellationSpec extends munit.FunSuite {
  override val munitTimeout      = 10.seconds
  private given ExecutionContext = munitExecutionContext

  protected def tryWithLock[A](commands: CommandRunner[CIO, String], lease: FiniteDuration)(body: CIO[A]): CIO[Option[A]] =
    new LockExecutor[String](lease, "cancel").tryWithLock(commands, "key")(body)

  protected def withLock[A](commands: CommandRunner[CIO, String], lease: FiniteDuration, wait: FiniteDuration)(body: CIO[A]): CIO[A] =
    new LockExecutor[String](lease, "cancel").withLock(commands, "key", wait)(body)

  protected def runner(released: AtomicBoolean, renewals: AtomicInteger, stallRelease: Boolean): CommandRunner[CIO, String] =
    new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] = CIO.defer(()).flatMap { _ =>
        command.args(4).asUtf8String match {
          case "renew"   => renewals.incrementAndGet()
          case "release" => released.set(true)
          case _         => ()
        }
        if (stallRelease && command.args(4).asUtf8String == "release") CIO.never
        else command.decode(Frame.Integer(1)).fold(CIO.fail(_), CIO.value(_))
      }
    }

  test("cancelling a protected body releases ownership and stops renewal") {
    val released    = new AtomicBoolean(false)
    val renewals    = new AtomicInteger(0)
    val bodyStopped = new AtomicBoolean(false)
    val body        = CIO.ensure(CIO.defer(bodyStopped.set(true)))(CIO.never)
    CIO
      .timeout(150.millis)(tryWithLock(runner(released, renewals, false), 300.millis)(body))
      .flatMap { result =>
        assertEquals(result, None)
        // Kyo runs interrupt finalizers asynchronously.
        CIO.sleep(100.millis).flatMap { _ =>
          assert(released.get())
          assert(bodyStopped.get())
          val count = renewals.get()
          CIO.sleep(400.millis).map(_ => assertEquals(renewals.get(), count))
        }
      }
      .unsafeRun
  }

  test("losing ownership cancels the protected body") {
    val bodyStopped = new AtomicBoolean(false)
    val commands    = new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] =
        command.decode(Frame.Integer(if (command.args(4).asUtf8String == "renew") 0 else 1)).fold(CIO.fail(_), CIO.value(_))
    }
    val body        = CIO.ensure(CIO.defer(bodyStopped.set(true)))(CIO.never)
    tryWithLock(commands, 300.millis)(body).liftToTry.flatMap { result =>
      assert(result.failed.get.isInstanceOf[LockLost], result.toString)
      CIO.sleep(100.millis).map(_ => assert(bodyStopped.get()))
    }.unsafeRun
  }

  test("cleanup stays bounded when the release command never replies") {
    val released = new AtomicBoolean(false)
    val renewals = new AtomicInteger(0)
    val failure  = ServerError("ERR", "body failed")
    tryWithLock(runner(released, renewals, true), 300.millis)(CIO.fail(failure)).unsafeRun.failed.map { error =>
      assert(error eq failure)
      assert(released.get())
    }
  }

  test("a contended acquisition can be cancelled before its wait timeout") {
    val busy = new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] = command.decode(Frame.Integer(0)).fold(CIO.fail(_), CIO.value(_))
    }
    CIO.timeout(100.millis)(withLock(busy, 3.seconds, 30.seconds)(CIO.unit)).unsafeRun.map(result => assertEquals(result, None))
  }
}

final class LockTestClient(runner: CommandRunner[CIO, String]) extends Client[CIO, String] {
  private def unsupported[A]: CIO[A] = CIO.fail(new AssertionError("unexpected client operation in a lock test"))

  def run[A](command: Command[A]): CIO[A]                                                                                        = runner.run(command)
  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A]                                                                = unsupported
  private[sage] def pipeline[Out, R](p: Pipeline[Out, R]): CIO[Out]                                                              = unsupported
  private[sage] def pipelineAttempt[Out, R](p: Pipeline[Out, R]): CIO[R]                                                         = unsupported
  def transaction[A](body: TransactionScope[CIO, String] => CIO[A]): CIO[A]                                                      = unsupported
  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]]                       = unsupported
  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]]                = unsupported
  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]]                  = unsupported
  private[sage] def scanTargets: CIO[Vector[ScanTarget]]                                                                         = unsupported
  private[sage] def runOn[A](target: ScanTarget, command: Command[A]): CIO[A]                                                    = unsupported
  private[sage] def rateLimitAcquire[RK](executor: RateLimitExecutor[RK], subject: RK, cost: Long, peek: Boolean): CIO[Decision] = unsupported
  private[sage] def lockTryWith[LK, A](executor: LockExecutor[LK], key: LK)(body: => CIO[A]): CIO[Option[A]]                     =
    executor.tryWithLock(this, key)(body)
  private[sage] def lockWith[LK, A](executor: LockExecutor[LK], key: LK, waitTimeout: FiniteDuration)(body: => CIO[A]): CIO[A]   =
    executor.withLock(this, key, waitTimeout)(body)
  def close: CIO[Unit]                                                                                                           = CIO.unit
}

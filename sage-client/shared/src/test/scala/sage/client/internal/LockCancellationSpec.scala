package sage.client.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.{ExecutionContext, Promise}
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

  protected def tryWithLock[A](commands: SharedRunner, lease: FiniteDuration)(body: CIO[A]): CIO[Option[A]] =
    new LockExecutor[String](lease, "cancel", replicaAcknowledgement = true).tryWithLock(commands, "key")(body)

  protected def withLock[A](commands: SharedRunner, lease: FiniteDuration, wait: FiniteDuration)(body: CIO[A]): CIO[A] =
    new LockExecutor[String](lease, "cancel", replicaAcknowledgement = true).withLock(commands, "key", wait)(body)

  protected def runner(
    released: AtomicBoolean,
    renewals: AtomicInteger,
    stallRelease: Boolean,
    releaseCompleted: () => Unit = () => ()
  ): SharedRunner =
    new SharedRunner {
      def run[A](command: Command[A]): CIO[A] = CIO.defer(()).flatMap { _ =>
        command.args(4).asUtf8String match {
          case "renew"   => renewals.incrementAndGet()
          case "release" =>
            released.set(true)
            releaseCompleted()
          case _         => ()
        }
        if (stallRelease && command.args(4).asUtf8String == "release") CIO.never
        else command.decode(Frame.Integer(1)).fold(CIO.fail(_), CIO.value(_))
      }
    }

  test("cancelling a protected body releases ownership and runs its finalizer") {
    val released         = new AtomicBoolean(false)
    val renewals         = new AtomicInteger(0)
    val bodyStopped      = new AtomicBoolean(false)
    val bodyStarted      = Promise[Unit]()
    val remainingCleanup = new AtomicInteger(2)
    val cleanupCompleted = Promise[Unit]()
    val signalCleanup    = () => {
      if (remainingCleanup.decrementAndGet() == 0) {
        val _ = cleanupCompleted.trySuccess(())
      }
      ()
    }
    val releaseCompleted = () => signalCleanup()
    val body             = CIO.ensure(CIO.defer {
      bodyStopped.set(true)
      signalCleanup()
    })(CIO.defer(bodyStarted.trySuccess(())).flatMap(_ => CIO.never))
    val scope            = tryWithLock(runner(released, renewals, false, releaseCompleted), 300.millis)(body)
    CIO
      .race(scope, CIO.fromScalaFuture(bodyStarted.future).map(_ => None))
      .unsafeRun
      .map { result =>
        assertEquals(result, None)
      }
      .flatMap(_ => cleanupCompleted.future)
      .map { _ =>
        assert(released.get())
        assert(bodyStopped.get())
      }
  }

  test("losing ownership cancels the protected body") {
    val bodyStarted      = new AtomicBoolean(false)
    val bodyStopped      = new AtomicBoolean(false)
    val cleanupCompleted = Promise[Unit]()
    val commands         = new SharedRunner {
      def run[A](command: Command[A]): CIO[A] =
        command
          .decode(Frame.Integer(if (command.args(4).asUtf8String == "renew" && bodyStarted.get()) 0 else 1))
          .fold(CIO.fail(_), CIO.value(_))
    }
    val body             = CIO.ensure(CIO.defer {
      bodyStopped.set(true)
      val _ = cleanupCompleted.trySuccess(())
      ()
    })(CIO.defer(bodyStarted.set(true)).flatMap(_ => CIO.never))
    tryWithLock(commands, 300.millis)(body).liftToTry.unsafeRun
      .map(result => assert(result.failed.get.isInstanceOf[LockLost], result.toString))
      .flatMap(_ => cleanupCompleted.future)
      .map(_ => assert(bodyStopped.get()))
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
    val busy = new SharedRunner {
      def run[A](command: Command[A]): CIO[A] = command.decode(Frame.Integer(0)).fold(CIO.fail(_), CIO.value(_))
    }
    CIO.timeout(100.millis)(withLock(busy, 3.seconds, 30.seconds)(CIO.unit)).unsafeRun.map(result => assertEquals(result, None))
  }

  test("a key-type view keeps the runner of the client it wraps") {
    val client = new LockTestClient(SharedRunner.unavailable)
    assert(client.as[Array[Byte]].runner eq client)
  }
}

final class LockTestClient(commands: SharedRunner) extends Client[CIO, String] with SharedRunner {
  private def unsupported[A]: CIO[A] = CIO.fail(new AssertionError("unexpected client operation in a lock test"))

  def run[A](command: Command[A]): CIO[A]                                                                                        = commands.run(command)
  def cached[A](command: Command[A], ttl: FiniteDuration): CIO[A]                                                                = unsupported
  private[sage] def pipeline[R](p: Pipeline[R]): CIO[R]                                                                          = unsupported
  def transaction[A](body: TransactionScope[CIO, String] => CIO[A]): CIO[A]                                                      = unsupported
  def subscribeChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]]                       = unsupported
  def subscribePatterns[V: ValueCodec](pattern: String, rest: String*): CIO[Subscription[CIO, PatternMessage[V]]]                = unsupported
  def subscribeShardChannels[V: ValueCodec](channel: String, rest: String*): CIO[Subscription[CIO, Message[V]]]                  = unsupported
  private[sage] def rateLimitAcquire[RK](executor: RateLimitExecutor[RK], subject: RK, cost: Long, peek: Boolean): CIO[Decision] = unsupported
  private[sage] def lockTryWith[LK, A](executor: LockExecutor[LK], key: LK)(body: => CIO[A]): CIO[Option[A]]                     =
    executor.tryWithLock(this, key)(body)
  private[sage] def lockWith[LK, A](executor: LockExecutor[LK], key: LK, waitTimeout: FiniteDuration)(body: => CIO[A]): CIO[A]   =
    executor.withLock(this, key, waitTimeout)(body)
  override private[sage] def runner: SharedRunner                                                                                = this
  def close: CIO[Unit]                                                                                                           = CIO.unit
}

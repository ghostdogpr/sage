package sage.client.internal

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.*

import kyo.compat.*

import sage.SageException.LockLost
import sage.backend.SageClient
import sage.commands.Command
import sage.protocol.Frame

class LockFutureSpec extends munit.FunSuite {
  private given ExecutionContext = munitExecutionContext

  test("lease loss stops renewal while a Future body keeps running") {
    val body      = Promise[Int]()
    val continued = Promise[Int]()
    val renewals  = new AtomicInteger(0)
    val released  = new AtomicBoolean(false)
    val commands  = new CommandRunner[CIO, String] {
      def run[A](command: Command[A]): CIO[A] = CIO.defer(()).flatMap { _ =>
        val reply = command.args(4).asUtf8String match {
          case "renew"   =>
            renewals.incrementAndGet()
            0L
          case "release" =>
            released.set(true)
            1L
          case _         => 1L
        }
        command.decode(Frame.Integer(reply)).fold(CIO.fail(_), CIO.value(_))
      }
    }
    val client    = new SageClient.Lowered(new LockTestClient(commands))
    val scoped    = client.lock[String](300.millis).tryWithLock("key") {
      body.future.map { value =>
        continued.success(value)
        value
      }
    }
    for {
      error <- scoped.failed
      _      = assert(error.isInstanceOf[LockLost], error.toString)
      _      = assert(!continued.isCompleted)
      _      = assert(released.get())
      count  = renewals.get()
      _     <- CIO.sleep(400.millis).unsafeRun
      _      = assertEquals(renewals.get(), count)
      _      = body.success(42)
      value <- continued.future
      _      = assertEquals(value, 42)
      _     <- CIO.sleep(400.millis).unsafeRun
    } yield assertEquals(renewals.get(), count)
  }
}

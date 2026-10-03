package sage.integration

import scala.concurrent.duration.*
import scala.util.Failure

import kyo.compat.*

/**
  * Polling for state a server reaches on its own schedule. Each attempt runs the same `CIO` value again.
  */
object Eventually {

  /**
    * Runs `check` up to `attempts` times, `interval` apart, until no assertion in it fails, and returns its result. Any other failure, such as a
    * command error, fails at once.
    */
  def apply[A](attempts: Int, interval: FiniteDuration = 100.millis)(check: CIO[A]): CIO[A] =
    retry(attempts, interval)(check) {
      case _: AssertionError => true
      case _                 => false
    }

  /**
    * Runs `action` up to `attempts` times, `interval` apart, until it succeeds, and returns its result. Otherwise fails with the last failure.
    */
  def succeeds[A](attempts: Int, interval: FiniteDuration = 100.millis)(action: CIO[A]): CIO[A] =
    retry(attempts, interval)(action)(_ => true)

  private def retry[A](attempts: Int, interval: FiniteDuration)(action: CIO[A])(retries: Throwable => Boolean): CIO[A] =
    action.liftToTry.flatMap {
      case Failure(error) if attempts > 1 && retries(error) => CIO.sleep(interval).flatMap(_ => retry(attempts - 1, interval)(action)(retries))
      case result                                           => CIO.get(result)
    }
}

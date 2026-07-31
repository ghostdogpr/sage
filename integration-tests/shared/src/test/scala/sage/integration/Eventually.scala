package sage.integration

import scala.concurrent.duration.*

import kyo.compat.*

/**
  * Polling for state a server reaches on its own schedule. The action is a thunk, not by-name: a by-name `CIO` erases to the same shape the
  * Future and Ox cells give the effect itself, and casts at run time.
  */
object Eventually {

  /**
    * Re-runs `action` until `holds` accepts its result, yielding whatever was seen last once the attempts run out.
    */
  def value[A](attempts: Int, interval: FiniteDuration = 100.millis)(action: () => CIO[A])(holds: A => Boolean): CIO[A] =
    action().flatMap { seen =>
      if (holds(seen) || attempts <= 1) CIO.value(seen)
      else CIO.sleep(interval).flatMap(_ => value(attempts - 1, interval)(action)(holds))
    }

  /**
    * Like [[value]], but fails with `orFail`'s message when the state never arrives.
    */
  def converges[A](attempts: Int, interval: FiniteDuration = 100.millis)(action: () => CIO[A])(holds: A => Boolean)(
    orFail: A => String
  ): CIO[Unit] =
    value(attempts, interval)(action)(holds).flatMap { seen =>
      if (holds(seen)) CIO.value(()) else CIO.fail(new RuntimeException(orFail(seen)))
    }
}

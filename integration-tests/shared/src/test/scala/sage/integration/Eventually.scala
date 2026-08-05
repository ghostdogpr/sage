package sage.integration

import scala.concurrent.duration.*

import kyo.compat.*

/**
  * Polling for state a server reaches on its own schedule. The action is passed as a thunk. A by-name `CIO` parameter would erase to the same
  * JVM signature as the effect value used by the Future and Ox cells, leading to runtime casts.
  */
object Eventually {

  /**
    * Runs `action` up to `attempts` times, `interval` apart, yielding the first result `holds` accepts or the last one seen.
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

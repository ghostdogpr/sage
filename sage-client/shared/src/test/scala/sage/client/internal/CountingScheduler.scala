package sage.client.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Delegates to [[Scheduler.real]] while counting zero-delay offloads, so a test can assert a dispatch stayed inline or wait until the
  * offloaded work has finished.
  */
final class CountingScheduler extends Scheduler {

  val zeroDelays = new AtomicInteger(0)

  val unfinished = new AtomicInteger(0)

  def nowMillis: Long = Scheduler.real.nowMillis

  def jitterMillis(boundExclusive: Long): Long = Scheduler.real.jitterMillis(boundExclusive)

  def after(delay: FiniteDuration)(task: => Unit): Unit =
    if (delay <= Duration.Zero) {
      zeroDelays.incrementAndGet()
      unfinished.incrementAndGet()
      Scheduler.real.after(delay)(
        try task
        finally unfinished.decrementAndGet(): Unit
      )
    } else Scheduler.real.after(delay)(task)

  def every(interval: FiniteDuration)(task: => Unit): Scheduler.Cancelable = Scheduler.real.every(interval)(task)
}

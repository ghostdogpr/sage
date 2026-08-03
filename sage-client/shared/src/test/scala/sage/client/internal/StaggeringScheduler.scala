package sage.client.internal

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Delegates to [[Scheduler.real]], but holds the first zero-delay offload taken after `arm()` back by `delay`, so a path that offloads one
  * task per position cannot pass by having the earliest one win the race.
  */
final class StaggeringScheduler(delay: FiniteDuration) extends Scheduler {

  private val armed = new AtomicBoolean(false)

  def arm(): Unit = armed.set(true)

  def nowMillis: Long = Scheduler.real.nowMillis

  def jitterMillis(boundExclusive: Long): Long = Scheduler.real.jitterMillis(boundExclusive)

  def after(requested: FiniteDuration)(task: => Unit): Unit =
    if (requested <= Duration.Zero && armed.compareAndSet(true, false)) Scheduler.real.after(delay)(task)
    else Scheduler.real.after(requested)(task)

  def every(interval: FiniteDuration)(task: => Unit): Scheduler.Cancelable = Scheduler.real.every(interval)(task)
}

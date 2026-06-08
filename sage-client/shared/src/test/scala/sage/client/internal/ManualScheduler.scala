package sage.client.internal

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
  * A [[Scheduler]] over virtual time: nothing runs until `advance` moves the clock, then due tasks fire in chronological order on the
  * calling thread. Jitter is deterministic (top of the range) so backoff delays are exact.
  */
final class ManualScheduler extends Scheduler {

  private var clockMillis: Long = 0L
  private val oneShots          = mutable.ArrayBuffer.empty[(Long, () => Unit)]
  private val periodics         = mutable.ArrayBuffer.empty[ManualScheduler.Periodic]

  def nowMillis: Long = clockMillis

  def jitterMillis(boundExclusive: Long): Long = if (boundExclusive <= 0) 0L else boundExclusive - 1

  def after(delay: FiniteDuration)(task: => Unit): Unit = {
    val _ = oneShots += ((clockMillis + delay.toMillis, () => task))
  }

  def every(interval: FiniteDuration)(task: => Unit): Scheduler.Cancelable = {
    val periodic = new ManualScheduler.Periodic(interval.toMillis, clockMillis + interval.toMillis, () => task)
    periodics += periodic
    () => periodic.cancelled = true
  }

  def advance(delta: FiniteDuration): Unit = {
    val target  = clockMillis + delta.toMillis
    var running = true
    while (running) {
      val oneShot     = oneShots.zipWithIndex.filter { case ((due, _), _) => due <= target }.minByOption { case ((due, _), _) => due }
      val periodic    = periodics.filter(p => !p.cancelled && p.nextDue <= target).minByOption(_.nextDue)
      val oneShotDue  = oneShot.map { case ((due, _), _) => due }
      val periodicDue = periodic.map(_.nextDue)
      if (oneShot.isEmpty && periodic.isEmpty) running = false
      else if (periodicDue.isEmpty || (oneShotDue.nonEmpty && oneShotDue.get <= periodicDue.get)) {
        val ((due, task), index) = oneShot.get
        clockMillis = due
        oneShots.remove(index)
        task()
      } else {
        val p = periodic.get
        clockMillis = p.nextDue
        p.nextDue += p.intervalMillis
        p.task()
      }
    }
    clockMillis = target
  }
}

private object ManualScheduler {

  final class Periodic(val intervalMillis: Long, var nextDue: Long, val task: () => Unit) {
    var cancelled: Boolean = false
  }
}

package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.Duration

/**
  * Single-flight, throttled discovery: collapses concurrent refreshes onto one in-flight run (others block until it finishes) and skips a
  * run that lands within `minRefreshMs` of the last, unless `force`. Shared by the cluster and master-replica runtimes, which differ only in
  * what `work` does. `lastRefresh` starts a full window in the past, so the first triggered refresh always runs.
  */
final private[client] class RefreshThrottle(scheduler: Scheduler, minRefreshMs: Long) {

  private val lock          = new ReentrantLock()
  private val done          = lock.newCondition()
  private var refreshing    = false
  private var lastRefreshMs = scheduler.nowMillis - minRefreshMs

  def apply(force: Boolean)(work: => Unit): Unit =
    if (claim(force, wait = true)) run(work)

  /**
    * Schedules a non-forced refresh only when this call claims the throttle. Unlike [[apply]], callers never wait for an in-flight refresh.
    */
  def trigger(work: => Unit): Unit =
    if (claim(force = false, wait = false))
      try scheduler.after(Duration.Zero)(run(work))
      catch {
        case error: Throwable =>
          finish()
          throw error
      }

  private def claim(force: Boolean, wait: Boolean): Boolean = {
    lock.lock()
    try
      if (refreshing && wait) { while (refreshing) done.awaitUninterruptibly(); false }
      else if (refreshing || (!force && scheduler.nowMillis - lastRefreshMs < minRefreshMs)) false
      else { refreshing = true; true }
    finally lock.unlock()
  }

  private def run(work: => Unit): Unit =
    try work
    finally finish()

  private def finish(): Unit = {
    lock.lock()
    try { refreshing = false; lastRefreshMs = scheduler.nowMillis; done.signalAll() }
    finally lock.unlock()
  }
}

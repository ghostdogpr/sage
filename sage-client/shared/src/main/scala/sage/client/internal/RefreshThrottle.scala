package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Single-flight, throttled discovery: collapses concurrent refreshes onto one in-flight run (others block until it finishes) and skips a
  * run that lands within `minRefreshMs` of the last, unless `force`. Shared by the cluster and master-replica runtimes, which differ only in
  * what `work` does. `lastRefresh` starts a full window in the past, so the first triggered refresh always runs.
  *
  * It also owns the optional background poll ([[startPolling]]/[[stopPolling]]), so both runtimes get one lifecycle for it.
  */
final private[client] class RefreshThrottle(scheduler: Scheduler, minRefreshMs: Long) {

  private val lock          = new ReentrantLock()
  private val done          = lock.newCondition()
  private var refreshing    = false
  private var lastRefreshMs = scheduler.nowMillis - minRefreshMs
  private var ticker        = null: Scheduler.Cancelable
  private var stopped       = false

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

  /**
    * Starts the background poll when an interval is configured; `tick` runs on the timer thread, so it must only queue work.
    */
  def startPolling(interval: Option[FiniteDuration])(tick: => Unit): Unit =
    interval.foreach { period =>
      val handle = scheduler.every(period)(tick)
      lock.lock()
      val keep   =
        try
          if (stopped) false
          else {
            ticker = handle
            true
          }
        finally lock.unlock()
      if (!keep) handle.cancel()
    }

  def stopPolling(): Unit = {
    lock.lock()
    val handle =
      try {
        stopped = true
        val current = ticker
        ticker = null
        current
      } finally lock.unlock()
    if (handle != null) handle.cancel()
  }

  private def claim(force: Boolean, wait: Boolean): Boolean = {
    lock.lock()
    try
      if (refreshing && wait) {
        while (refreshing) done.awaitUninterruptibly()
        false
      } else if (refreshing || (!force && scheduler.nowMillis - lastRefreshMs < minRefreshMs)) false
      else {
        refreshing = true
        true
      }
    finally lock.unlock()
  }

  private def run(work: => Unit): Unit =
    try work
    finally finish()

  private def finish(): Unit = {
    lock.lock()
    try {
      refreshing = false
      lastRefreshMs = scheduler.nowMillis
      done.signalAll()
    } finally lock.unlock()
  }
}

package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Coordinates discovery refreshes for the cluster and master-replica runtimes. Only one refresh runs at a time. [[apply]] waits for a current
  * refresh to finish and then returns. Non-forced calls within `minRefreshMs` of the previous refresh are skipped. A forced call ignores this
  * minimum interval. The initial completion time allows the first refresh to run immediately.
  *
  * It also starts and stops the optional background polling task.
  */
final private[client] class RefreshThrottle(scheduler: Scheduler, minRefreshMs: Long) {

  private val lock                    = new ReentrantLock()
  private val done                    = lock.newCondition()
  // volatile so `throttled` can answer without the lock; every mutation still happens under it
  @volatile private var refreshing    = false
  @volatile private var lastRefreshMs = scheduler.nowMillis - minRefreshMs
  private var ticker                  = null: Scheduler.Cancelable
  private var stopped                 = false

  def apply(force: Boolean)(work: => Unit): Unit =
    if (claim(force, wait = true)) run(work)

  /**
    * Schedules a non-forced refresh when no refresh is active and the minimum interval has passed. It returns immediately while another
    * refresh is active or the interval has not passed. Routing may call this for every read when no replica is available. Taking a pre-created
    * callback avoids allocating a new closure for each call that returns without scheduling work.
    */
  def trigger(work: () => Unit): Unit =
    if (claim(force = false, wait = false))
      try scheduler.after(Duration.Zero)(run(work()))
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
    if (!force && !wait && throttled) return false
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

  private def throttled: Boolean = refreshing || scheduler.nowMillis - lastRefreshMs < minRefreshMs

  private def run(work: => Unit): Unit =
    try work
    finally finish()

  private def finish(): Unit = {
    lock.lock()
    try {
      // Record the completion time before publishing refreshing = false. Volatile write ordering ensures a lock-free throttled check that sees
      // false also sees the updated completion time.
      lastRefreshMs = scheduler.nowMillis
      refreshing = false
      done.signalAll()
    } finally lock.unlock()
  }
}

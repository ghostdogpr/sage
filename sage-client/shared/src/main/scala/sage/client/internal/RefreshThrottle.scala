package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

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

  def apply(force: Boolean)(work: => Unit): Unit = {
    lock.lock()
    val mine =
      try
        if (refreshing) { while (refreshing) done.awaitUninterruptibly(); false }
        else if (!force && scheduler.nowMillis - lastRefreshMs < minRefreshMs) false
        else { refreshing = true; true }
      finally lock.unlock()

    if (mine)
      try work
      finally {
        lock.lock()
        try { refreshing = false; lastRefreshMs = scheduler.nowMillis; done.signalAll() }
        finally lock.unlock()
      }
  }
}

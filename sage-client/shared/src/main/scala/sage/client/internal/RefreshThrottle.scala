package sage.client.internal

import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.*

/**
  * Coordinates discovery refreshes (`work`) for the cluster and master-replica runtimes. Only one refresh runs at a time. [[apply]] waits for a current
  * refresh to finish and then returns. A non-forced call to `apply` within `minRefreshMs` of the previous refresh is skipped.
  * `request` retains a refresh until it can run. A forced call ignores the minimum interval. The first refresh can run immediately.
  * After [[stop]], no refresh starts, including one that was already queued.
  */
final private[client] class RefreshThrottle(scheduler: Scheduler, minRefreshMs: Long, work: () => Unit) {
  import RefreshThrottle.Phase

  private val lock                   = new ReentrantLock()
  private val done                   = lock.newCondition()
  private var lastRefreshMs          = scheduler.nowMillis - minRefreshMs
  // volatile so a request already pending returns without the lock; every change still happens under it
  @volatile private var phase: Phase = Phase.Idle

  private inline def locked[A](inline body: A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  // wait for any current refresh to finish before callers read `topologyRef`
  def apply(force: Boolean): Unit =
    if (claim(force)) run()

  /**
    * Retains a refresh request until it can run. Requests during a refresh or its minimum interval coalesce into one later refresh. Routing
    * may call this for every read when no replica is available.
    */
  def request(): Unit =
    phase match {
      case Phase.Idle | Phase.Running(false) =>
        locked(phase match {
          case Phase.Idle           => schedule()
          case Phase.Running(false) => phase = Phase.Running(again = true)
          case _                    => ()
        })
      case _                                 => ()
    }

  // Must hold lock. A timer whose Scheduled phase was replaced does nothing when it fires.
  private def schedule(): Unit = {
    val mine        = Phase.Scheduled()
    phase = mine
    val delayMillis = math.max(0L, minRefreshMs - (scheduler.nowMillis - lastRefreshMs))
    onThrow(scheduler.after(delayMillis.millis)(runScheduled(mine)))(_ => phase = Phase.Idle)
  }

  private def runScheduled(mine: Phase): Unit =
    if (
      locked((phase eq mine) && {
        val due = scheduler.nowMillis - lastRefreshMs >= minRefreshMs
        if (due) phase = Phase.Running(again = false) else schedule()
        due
      })
    ) run()

  def stop(): Unit = locked { phase = Phase.Stopped }

  // a refresh claimed while one is scheduled keeps the scheduled one as a follow-up
  private def claim(force: Boolean): Boolean =
    locked(phase match {
      case Phase.Stopped    => false
      case Phase.Running(_) =>
        while (phase.isRunning) done.awaitUninterruptibly()
        false
      case waiting          =>
        val due = force || scheduler.nowMillis - lastRefreshMs >= minRefreshMs
        if (due) phase = Phase.Running(again = waiting != Phase.Idle)
        due
    })

  // a refresh queued before stop must not open new connections
  private def run(): Unit =
    try if (phase != Phase.Stopped) work()
    finally finish()

  private def finish(): Unit =
    locked {
      lastRefreshMs = scheduler.nowMillis
      done.signalAll()
      phase match {
        case Phase.Running(true)  => schedule()
        case Phase.Running(false) => phase = Phase.Idle
        case _                    => ()
      }
    }
}

private object RefreshThrottle {

  // Scheduled has an identity per timer; Running(again) records a request that arrived during the refresh
  enum Phase {
    case Idle, Stopped
    case Scheduled()
    case Running(again: Boolean)

    def isRunning: Boolean = this match {
      case Running(_) => true
      case _          => false
    }
  }
}

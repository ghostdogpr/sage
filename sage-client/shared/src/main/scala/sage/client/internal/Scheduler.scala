package sage.client.internal

import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, ThreadLocalRandom, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.duration.*
import scala.util.control.NonFatal

import sage.client.BackoffConfig

/**
  * The clock and timer abstraction under the reconnect loop and the watchdog, injected so tests drive virtual time. `nowMillis` is monotonic. A
  * one-shot `after` task may block (connect, bootstrap); a periodic `every` tick must not.
  */
private[client] trait Scheduler {

  def nowMillis: Long

  /**
    * A non-negative jitter sample in `[0, boundExclusive)`.
    */
  def jitterMillis(boundExclusive: Long): Long

  def after(delay: FiniteDuration)(task: => Unit): Unit

  def every(interval: FiniteDuration)(task: => Unit): Scheduler.Cancelable

  final def offload(task: => Unit): Unit = after(Duration.Zero)(task)

  // Reconnects and cluster-redirect retries share this formula: exponential backoff capped at maxDelay, then full jitter in [0, base].
  final def backoffMillis(config: BackoffConfig, attempt: Int): Long = {
    val capped = config.maxDelay.toMillis
    val raw    = config.initialDelay.toMillis.toDouble * math.pow(config.multiplier, attempt.toDouble)
    val base   = if (raw.isInfinite || raw >= capped.toDouble) capped else math.max(0L, raw.toLong)
    jitterMillis(base + 1)
  }

  final def afterBackoff(config: BackoffConfig, attempt: Int)(task: => Unit): Unit = after(backoffMillis(config, attempt).millis)(task)
}

/**
  * The reconnect loop of one connection or subscription manager, guarded by its owner's `lock`. Attempts keep counting across connections
  * that drop before staying live for `maxDelay`, so a connection the server keeps closing backs off instead of reconnecting at the initial
  * delay. At most one retry waits at a time; a loss while one waits joins it.
  */
final private[internal] class Reconnects(scheduler: Scheduler, config: BackoffConfig, lock: ReentrantLock) {
  private var attempt   = 0
  private var liveSince = -1L
  private var waiting   = false

  def live(): Unit = liveSince = scheduler.nowMillis

  // Must hold lock. After the backoff, runs `retry` if `wanted` still holds; a failed retry is reported and scheduled again while it holds.
  // With `immediately`, the first attempt runs without waiting.
  def schedule(wanted: => Boolean, onFailure: Throwable => Unit, immediately: Boolean = false)(retry: => Unit): Unit =
    if (!waiting) {
      if (liveSince >= 0L && scheduler.nowMillis - liveSince >= config.maxDelay.toMillis) attempt = 0
      val delay = if (immediately && attempt == 0) 0L else scheduler.backoffMillis(config, attempt)
      attempt += 1
      liveSince = -1L
      waiting = true
      scheduler.after(delay.millis) {
        if (holding { waiting = false; wanted })
          try retry
          catch { case NonFatal(error) => holding(if (wanted) { onFailure(error); schedule(wanted, onFailure)(retry) }) }
      }
    }

  private def holding[A](body: => A): A = {
    lock.lock()
    try body
    finally lock.unlock()
  }
}

private[client] object Scheduler {

  trait Cancelable {
    def cancel(): Unit
  }

  /**
    * One shared daemon thread handles timing. Each one-shot body runs on a virtual thread, preventing a blocking reconnect from delaying
    * other timers.
    */
  val real: Scheduler = new Scheduler {

    private val timer: ScheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor { runnable =>
        val thread = new Thread(runnable, "sage-scheduler")
        thread.setDaemon(true)
        thread
      }

    def nowMillis: Long = System.nanoTime() / 1000000L

    def jitterMillis(boundExclusive: Long): Long =
      if (boundExclusive <= 0) 0L else ThreadLocalRandom.current().nextLong(boundExclusive)

    def after(delay: FiniteDuration)(task: => Unit): Unit =
      // zero-delay offloads must not queue behind watchdog/reconnect timing on the timer thread
      if (delay <= Duration.Zero) { Thread.ofVirtual().name("sage-offload").start(() => task): Unit }
      else {
        val fire: Runnable = () => Thread.ofVirtual().name("sage-reconnect").start(() => task): Unit
        timer.schedule(fire, delay.toMillis, TimeUnit.MILLISECONDS): Unit
      }

    def every(interval: FiniteDuration)(task: => Unit): Cancelable = {
      // a throwing task cancels a scheduleAtFixedRate future forever, so swallow it to keep the periodic alive
      val guarded: Runnable          = () =>
        try task
        catch { case NonFatal(_) => () }
      val future: ScheduledFuture[?] = timer.scheduleAtFixedRate(guarded, interval.toMillis, interval.toMillis, TimeUnit.MILLISECONDS)
      () => future.cancel(false): Unit
    }
  }
}

package sage.client.internal

import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, ThreadLocalRandom, TimeUnit}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/**
  * The clock and timer seam under the reconnect loop and the watchdog, injected so tests drive virtual time. `nowMillis` is monotonic. A
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
}

private[client] object Scheduler {

  trait Cancelable {
    def cancel(): Unit
  }

  /**
    * Timing on one shared daemon thread; each one-shot body runs on its own virtual thread so a blocking reconnect never stalls the timer.
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

    def after(delay: FiniteDuration)(task: => Unit): Unit = {
      val _ = timer.schedule(
        (() => { val _ = Thread.ofVirtual().name("sage-reconnect").start(() => task) }): Runnable,
        delay.toMillis,
        TimeUnit.MILLISECONDS
      )
    }

    def every(interval: FiniteDuration)(task: => Unit): Cancelable = {
      // a throwing task cancels a scheduleAtFixedRate future forever, so swallow it to keep the periodic alive
      val guarded: Runnable          = () =>
        try task
        catch { case NonFatal(_) => () }
      val future: ScheduledFuture[?] = timer.scheduleAtFixedRate(guarded, interval.toMillis, interval.toMillis, TimeUnit.MILLISECONDS)
      () => { val _ = future.cancel(false) }
    }
  }
}

package sage.testkit

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration.FiniteDuration

/**
  * A wall clock for the in-memory client, used for key expiry and for `TIME`. Without one, the client reads the backend's own clock: ZIO's
  * `Clock`, Cats Effect's `IO.realTime`, Kyo's `Clock`, or the system clock for Ox, Pekko and the direct backend. That lets the test
  * clocks of ZIO, Cats Effect and Kyo control expiry. A `TestClock` starts at `start` and moves only when the test calls [[advance]] or
  * [[set]], which gives the same control to backends without a test clock. It is safe to read and change from several threads.
  */
final class TestClock(start: Instant = Instant.parse("2026-01-01T00:00:00Z")) {

  private val current = new AtomicLong(start.toEpochMilli)

  /**
    * The current time in milliseconds since the Unix epoch.
    */
  def nowMillis: Long = current.get()

  /**
    * Moves the clock forward by `duration`, rounded down to whole milliseconds. Keys whose expiry time has been reached are treated as
    * missing by the next command that reads them.
    */
  def advance(duration: FiniteDuration): Unit = current.addAndGet(duration.toMillis): Unit

  /**
    * Moves the clock to `instant`, which may be earlier than the current time.
    */
  def set(instant: Instant): Unit = current.set(instant.toEpochMilli)
}

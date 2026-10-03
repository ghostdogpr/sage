package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.*

class RefreshThrottleSpec extends munit.FunSuite {

  test("retained requests coalesce and run after the minimum interval without another failure") {
    val scheduler = new ManualScheduler
    var runs      = 0
    val throttle  = new RefreshThrottle(scheduler, minRefreshMs = 1000L, () => runs += 1)
    throttle.request()
    throttle.request()
    scheduler.advance(Duration.Zero)
    assertEquals(runs, 1, "a second request duplicated an immediate refresh")
    (1 to 100).foreach(_ => throttle.request())
    scheduler.advance(999.millis)
    assertEquals(runs, 1, "a request inside the window ran before it closed")
    scheduler.advance(1.milli)
    assertEquals(runs, 2)
    scheduler.advance(2.seconds)
    assertEquals(runs, 2)
  }

  test("a retained request during refresh runs after completion and respects a later forced refresh") {
    val scheduler                 = new ManualScheduler
    var runs                      = 0
    var throttle: RefreshThrottle = null
    throttle = new RefreshThrottle(
      scheduler,
      minRefreshMs = 1000L,
      () => {
        runs += 1
        if (runs == 1) throttle.request()
      }
    )
    throttle.request()
    scheduler.advance(Duration.Zero)
    scheduler.advance(500.millis)
    throttle(force = true)
    scheduler.advance(999.millis)
    assertEquals(runs, 2)
    scheduler.advance(1.milli)
    assertEquals(runs, 3)
  }

  test("closing discards a retained refresh") {
    val scheduler = new ManualScheduler
    var runs      = 0
    val throttle  = new RefreshThrottle(scheduler, minRefreshMs = 1000L, () => runs += 1)
    throttle(force = true)
    throttle.request()
    throttle.stop()
    scheduler.advance(2.seconds)
    assertEquals(runs, 1, "refresh ran after close")
  }

  test("requests during an in-flight refresh coalesce into one follow-up refresh, and a blocking caller waits") {
    val scheduler = new CountingScheduler
    val started   = new CountDownLatch(1)
    val release   = new CountDownLatch(1)
    val finished  = new CountDownLatch(1)
    val runs      = new AtomicInteger(0)
    val throttle  = new RefreshThrottle(
      scheduler,
      minRefreshMs = 0L,
      () =>
        if (runs.incrementAndGet() == 1) {
          started.countDown()
          release.await()
          finished.countDown()
        }
    )

    throttle.request()
    assert(started.await(2, TimeUnit.SECONDS), "the requested refresh did not start")

    var i = 0
    while (i < 1000) {
      throttle.request()
      i += 1
    }
    assertEquals(scheduler.zeroDelays.get(), 1, "an in-flight refresh should own the only scheduler offload")

    val blockingStarted = new CountDownLatch(1)
    val blockingDone    = new CountDownLatch(1)
    val waiter          = Thread.ofVirtual().start { () =>
      blockingStarted.countDown()
      throttle(force = false)
      blockingDone.countDown()
    }
    assert(blockingStarted.await(2, TimeUnit.SECONDS), "the blocking caller did not start")
    assert(!blockingDone.await(100, TimeUnit.MILLISECONDS), "the existing blocking entry point must still wait for the in-flight refresh")

    release.countDown()
    assert(finished.await(2, TimeUnit.SECONDS), "the requested refresh did not finish")
    assert(blockingDone.await(2, TimeUnit.SECONDS), "the blocking caller did not resume")
    waiter.join()
    while (runs.get() < 2) Thread.onSpinWait()
    throttle.stop()
    assertEquals(runs.get(), 2, "the requests made during the refresh did not run exactly one follow-up")
  }

  test("a request inside the refresh window runs once the window closes") {
    val scheduler = new ManualScheduler
    val runs      = new AtomicInteger(0)
    val throttle  = new RefreshThrottle(scheduler, minRefreshMs = 1000L, () => runs.incrementAndGet(): Unit)

    throttle.request()
    scheduler.advance(Duration.Zero)
    assertEquals(runs.get(), 1, "the first request did not run")

    throttle.request()
    scheduler.advance(500.millis)
    assertEquals(runs.get(), 1, "a request inside the refresh window ran before it closed")

    scheduler.advance(500.millis)
    assertEquals(runs.get(), 2, "the retained request did not run when the window closed")
  }

  test("requests inside the refresh window schedule no immediate offload") {
    val scheduler = new CountingScheduler
    val ran       = new CountDownLatch(1)
    val throttle  = new RefreshThrottle(scheduler, minRefreshMs = 60000L, () => ran.countDown())

    throttle.request()
    assert(ran.await(2, TimeUnit.SECONDS), "the first request did not run")

    val offloads = scheduler.zeroDelays.get()
    var i        = 0
    while (i < 1000) {
      throttle.request()
      i += 1
    }
    assertEquals(scheduler.zeroDelays.get(), offloads, "a throttled request offloaded")
    throttle.stop()
  }
}

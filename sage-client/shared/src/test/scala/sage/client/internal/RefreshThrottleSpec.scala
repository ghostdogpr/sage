package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.*

class RefreshThrottleSpec extends munit.FunSuite {

  test("a non-blocking trigger schedules once while a refresh is in flight without changing blocking callers") {
    val scheduler = new CountingScheduler
    val throttle  = new RefreshThrottle(scheduler, minRefreshMs = 0L)
    val started   = new CountDownLatch(1)
    val release   = new CountDownLatch(1)
    val finished  = new CountDownLatch(1)

    throttle.trigger { () =>
      started.countDown()
      release.await()
      finished.countDown()
    }
    assert(started.await(2, TimeUnit.SECONDS), "the triggered refresh did not start")

    var i = 0
    while (i < 1000) {
      throttle.trigger(() => fail("an in-flight trigger must not run later work"))
      i += 1
    }
    assertEquals(scheduler.zeroDelays.get(), 1, "an in-flight refresh should own the only scheduler offload")

    val blockingStarted = new CountDownLatch(1)
    val blockingDone    = new CountDownLatch(1)
    val waiter          = Thread.ofVirtual().start { () =>
      blockingStarted.countDown()
      throttle(force = false)(())
      blockingDone.countDown()
    }
    assert(blockingStarted.await(2, TimeUnit.SECONDS), "the blocking caller did not start")
    assert(!blockingDone.await(100, TimeUnit.MILLISECONDS), "the existing blocking entry point must still wait for the in-flight refresh")

    release.countDown()
    assert(finished.await(2, TimeUnit.SECONDS), "the triggered refresh did not finish")
    assert(blockingDone.await(2, TimeUnit.SECONDS), "the blocking caller did not resume")
    waiter.join()
  }

  test("the window closes as soon as a refresh finishes and reopens once it elapses") {
    val scheduler = new ManualScheduler
    val throttle  = new RefreshThrottle(scheduler, minRefreshMs = 1000L)
    val runs      = new AtomicInteger(0)
    val work      = () => runs.incrementAndGet(): Unit

    throttle.trigger(work)
    scheduler.advance(Duration.Zero)
    assertEquals(runs.get(), 1, "the first trigger did not run")

    throttle.trigger(work)
    scheduler.advance(500.millis)
    assertEquals(runs.get(), 1, "a trigger inside the refresh window ran its work")

    scheduler.advance(600.millis)
    throttle.trigger(work)
    scheduler.advance(Duration.Zero)
    assertEquals(runs.get(), 2, "the window did not reopen once minRefreshInterval elapsed")
  }

  test("a trigger inside the refresh window offloads nothing") {
    val scheduler = new CountingScheduler
    val throttle  = new RefreshThrottle(scheduler, minRefreshMs = 60000L)
    val ran       = new CountDownLatch(1)

    throttle.trigger(() => ran.countDown())
    assert(ran.await(2, TimeUnit.SECONDS), "the first trigger did not run")

    val offloads = scheduler.zeroDelays.get()
    var i        = 0
    while (i < 1000) {
      throttle.trigger(() => fail("a throttled trigger must not run work"))
      i += 1
    }
    assertEquals(scheduler.zeroDelays.get(), offloads, "a throttled trigger offloaded")
  }
}

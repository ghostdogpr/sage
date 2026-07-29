package sage.client.internal

import java.util.concurrent.{CountDownLatch, TimeUnit}

class RefreshThrottleSpec extends munit.FunSuite {

  test("a non-blocking trigger schedules once while a refresh is in flight without changing blocking callers") {
    val scheduler = new CountingScheduler
    val throttle  = new RefreshThrottle(scheduler, minRefreshMs = 0L)
    val started   = new CountDownLatch(1)
    val release   = new CountDownLatch(1)
    val finished  = new CountDownLatch(1)

    throttle.trigger {
      started.countDown()
      release.await()
      finished.countDown()
    }
    assert(started.await(2, TimeUnit.SECONDS), "the triggered refresh did not start")

    var i = 0
    while (i < 1000) {
      throttle.trigger(fail("an in-flight trigger must not evaluate later work"))
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
}

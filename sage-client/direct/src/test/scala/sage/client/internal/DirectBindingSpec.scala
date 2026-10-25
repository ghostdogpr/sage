package sage.client.internal

import java.util.concurrent.{CompletionException, TimeoutException}

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.{Failure, Try}

import kyo.compat.*

class DirectBindingSpec extends munit.FunSuite {
  override val munitTimeout = 30.seconds

  test("unsafeRun completes its future when the computation throws a fatal throwable") {
    val future = CIO.defer[Unit](throw new InterruptedException("fatal")).unsafeRun
    assert(Try(Await.ready(future, 2.seconds)).isSuccess, "the future should complete")
    assert(future.value.exists(_.isFailure), String.valueOf(future.value))
  }

  test("timeout reports a computation's own TimeoutException as a failure") {
    val failure = new TimeoutException("from the computation")
    assertEquals(CIO.timeout(5.seconds)(CIO.fail(failure)).liftToTry.lower, Failure(failure))
  }

  test("the first failure in a concurrent collection interrupts every element that started") {
    val boom    = new RuntimeException("boom")
    val outcome = (1 to 200).iterator
      .map { _ =>
        CIO.timeout(2.seconds)(CIO.foreach(0 until 50)(i => if (i == 0) CIO.fail(boom) else CIO.never).liftToTry).lower
      }
      .find(_ != Some(Failure(boom)))
    assertEquals(outcome, None)
  }

  test("a recursive flatMap loop of a million steps completes without overflowing the stack") {
    def loop(n: Int, sum: Long): CIO[Long] =
      if (n == 0) CIO.value(sum) else CIO.defer(n).flatMap(i => loop(n - 1, sum + i))
    assertEquals(loop(1000000, 0L).lower, 500000500000L)
  }

  test("a failure after a long flatMap chain has moved to a new thread reaches the caller unchanged") {
    val failure                 = new CompletionException(new RuntimeException("cause"))
    def loop(n: Int): CIO[Unit] = if (n == 0) CIO.fail(failure) else CIO.unit.flatMap(_ => loop(n - 1))
    assertEquals(loop(1000).liftToTry.lower, Failure(failure))
  }

  test("a promise and an async callback rethrow the exact failure they were completed with") {
    val failure = new CompletionException(new RuntimeException("cause"))
    val promise = CPromise.init[Unit].lower
    assert(promise.fail(failure).lower)
    assertEquals(promise.get.liftToTry.lower, Failure(failure))
    assertEquals(promise.poll.lower, Some(Failure(failure)))
    assertEquals(CIO.async[Unit](_(Failure(failure))).liftToTry.lower, Failure(failure))
  }

  test("fromCompletionStage rethrows a failure thrown while creating the stage unchanged") {
    val failure = new CompletionException(new RuntimeException("cause"))
    assertEquals(CIO.fromCompletionStage[Unit](throw failure).liftToTry.lower, Failure(failure))
  }

  test("a recursive loop through every combinator that runs a nested CIO on the same thread completes without overflowing the stack") {
    val local                  = CLocal.init(0).lower
    val meter                  = CMeter.init(Int.MaxValue).lower
    def loop(n: Int): CIO[Int] =
      if (n == 0) local.get
      else CIO.ensure(CIO.unit)(local.let(n)(meter.run(CIO.unit.flatMap(_ => loop(n - 1))))).recover(CIO.fail)
    assertEquals(loop(100000).lower, 1)
  }

  test("race runs its first computation on the calling thread at every depth around the flatMap limit") {
    val context                             = new ThreadLocal[String]
    context.set("request-42")
    // the first computation is itself a chain of 100 steps, as a lock body wrapped by the lock executor would be
    val first                               = (1 to 100).foldLeft(CIO.defer((Thread.currentThread(), context.get())))((acc, _) => CIO.unit.flatMap(_ => acc))
    def loop(n: Int): CIO[(Thread, String)] = if (n == 0) CIO.race(first, CIO.never) else CIO.unit.flatMap(_ => loop(n - 1))
    try
      for (n <- List(0, 1, 155, 156, 157, 254, 255, 256, 257, 511, 512, 513, 1000)) {
        val (thread, value) = loop(n).lower
        assert(thread eq Thread.currentThread(), s"at depth $n the first computation ran on $thread")
        assertEquals(value, "request-42", s"at depth $n")
        assert(!Thread.interrupted(), s"at depth $n")
      }
    finally context.remove()
  }

  test("nested races inside a moved chain all run on the calling thread") {
    val caller                     = Thread.currentThread()
    def deep[A](c: CIO[A]): CIO[A] = (1 to 1000).foldLeft(c)((acc, _) => CIO.unit.flatMap(_ => acc))
    val inner                      = deep(CIO.race(CIO.defer(Thread.currentThread()), CIO.never))
    val outer                      = deep(CIO.race(inner.map(t => (t, Thread.currentThread())), CIO.never))
    val (innerThread, outerThread) = outer.lower
    assert(innerThread eq caller)
    assert(outerThread eq caller)
  }

}

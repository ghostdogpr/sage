package kyo.compat

import java.util.concurrent.{CompletableFuture, CompletionException, CompletionStage}

import scala.concurrent.{Await, Future as ScalaFuture}
import scala.concurrent.duration.{Duration as ScalaDuration, FiniteDuration, NANOSECONDS}
import scala.jdk.FutureConverters.*
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
  * Uses a function from the current `flatMap` nesting depth to the result. `lift` wraps ordinary blocking code, and `lower` calls the
  * function at depth 0 on the calling thread. Errors are thrown exceptions and cancellation is thread interruption. `flatMap` passes the
  * depth on and calls its functions directly. Once the depth reaches 256, the rest of the chain runs on a fresh virtual thread that the
  * caller waits for, so long chains and recursive loops do not overflow the stack. `race` still runs its first argument on the caller. `async` blocks the calling thread until the callback
  * fires, which is cheap on a virtual thread. Concurrent operators such as `race`, `timeout`, `zip`, and `foreach` run work on virtual
  * threads and wait for every thread they start before returning.
  */
opaque type CIO[+A] = Int => A

object CIO {

  // The maximum `flatMap` nesting on one thread's stack. Deeper chains continue on a fresh virtual thread.
  private inline val Limit = 256

  /**
    * Wraps blocking code as a `CIO`. The code runs each time the `CIO` runs.
    */
  def lift[A](f: => A): CIO[A] = _ => f

  /**
    * Returns a successful `CIO` with the given value.
    */
  def value[A](a: A): CIO[A] = _ => a

  /**
    * Returns a successful `CIO[Unit]`.
    */
  def unit: CIO[Unit] = _ => ()

  /**
    * Suspends a side-effecting thunk until the `CIO` runs.
    */
  def defer[A](thunk: => A): CIO[A] = _ => thunk

  /**
    * Returns a `CIO` that fails with `e`.
    */
  def fail(e: Throwable): CIO[Nothing] = _ => throw e

  /**
    * Succeeds with the value of a `Success` and fails with the throwable of a `Failure`.
    */
  def get[A](t: Try[A]): CIO[A] = _ => t.get

  /**
    * Waits for a Scala `Future` by blocking the calling thread.
    */
  def fromScalaFuture[A](f: => ScalaFuture[A]): CIO[A] = _ => Await.result(f, ScalaDuration.Inf)

  /**
    * Waits for a `CompletionStage` by blocking the calling thread. A failure is rethrown without the `CompletionException` that dependent
    * stages wrap it in. Interrupting the caller stops the wait but does not cancel the stage.
    */
  def fromCompletionStage[A](cs: => CompletionStage[A]): CIO[A] =
    _ => {
      val future = cs.toCompletableFuture
      try DirectRuntime.await(future)
      catch { case e: CompletionException if e.getCause ne null => throw e.getCause }
    }

  /**
    * Runs `use` with the acquired value and runs `release` on every exit path, including interruption. A release failure is added as a
    * suppressed exception when `use` also fails.
    */
  def acquireReleaseWith[A, B](
    acquire: CIO[A]
  )(
    release: A => CIO[Unit]
  )(
    use: A => CIO[B]
  ): CIO[B] =
    depth => {
      val resource                = acquire(depth)
      // Release runs on its own thread and the caller waits for it without reacting to interrupts, so a pending interrupt cannot cut it
      // short. The caller's interrupt status is restored afterward.
      def releaseResource(): Unit = DirectRuntime.uninterruptible(release(resource).lower)
      val result                  =
        try use(resource)(depth)
        catch {
          case useFailure: Throwable =>
            try releaseResource()
            catch { case releaseFailure: Throwable if releaseFailure ne useFailure => useFailure.addSuppressed(releaseFailure) }
            throw useFailure
        }
      releaseResource()
      result
    }

  // Runs `c` inside `wrap` on the calling thread at the depth of the enclosing `CIO`, so a recursion through `wrap` still reaches the
  // `flatMap` limit. Calling `c.lower` there would restart the depth at 0 on every level and overflow the stack.
  private[compat] def around[A, B](c: => CIO[A])(wrap: (() => A) => B): CIO[B] = depth => wrap(() => c(depth))

  /**
    * Runs `c` and then `cleanup` on every exit path.
    */
  def ensure[A](
    cleanup: CIO[Unit]
  )(
    c: CIO[A]
  ): CIO[A] =
    acquireReleaseWith(CIO.unit)(_ => cleanup)(_ => c)

  /**
    * Returns a `CIO` that blocks until the thread is interrupted.
    */
  val never: CIO[Nothing] = CIO.async[Nothing](_ => ())

  extension [A](self: CIO[A]) {

    /**
      * Runs this `CIO` on the calling thread and returns its result.
      */
    def lower: A = self(0)

    /**
      * Captures a non-fatal failure as `Failure`. Interruption and other fatal throwables are not captured.
      */
    def liftToTry: CIO[Try[A]] =
      depth =>
        try Success(self(depth))
        catch { case NonFatal(t) => Failure(t) }

    /**
      * Runs `handler` on failure to produce a recovery `CIO`.
      */
    def recover[A2 >: A](handler: Throwable => CIO[A2]): CIO[A2] = self.fold(CIO.value(_), handler)

    /**
      * Handles success and failure with the matching function.
      */
    def fold[B](
      onSuccess: A => CIO[B],
      onFail: Throwable => CIO[B]
    ): CIO[B] =
      self.liftToTry.flatMap {
        case Success(a) => onSuccess(a)
        case Failure(t) => onFail(t)
      }

    /**
      * Discards the success value. A failure still propagates.
      */
    def unit: CIO[Unit] = self.map(_ => ())

    /**
      * Runs `that` if this `CIO` fails.
      */
    def orElse[A2 >: A](that: CIO[A2]): CIO[A2] = self.recover(_ => that)

    /**
      * Transforms a failure with `f`.
      */
    def mapError(f: Throwable => Throwable): CIO[A] = self.recover(t => CIO.fail(f(t)))

    /**
      * Transforms the success value with a pure function.
      */
    def map[B](f: A => B): CIO[B] = self.flatMap(a => CIO.value(f(a)))

    /**
      * Continues with a `CIO` built from the success value. When the depth reaches the limit, this step and the rest of the chain run at
      * depth 0 on a new virtual thread. Its failures reach the caller unchanged, it inherits the caller's `CLocal` values, and interrupting
      * the caller interrupts the thread and waits for it to exit before the interrupt is rethrown. While it waits, the caller runs the
      * first argument of each `race` in the chain.
      */
    def flatMap[B](f: A => CIO[B]): CIO[B] =
      depth =>
        if (depth < Limit) f(self(depth + 1))(depth + 1)
        else DirectRuntime.bounce(() => f(self.lower).lower)

    /**
      * Runs this `CIO` on a new virtual thread and returns a `Future` with its outcome.
      */
    def unsafeRun: ScalaFuture[A] = DirectRuntime.Fiber.start(() => self.lower).result.asScala

  }

  /**
    * Blocks the calling thread for `d`.
    */
  def sleep(d: FiniteDuration): CIO[Unit] = _ => if (d.toNanos > 0) Thread.sleep(java.time.Duration.ofNanos(d.toNanos))

  /**
    * Returns the wall-clock time.
    */
  def now: CIO[java.time.Instant] = _ => java.time.Instant.now()

  /**
    * Returns a monotonic timestamp. Use it to measure elapsed time between two points in the program.
    */
  def nowMonotonic: CIO[FiniteDuration] = _ => FiniteDuration(System.nanoTime(), NANOSECONDS)

  /**
    * Runs `c` with a deadline and returns `None` if `d` elapses first. The computation runs on a virtual thread that is interrupted at the
    * deadline or when the caller is interrupted, and this `CIO` returns only after that thread has exited.
    */
  def timeout[A](d: FiniteDuration)(c: CIO[A]): CIO[Option[A]] =
    _ => DirectRuntime.Fiber.scoped(() => c.lower)(_.await(d))

  /**
    * Runs `c` with a deadline and fails with `e` if `d` elapses first.
    */
  def timeoutWithError[A](d: FiniteDuration)(e: Throwable)(c: CIO[A]): CIO[A] =
    _ => DirectRuntime.Fiber.scoped(() => c.lower)(_.await(d)).getOrElse(throw e)

  /**
    * Sleeps for `d` and then runs `c`.
    */
  def delay[A](d: FiniteDuration)(c: CIO[A]): CIO[A] =
    CIO.sleep(d).flatMap(_ => c)

  /**
    * Runs `a` and `b` concurrently and returns the first successful result. `a` runs on the thread that called `lower`, even when a long
    * `flatMap` chain has moved to another thread before the race, and `b` runs on a virtual thread. The losing side is interrupted.
    */
  def race[A](
    a: CIO[A],
    b: CIO[A]
  ): CIO[A] =
    // `a` starts at depth 0 so that its first steps, such as a lock body, run before its chain can move to another thread. Each nested
    // race can therefore add up to the depth limit to the stack of the thread that runs it.
    _ => DirectRuntime.onHome(() => DirectRuntime.race(() => a.lower, () => b.lower))

  /**
    * Maps the elements concurrently, with at most `concurrency` computations running at once.
    */
  def foreach[A, B](
    coll: Iterable[A],
    concurrency: Int = Int.MaxValue
  )(f: A => CIO[B]): CIO[CChunk[B]] =
    CIO.defer(CChunk.lift(DirectRuntime.parallel(coll, concurrency)(a => f(a).lower)))

  /**
    * Like [[foreach]], and passes each element's index to `f`.
    */
  def foreachIndexed[A, B](
    coll: Iterable[A],
    concurrency: Int = Int.MaxValue
  )(f: (Int, A) => CIO[B]): CIO[CChunk[B]] =
    foreach(coll.view.zipWithIndex, concurrency) { case (a, i) => f(i, a) }

  /**
    * Runs `f` for each element concurrently and discards the results.
    */
  def foreachDiscard[A](
    coll: Iterable[A],
    concurrency: Int = Int.MaxValue
  )(f: A => CIO[Any]): CIO[Unit] =
    foreach(coll, concurrency)(f).unit

  /**
    * Keeps the elements that satisfy `p`, evaluating the predicate concurrently.
    */
  def filter[A](
    coll: Iterable[A],
    concurrency: Int = Int.MaxValue
  )(p: A => CIO[Boolean]): CIO[CChunk[A]] =
    foreach(coll, concurrency)(a => p(a).map(Option.when(_)(a))).map(kept => CChunk.lift(kept.lower.flatten))

  /**
    * Runs the computations concurrently and returns their results in order.
    */
  def collectAll[A](
    coll: Iterable[CIO[A]],
    concurrency: Int = Int.MaxValue
  ): CIO[CChunk[A]] =
    foreach(coll, concurrency)(c => c)

  /**
    * Runs the computations concurrently and discards the results.
    */
  def collectAllDiscard(
    coll: Iterable[CIO[Any]],
    concurrency: Int = Int.MaxValue
  ): CIO[Unit] =
    foreachDiscard(coll, concurrency)(c => c)

  /**
    * Bridges a one-shot completion callback. `register` receives the callback, and the calling thread blocks until it fires. Interrupting the
    * caller stops the wait. A callback that fires afterward is ignored.
    */
  def async[A](register: ((Try[A] => Unit) => Unit)): CIO[A] =
    _ => {
      val outcome = new CompletableFuture[Try[A]]()
      register(t => outcome.complete(t): Unit)
      outcome.get().get
    }

  /**
    * Yields the calling thread so other threads can run.
    */
  def cede: CIO[Unit] = _ => Thread.`yield`()

  /**
    * Runs blocking code. Every `CIO` already runs on the calling thread, so this is the same as [[defer]].
    */
  def blocking[A](thunk: => A): CIO[A] = _ => thunk

  private def zipAll[T <: Tuple](cios: Tuple.Map[T, CIO]): CIO[T] =
    collectAll(cios.productIterator.map(_.asInstanceOf[CIO[Any]]).toVector).map(results => Tuple.fromArray(results.lower.toArray).asInstanceOf[T])

  /**
    * Runs two computations concurrently and returns both results.
    */
  def zip[A1, A2](
    a1: CIO[A1],
    a2: CIO[A2]
  ): CIO[(A1, A2)] =
    zipAll[(A1, A2)]((a1, a2))

  /**
    * Runs three computations concurrently and returns all results.
    */
  def zip[A1, A2, A3](
    a1: CIO[A1],
    a2: CIO[A2],
    a3: CIO[A3]
  ): CIO[(A1, A2, A3)] =
    zipAll[(A1, A2, A3)]((a1, a2, a3))

  /**
    * Runs four computations concurrently and returns all results.
    */
  def zip[A1, A2, A3, A4](
    a1: CIO[A1],
    a2: CIO[A2],
    a3: CIO[A3],
    a4: CIO[A4]
  ): CIO[(A1, A2, A3, A4)] =
    zipAll[(A1, A2, A3, A4)]((a1, a2, a3, a4))

  /**
    * Runs five computations concurrently and returns all results.
    */
  def zip[A1, A2, A3, A4, A5](
    a1: CIO[A1],
    a2: CIO[A2],
    a3: CIO[A3],
    a4: CIO[A4],
    a5: CIO[A5]
  ): CIO[(A1, A2, A3, A4, A5)] =
    zipAll[(A1, A2, A3, A4, A5)]((a1, a2, a3, a4, a5))

  /**
    * Runs six computations concurrently and returns all results.
    */
  def zip[A1, A2, A3, A4, A5, A6](
    a1: CIO[A1],
    a2: CIO[A2],
    a3: CIO[A3],
    a4: CIO[A4],
    a5: CIO[A5],
    a6: CIO[A6]
  ): CIO[(A1, A2, A3, A4, A5, A6)] =
    zipAll[(A1, A2, A3, A4, A5, A6)]((a1, a2, a3, a4, a5, a6))

  /**
    * Runs seven computations concurrently and returns all results.
    */
  def zip[A1, A2, A3, A4, A5, A6, A7](
    a1: CIO[A1],
    a2: CIO[A2],
    a3: CIO[A3],
    a4: CIO[A4],
    a5: CIO[A5],
    a6: CIO[A6],
    a7: CIO[A7]
  ): CIO[(A1, A2, A3, A4, A5, A6, A7)] =
    zipAll[(A1, A2, A3, A4, A5, A6, A7)]((a1, a2, a3, a4, a5, a6, a7))

}

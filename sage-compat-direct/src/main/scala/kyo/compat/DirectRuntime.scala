package kyo.compat

import java.util.concurrent.{CompletableFuture, ExecutionException, Executors, LinkedBlockingQueue, RejectedExecutionException, Semaphore, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

// The thread helpers behind the direct binding. The doc comment of `CIO` describes the execution model.
private[compat] object DirectRuntime {

  // Creates every thread the binding starts for a computation. The factory is safe to share, and its threads inherit `CLocal` values.
  private[compat] val threads: java.util.concurrent.ThreadFactory = Thread.ofVirtual().name("sage-direct").factory()

  private[compat] def uninterruptible[A](body: => A): A = {
    val fiber = Fiber.start(() => body)
    joinUninterruptibly(fiber.thread)
    fiber.await()
  }

  // Waits for `future` and rethrows the exact throwable it was completed with.
  private[compat] def await[A](future: CompletableFuture[? <: A]): A =
    try future.get()
    catch { case _: ExecutionException => outcome(future).get }

  // Returns the outcome of a completed future. `get`, `join` and `exceptionNow` replace a `CompletionException` with its cause, so the
  // outcome is read through `handle`, which passes the throwable unchanged.
  private[compat] def outcome[A](future: CompletableFuture[? <: A]): Try[A] =
    future.handle[Try[A]]((a, error) => if (error eq null) Success(a) else Failure(error)).join()

  /**
    * Races `a` on the calling thread against `b` on a forked thread and returns the first successful result. Running `a` on the caller keeps
    * its thread-local context, which matters when `a` is user code such as a lock body. If `b` succeeds first, the caller is interrupted to
    * stop `a`, and the interrupt is cleared before `b`'s result is returned. If both fail, the first failure is thrown with the other one
    * suppressed.
    */
  private[compat] def race[A](a: () => A, b: () => A): A = {
    val caller  = Thread.currentThread()
    // The side that sets `settled` first decides the race. When `b` succeeds first, it interrupts the caller to stop `a`.
    val settled = new AtomicBoolean(false)
    Fiber.scoped { () =>
      val value = b()
      if (settled.compareAndSet(false, true)) caller.interrupt()
      value
    } { fiber =>
      // `b` won. Wait until its thread has exited so its interrupt has been delivered, then clear it.
      def resultOfB(): A = {
        joinUninterruptibly(fiber.thread)
        Thread.interrupted(): Unit
        fiber.await()
      }
      try {
        val value = a()
        if (settled.compareAndSet(false, true)) value else resultOfB()
      } catch {
        case failureA: Throwable if settled.compareAndSet(false, true) =>
          if (!NonFatal(failureA)) throw failureA
          try fiber.await()
          catch {
            case NonFatal(failureB) =>
              failureA.addSuppressed(failureB)
              throw failureA
          }
        case _: Throwable                                              => resultOfB()
      }
    }
  }

  /**
    * Runs `run` for each item on its own virtual thread, at most `concurrency` at a time, and returns the results in input order. The first
    * failure interrupts the remaining work and is thrown after every started thread has exited.
    */
  private[compat] def parallel[A, B](items: Iterable[A], concurrency: Int)(run: A => B): Vector[B] = {
    val permits  = new Semaphore(math.max(1, concurrency))
    val failure  = new AtomicReference[Throwable](null)
    // `shutdownNow` interrupts every thread the executor has started and rejects later submissions, so the first failure stops all work.
    val executor = Executors.newThreadPerTaskExecutor(threads)
    val futures  = Vector.newBuilder[java.util.concurrent.Future[B]]
    try {
      val remaining = items.iterator
      while (remaining.hasNext && !executor.isShutdown) {
        val item = remaining.next()
        permits.acquire()
        try
          futures += executor.submit[B] { () =>
            try run(item)
            catch {
              case t: Throwable =>
                if (failure.compareAndSet(null, t)) executor.shutdownNow(): Unit
                throw t
            } finally permits.release()
          }: Unit
        catch { case _: RejectedExecutionException => permits.release() }
      }
      executor.shutdown()
      executor.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS): Unit
    } catch {
      case e: Throwable =>
        executor.shutdownNow(): Unit
        executor.close()
        throw e
    }
    val failed   = failure.get()
    if (failed ne null) throw failed
    futures.result().map(_.get())
  }

  // The work queue of the thread that waits for the current bounced chain (see `CIO.flatMap`). It is set on the threads that continue the
  // chain, and it is a plain `ThreadLocal`, so threads that they fork do not inherit it.
  private val home = new ThreadLocal[LinkedBlockingQueue[Runnable]]

  /**
    * Continues a chain on a fresh virtual thread and waits for it. The thread that started the chain runs the work that [[onHome]] hands
    * back to it while it waits, so that work sees the thread-local values of the code that started the chain.
    */
  private[compat] def bounce[A](body: () => A): A = {
    val inherited = home.get
    val inbox     = if (inherited eq null) new LinkedBlockingQueue[Runnable] else inherited
    Fiber.scoped { () =>
      home.set(inbox)
      body()
    } { fiber =>
      if (inherited eq null) {
        // wake the loop once the chain has finished
        fiber.result.whenComplete((_, _) => inbox.put(() => ())): Unit
        while (!fiber.result.isDone) inbox.take().run()
      }
      fiber.await()
    }
  }

  // Runs `body` on the thread that started the current bounced chain, or on the calling thread outside a bounced chain.
  private[compat] def onHome[A](body: () => A): A = {
    val inbox = home.get
    if (inbox eq null) body()
    else {
      val result = new CompletableFuture[A]()
      inbox.put(() => completeWith(result)(body))
      await(result)
    }
  }

  private def completeWith[A](result: CompletableFuture[A])(body: () => A): Unit =
    try result.complete(body()): Unit
    catch { case t: Throwable => result.completeExceptionally(t): Unit }

  private[compat] def joinUninterruptibly(thread: Thread): Unit = {
    var interrupted = false
    while (thread.isAlive)
      try thread.join()
      catch { case _: InterruptedException => interrupted = true }
    if (interrupted) Thread.currentThread().interrupt()
  }

  // A computation running on its own virtual thread. `result` holds its outcome, including fatal throwables.
  final private[compat] class Fiber[+A] private (val thread: Thread, val result: CompletableFuture[? <: A]) {

    def await(): A = DirectRuntime.await(result)

    // Returns `None` when `d` elapses before the thread exits. The thread completes `result` as its last step, so `await()` then returns at once.
    def await(d: FiniteDuration): Option[A] =
      if (thread.join(java.time.Duration.ofNanos(d.toNanos))) Some(await()) else None
  }

  private[compat] object Fiber {

    def start[A](body: () => A): Fiber[A] = {
      val result = new CompletableFuture[A]()
      val thread = threads.newThread(() => completeWith(result)(body))
      val fiber  = new Fiber(thread, result)
      thread.start()
      fiber
    }

    /**
      * Runs `body` on a new fiber and passes the fiber to `use`. If the fiber is still running when `use` returns or throws, it is interrupted
      * and joined first, so a forked computation never outlives the operation that started it.
      */
    def scoped[A, B](body: () => A)(use: Fiber[A] => B): B = {
      val fiber = start(body)
      try use(fiber)
      finally
        if (!fiber.result.isDone) {
          fiber.thread.interrupt()
          joinUninterruptibly(fiber.thread)
        }
    }
  }
}

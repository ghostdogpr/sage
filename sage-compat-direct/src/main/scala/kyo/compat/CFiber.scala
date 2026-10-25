package kyo.compat

/**
  * Uses a fiber from the binding's runtime, which runs a computation on its own virtual thread. A fiber is not tied to the computation that started
  * it. It runs until its body completes, fails, or its thread is interrupted.
  */
opaque type CFiber[+A] = DirectRuntime.Fiber[A]

object CFiber {

  /**
    * Starts `c` on a new virtual thread. The thread inherits the caller's `CLocal` values.
    */
  def init[A](c: CIO[A]): CIO[CFiber[A]] =
    CIO.defer(DirectRuntime.Fiber.start(() => c.lower))

  extension [A](self: CFiber[A]) {

    /**
      * Blocks until the fiber completes and returns its result or rethrows its failure.
      */
    def get: CIO[A] = CIO.defer(self.await())

    /**
      * Registers `cb` to run on a new virtual thread when the fiber completes. A callback registered after completion runs immediately. A
      * failure inside `cb` goes to the thread's uncaught exception handler.
      */
    def onComplete(cb: scala.util.Try[A] => CIO[Unit]): CIO[Unit] =
      CIO.defer {
        self.result.whenComplete((_, _) => DirectRuntime.threads.newThread(() => cb(DirectRuntime.outcome(self.result)).lower).start()): Unit
      }

  }

}

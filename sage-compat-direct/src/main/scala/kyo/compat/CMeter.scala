package kyo.compat

import java.util.concurrent.Semaphore

/**
  * Uses `java.util.concurrent.Semaphore`. `lift` and `lower` return the existing semaphore. `run` releases its permit when the computation
  * succeeds, fails, or is interrupted.
  */
opaque type CMeter = Semaphore

object CMeter {

  /**
    * Allocates a semaphore with `permits` permits.
    */
  inline def init(inline permits: Int): CIO[CMeter] =
    CIO.defer(new Semaphore(permits))

  /**
    * Wraps a native `Semaphore` as a `CMeter`. The conversion is the identity on the carrier.
    */
  inline def lift(inline u: Semaphore): CMeter = u

  extension (self: CMeter) {

    /**
      * Unwraps to the native `Semaphore`. The conversion is the identity on the carrier.
      */
    inline def lower: Semaphore = self

    /**
      * Acquires a permit, blocking until one is available, runs `c`, and then releases the permit.
      */
    def run[A](c: => CIO[A]): CIO[A] =
      CIO.around(c) { run =>
        self.acquire()
        try run()
        finally self.release()
      }

    /**
      * Runs `c` if a permit is available immediately. Returns `None` without running `c` otherwise.
      */
    def tryRun[A](c: => CIO[A]): CIO[Option[A]] =
      CIO.around(c) { run =>
        if (!self.tryAcquire()) None
        else
          try Some(run())
          finally self.release()
      }

    /**
      * Returns the number of available permits.
      */
    inline def availablePermits: CIO[Int] =
      CIO.defer(self.availablePermits())

  }

}

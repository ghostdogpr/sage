package kyo.compat

import java.util.concurrent.CountDownLatch

/**
  * Uses `java.util.concurrent.CountDownLatch`. `lift` and `lower` return the existing latch. A count of zero or less creates a latch that is
  * already released.
  */
opaque type CLatch = CountDownLatch

object CLatch {

  /**
    * Allocates a latch with count `n`.
    */
  inline def init(inline n: Int): CIO[CLatch] =
    CIO.defer(new CountDownLatch(math.max(0, n)))

  /**
    * Wraps a native `CountDownLatch` as a `CLatch`. The conversion is the identity on the carrier.
    */
  inline def lift(inline u: CountDownLatch): CLatch = u

  extension (inline self: CLatch) {

    /**
      * Unwraps to the native `CountDownLatch`. The conversion is the identity on the carrier.
      */
    inline def lower: CountDownLatch = self

    /**
      * Blocks until the count reaches zero.
      */
    inline def await: CIO[Unit] = CIO.defer(self.await())

    /**
      * Decrements the count. Waiters are released when it reaches zero.
      */
    inline def release: CIO[Unit] = CIO.defer(self.countDown())

  }

}

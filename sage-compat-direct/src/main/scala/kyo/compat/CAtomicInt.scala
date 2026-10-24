package kyo.compat

import java.util.concurrent.atomic.AtomicInteger

/**
  * Uses `java.util.concurrent.atomic.AtomicInteger`. `lift` and `lower` return the existing atomic cell.
  */
opaque type CAtomicInt = AtomicInteger

object CAtomicInt {

  /**
    * Allocates a fresh atomic cell initialized to `v`.
    */
  inline def init(inline v: Int): CIO[CAtomicInt] =
    CIO.defer(new AtomicInteger(v))

  /**
    * Wraps a native `AtomicInteger` as a `CAtomicInt`. The conversion is the identity on the carrier.
    */
  inline def lift(inline u: AtomicInteger): CAtomicInt = u

  extension (inline self: CAtomicInt) {

    /**
      * Unwraps to the native `AtomicInteger`. The conversion is the identity on the carrier.
      */
    inline def lower: AtomicInteger = self

    /**
      * Reads the current value.
      */
    inline def get: CIO[Int] = CIO.defer(self.get())

    /**
      * Atomically sets the value to `v`.
      */
    inline def set(inline v: Int): CIO[Unit] = CIO.defer(self.set(v))

    /**
      * Atomically sets the value to `v` and returns the previous value.
      */
    inline def getAndSet(inline v: Int): CIO[Int] = CIO.defer(self.getAndSet(v))

    /**
      * Atomically increments by 1 and returns the new value.
      */
    inline def incrementAndGet: CIO[Int] = CIO.defer(self.incrementAndGet())

    /**
      * Atomically increments by 1 and returns the previous value.
      */
    inline def getAndIncrement: CIO[Int] = CIO.defer(self.getAndIncrement())

    /**
      * Atomically decrements by 1 and returns the new value.
      */
    inline def decrementAndGet: CIO[Int] = CIO.defer(self.decrementAndGet())

    /**
      * Atomically decrements by 1 and returns the previous value.
      */
    inline def getAndDecrement: CIO[Int] = CIO.defer(self.getAndDecrement())

    /**
      * Atomically adds `delta` and returns the new value.
      */
    inline def addAndGet(inline delta: Int): CIO[Int] = CIO.defer(self.addAndGet(delta))

    /**
      * Atomically adds `delta` and returns the previous value.
      */
    inline def getAndAdd(inline delta: Int): CIO[Int] = CIO.defer(self.getAndAdd(delta))

    /**
      * Atomic compare-and-set: replaces `expected` with `updated` if the current value equals `expected`.
      */
    inline def compareAndSet(inline expected: Int, inline updated: Int): CIO[Boolean] =
      CIO.defer(self.compareAndSet(expected, updated))

  }

}

package kyo.compat

import java.util.concurrent.atomic.AtomicReference

/**
  * Uses `java.util.concurrent.atomic.AtomicReference[A]`. `lift` and `lower` return the existing atomic reference. `compareAndSet` follows
  * the JDK contract and compares references, which matches the Ox binding.
  */
opaque type CAtomicRef[A] = AtomicReference[A]

object CAtomicRef {

  /**
    * Allocates a fresh atomic reference initialized to `a`.
    */
  inline def init[A](inline a: A): CIO[CAtomicRef[A]] =
    CIO.defer(new AtomicReference[A](a))

  /**
    * Wraps a native `AtomicReference` as a `CAtomicRef`. The conversion is the identity on the carrier.
    */
  inline def lift[A](inline u: AtomicReference[A]): CAtomicRef[A] = u

  extension [A](inline self: CAtomicRef[A]) {

    /**
      * Unwraps to the native `AtomicReference`. The conversion is the identity on the carrier.
      */
    inline def lower: AtomicReference[A] = self

    /**
      * Reads the current value.
      */
    inline def get: CIO[A] = CIO.defer(self.get())

    /**
      * Atomically sets the value to `a`.
      */
    inline def set(inline a: A): CIO[Unit] = CIO.defer(self.set(a))

    /**
      * Atomically sets the value to `a` and returns the previous value.
      */
    inline def getAndSet(inline a: A): CIO[A] = CIO.defer(self.getAndSet(a))

    /**
      * Atomically applies `f` to the current value and returns the new value.
      */
    inline def updateAndGet(inline f: A => A): CIO[A] = CIO.defer(self.updateAndGet(a => f(a)))

    /**
      * Atomically applies `f` to the current value and returns the previous value.
      */
    inline def getAndUpdate(inline f: A => A): CIO[A] = CIO.defer(self.getAndUpdate(a => f(a)))

    /**
      * Atomic compare-and-set: replaces `expected` with `updated` if the current value is the same reference as `expected`.
      */
    inline def compareAndSet(inline expected: A, inline updated: A): CIO[Boolean] =
      CIO.defer(self.compareAndSet(expected, updated))

  }

}

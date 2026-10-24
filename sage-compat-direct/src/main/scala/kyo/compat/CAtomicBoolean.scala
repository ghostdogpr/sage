package kyo.compat

import java.util.concurrent.atomic.AtomicBoolean

/**
  * Uses `java.util.concurrent.atomic.AtomicBoolean`. `lift` and `lower` return the existing atomic cell.
  */
opaque type CAtomicBoolean = AtomicBoolean

object CAtomicBoolean {

  /**
    * Allocates a fresh atomic boolean initialized to `v`.
    */
  inline def init(inline v: Boolean): CIO[CAtomicBoolean] =
    CIO.defer(new AtomicBoolean(v))

  /**
    * Wraps a native `AtomicBoolean` as a `CAtomicBoolean`. The conversion is the identity on the carrier.
    */
  inline def lift(inline u: AtomicBoolean): CAtomicBoolean = u

  extension (inline self: CAtomicBoolean) {

    /**
      * Unwraps to the native `AtomicBoolean`. The conversion is the identity on the carrier.
      */
    inline def lower: AtomicBoolean = self

    /**
      * Reads the current value.
      */
    inline def get: CIO[Boolean] = CIO.defer(self.get())

    /**
      * Atomically sets the value to `v`.
      */
    inline def set(inline v: Boolean): CIO[Unit] = CIO.defer(self.set(v))

    /**
      * Atomically sets the value to `v` and returns the previous value.
      */
    inline def getAndSet(inline v: Boolean): CIO[Boolean] = CIO.defer(self.getAndSet(v))

    /**
      * Atomic compare-and-set: replaces `expected` with `updated` if the current value equals `expected`.
      */
    inline def compareAndSet(inline expected: Boolean, inline updated: Boolean): CIO[Boolean] =
      CIO.defer(self.compareAndSet(expected, updated))

  }

}

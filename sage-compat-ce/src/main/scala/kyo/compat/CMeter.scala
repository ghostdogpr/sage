package kyo.compat

import cats.effect.IO
import cats.effect.std.Semaphore

/**
  * Uses `cats.effect.std.Semaphore[IO]`. Cats Effect has no `Frame` or `Trace` to propagate. `lift` and `lower` return the existing semaphore.
  * Cats Effect stores permit counts as `Long`, while `init` and `availablePermits` convert them to and from `Int`. `tryRun` uses
  * `tryPermit`, which returns `Resource[IO, Boolean]`.
  */
opaque type CMeter = Semaphore[IO]

object CMeter {

  /**
    * Allocates a counting semaphore with `permits` permits. `permits` must be nonnegative.
    */
  inline def init(inline permits: Int): CIO[CMeter] =
    CIO.lift(Semaphore[IO](permits.toLong))

  /**
    * Wraps a native `cats.effect.std.Semaphore` as a `CMeter`. The conversion is the identity on the carrier.
    */
  inline def lift(inline u: Semaphore[IO]): CMeter = u

  extension (inline self: CMeter) {

    /**
      * Unwraps to the native `cats.effect.std.Semaphore`. The conversion is the identity on the carrier.
      */
    inline def lower: Semaphore[IO] = self

    /**
      * Acquires one permit, runs `c`, and releases on completion (success or failure).
      */
    inline def run[A](inline c: CIO[A]): CIO[A] =
      CIO.lift(self.permit.use(_ => c.lower))

    /**
      * Attempts to acquire a permit without blocking; runs `c` if successful, otherwise returns `None`.
      */
    inline def tryRun[A](inline c: CIO[A]): CIO[Option[A]] =
      CIO.lift(self.tryPermit.use {
        case true  => c.lower.map(Some(_))
        case false => IO.none[A]
      })

    /**
      * Returns the current count of available permits.
      */
    inline def availablePermits: CIO[Int] =
      CIO.lift(self.available.map(_.toInt))

  }

}

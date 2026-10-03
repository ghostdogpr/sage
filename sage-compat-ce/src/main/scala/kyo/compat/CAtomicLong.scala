package kyo.compat

import cats.effect.IO
import cats.effect.kernel.Ref

/**
  * Uses `cats.effect.kernel.Ref[IO, Long]` through [[CAtomicRef]], which provides every operation.
  */
type CAtomicLong = CAtomicRef[Long]

object CAtomicLong {

  /**
    * Allocates a fresh atomic long initialized to `v`.
    */
  inline def init(inline v: Long): CIO[CAtomicLong] = CAtomicRef.init(v)

  /**
    * Wraps a native `cats.effect.kernel.Ref[IO, Long]` as a `CAtomicLong`. The conversion is the identity on the carrier.
    */
  inline def lift(inline u: Ref[IO, Long]): CAtomicLong = CAtomicRef.lift(u)
}

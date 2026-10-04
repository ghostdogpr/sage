package kyo.compat

import cats.effect.IO
import cats.effect.kernel.Ref

/**
  * Uses `cats.effect.kernel.Ref[IO, Boolean]` through [[CAtomicRef]], which provides every operation.
  */
type CAtomicBoolean = CAtomicRef[Boolean]

object CAtomicBoolean {

  /**
    * Allocates a fresh atomic boolean initialized to `v`.
    */
  inline def init(inline v: Boolean): CIO[CAtomicBoolean] = CAtomicRef.init(v)

  /**
    * Wraps a native `cats.effect.kernel.Ref[IO, Boolean]` as a `CAtomicBoolean`. The conversion is the identity on the carrier.
    */
  inline def lift(inline u: Ref[IO, Boolean]): CAtomicBoolean = CAtomicRef.lift(u)
}

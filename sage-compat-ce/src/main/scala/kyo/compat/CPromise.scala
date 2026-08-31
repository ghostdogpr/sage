package kyo.compat

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import cats.effect.Deferred
import cats.effect.IO

/**
  * Uses `cats.effect.Deferred[IO, Try[A]]`. Cats Effect has no `Frame` or `Trace` to propagate. `Deferred` stores one plain value and has no
  * error channel. `complete` accepts an `A`, and `get` returns an `A`. A `CPromise` must also store failures, so this implementation stores a
  * `Try[A]`. `succeed` and `fail` return `true` for the first completion and `false` for later attempts. `poll` returns the stored `Try`.
  */
opaque type CPromise[A] = Deferred[IO, Try[A]]

object CPromise {

  /**
    * Allocates a fresh single-shot promise.
    */
  inline def init[A]: CIO[CPromise[A]] =
    CIO.lift(Deferred[IO, Try[A]])

  /**
    * Wraps a native `cats.effect.Deferred` as a `CPromise`. The conversion is the identity on the carrier.
    */
  inline def lift[A](inline u: Deferred[IO, Try[A]]): CPromise[A] = u

  extension [A](inline self: CPromise[A]) {

    /**
      * Unwraps to the native `cats.effect.Deferred`. The conversion is the identity on the carrier.
      */
    inline def lower: Deferred[IO, Try[A]] = self

    /**
      * Attempts to complete the promise with `a`; returns `true` if this is the first completion.
      */
    inline def succeed(inline a: A): CIO[Boolean] =
      CIO.lift(self.complete(Success(a)))

    /**
      * Attempts to complete the promise with failure `e`; returns `true` if this is the first completion.
      */
    inline def fail(inline e: Throwable): CIO[Boolean] =
      CIO.lift(self.complete(Failure(e)))

    /**
      * Suspends until the promise is completed and returns its value.
      */
    inline def get: CIO[A] =
      CIO.lift(self.get.flatMap {
        case Success(a) => IO.pure(a)
        case Failure(e) => IO.raiseError(e)
      })

    /**
      * Returns the current state without blocking: `None` if pending, `Some(Try)` if completed.
      */
    inline def poll: CIO[Option[Try[A]]] =
      CIO.lift(self.tryGet)

    /**
      * Returns `true` if the promise has been completed.
      */
    inline def done: CIO[Boolean] =
      CIO.lift(self.tryGet.map(_.isDefined))

  }

}

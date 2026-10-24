package kyo.compat

import java.util.concurrent.CompletableFuture

/**
  * Uses `java.util.concurrent.CompletableFuture[A]`. `lift` and `lower` return the existing future. `succeed` and `fail` return `true` only
  * for the first completion. `get` blocks the calling thread and rethrows the original failure. `poll` reports a cancelled future as pending.
  */
opaque type CPromise[A] = CompletableFuture[A]

object CPromise {

  /**
    * Allocates a fresh single-shot promise.
    */
  inline def init[A]: CIO[CPromise[A]] =
    CIO.defer(new CompletableFuture[A]())

  /**
    * Wraps a native `CompletableFuture` as a `CPromise`. The conversion is the identity on the carrier.
    */
  inline def lift[A](inline u: CompletableFuture[A]): CPromise[A] = u

  extension [A](inline self: CPromise[A]) {

    /**
      * Unwraps to the native `CompletableFuture`. The conversion is the identity on the carrier.
      */
    inline def lower: CompletableFuture[A] = self

    /**
      * Attempts to complete the promise with `a`. Returns `true` if this is the first completion.
      */
    inline def succeed(inline a: A): CIO[Boolean] =
      CIO.defer(self.complete(a))

    /**
      * Attempts to complete the promise with failure `e`. Returns `true` if this is the first completion.
      */
    inline def fail(inline e: Throwable): CIO[Boolean] =
      CIO.defer(self.completeExceptionally(e))

    /**
      * Blocks until the promise is completed and returns its value or rethrows its failure.
      */
    inline def get: CIO[A] = CIO.defer(DirectRuntime.await(self.lower))

    /**
      * Returns `None` while the promise is pending or cancelled, and `Some` with the outcome once it is completed.
      */
    inline def poll: CIO[Option[scala.util.Try[A]]] =
      CIO.defer(Option.when(self.isDone && !self.isCancelled)(DirectRuntime.outcome(self.lower)))

    /**
      * Returns `true` if the promise has been completed.
      */
    inline def done: CIO[Boolean] =
      CIO.defer(self.isDone)

  }

}

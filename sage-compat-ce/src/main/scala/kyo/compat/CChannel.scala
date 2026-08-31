package kyo.compat

import cats.effect.IO
import cats.effect.std.Queue

/**
  * Uses `cats.effect.std.Queue[IO, A]`, a bounded asynchronous FIFO queue. Cats Effect has no `Frame` or `Trace` to propagate. `lift` and
  * `lower` return the existing Cats Effect queue. Cats Effect `Queue` has no shutdown operation, so this API does not provide `close`.
  */
opaque type CChannel[A] = Queue[IO, A]

object CChannel {

  /**
    * Allocates a bounded FIFO channel of the given capacity. `capacity` must be nonnegative; a capacity of 0 creates a synchronous
    * channel, where each `put` suspends until a matching `take`.
    */
  inline def init[A](inline capacity: Int): CIO[CChannel[A]] =
    CIO.lift(Queue.bounded[IO, A](capacity))

  /**
    * Wraps a native `cats.effect.std.Queue` as a `CChannel`. The conversion is the identity on the carrier.
    */
  inline def lift[A](inline u: Queue[IO, A]): CChannel[A] = u

  extension [A](inline self: CChannel[A]) {

    /**
      * Unwraps to the native `cats.effect.std.Queue`. The conversion is the identity on the carrier.
      */
    inline def lower: Queue[IO, A] = self

    /**
      * Enqueues `v`; suspends when the channel is full.
      */
    inline def put(inline v: A): CIO[Unit] = CIO.lift(self.offer(v))

    /**
      * Dequeues the next element; suspends when the channel is empty.
      */
    inline def take: CIO[A] = CIO.lift(self.take)

    /**
      * Dequeues without suspending: returns `Some(a)` if an element is available and `None` otherwise.
      */
    inline def poll: CIO[Option[A]] = CIO.lift(self.tryTake)

  }

}

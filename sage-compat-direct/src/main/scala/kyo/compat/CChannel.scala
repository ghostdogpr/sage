package kyo.compat

import java.util.concurrent.LinkedBlockingQueue

/**
  * Uses a bounded `java.util.concurrent.LinkedBlockingQueue[A]`. `lift` and `lower` return the existing queue. `put` blocks while the
  * channel is full and `take` blocks while it is empty. `poll` does not block.
  */
opaque type CChannel[A] = LinkedBlockingQueue[A]

object CChannel {

  /**
    * Allocates a bounded FIFO channel with the given capacity.
    */
  inline def init[A](inline capacity: Int): CIO[CChannel[A]] =
    CIO.defer(new LinkedBlockingQueue[A](capacity))

  /**
    * Wraps a native `LinkedBlockingQueue` as a `CChannel`. The conversion is the identity on the carrier.
    */
  inline def lift[A](inline u: LinkedBlockingQueue[A]): CChannel[A] = u

  extension [A](inline self: CChannel[A]) {

    /**
      * Unwraps to the native `LinkedBlockingQueue`. The conversion is the identity on the carrier.
      */
    inline def lower: LinkedBlockingQueue[A] = self

    /**
      * Enqueues `v`, blocking while the channel is full.
      */
    inline def put(inline v: A): CIO[Unit] = CIO.defer(self.put(v))

    /**
      * Dequeues the next element, blocking while the channel is empty.
      */
    inline def take: CIO[A] = CIO.defer(self.take())

    /**
      * Returns the next element if one is available, without blocking.
      */
    inline def poll: CIO[Option[A]] = CIO.defer(Option(self.poll()))

  }

}

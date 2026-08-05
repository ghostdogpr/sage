package kyo.compat

/**
  * Underlying carrier is `Vector[A]`. Cats Effect has no native bulk-collection type, so `CChunk` reuses the standard-library `Vector`.
  * `lift` and `lower` are identity since the carrier is already a `Vector`. The portable accessor surface (`toSeq`, `toIndexedSeq`,
  * `apply`, `size`, `iterator`, `isEmpty`) is exposed on every backend.
  */
opaque type CChunk[+A] = Vector[A]

object CChunk {

  /**
    * Wraps a `Vector` as a `CChunk`. The conversion is the identity on the carrier.
    */
  inline def lift[A](inline c: Vector[A]): CChunk[A] = c

  extension [A](inline self: CChunk[A]) {

    /**
      * Unwraps to the native `Vector`. The conversion is the identity on the carrier.
      */
    inline def lower: Vector[A] = self

    /**
      * Views the chunk as a `Seq[A]`.
      */
    inline def toSeq: Seq[A] = self

    /**
      * Views the chunk as an `IndexedSeq[A]`.
      */
    inline def toIndexedSeq: IndexedSeq[A] = self

    /**
      * Returns the element at index `i`.
      */
    inline def apply(inline i: Int): A = self(i)

    /**
      * Returns the number of elements.
      */
    inline def size: Int = self.size

    /**
      * Returns an iterator over the chunk's elements.
      */
    inline def iterator: Iterator[A] = self.iterator

    /**
      * Returns `true` if the chunk has no elements.
      */
    inline def isEmpty: Boolean = self.isEmpty

  }

}

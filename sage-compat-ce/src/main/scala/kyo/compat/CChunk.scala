package kyo.compat

/**
  * Uses `Vector[A]` because Cats Effect has no bulk collection type. `lift` and `lower` return the existing `Vector`. Every backend provides
  * `toSeq`, `toIndexedSeq`, `apply`, `size`, `iterator`, and `isEmpty`.
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

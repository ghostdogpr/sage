package sage

import java.nio.charset.StandardCharsets
import java.util.Arrays

/**
  * The Core's opaque, immutable byte container, used at every protocol and codec boundary. Content equality is explicit: use `sameBytes`,
  * never `==`, which compares references and is unreliable by design. Construction is copy-aware — see [[Bytes.wrap]] (zero-copy) versus
  * [[Bytes.fromArray]] (defensive copy).
  */
opaque type Bytes = IArray[Byte]

object Bytes {

  /**
    * The empty byte container.
    */
  val empty: Bytes = IArray.empty[Byte]

  /**
    * The UTF-8 encoding of `value`.
    */
  def utf8(value: String): Bytes = IArray.unsafeFromArray(value.getBytes(StandardCharsets.UTF_8))

  /**
    * Zero-copy: wraps an already-immutable `IArray`.
    */
  def wrap(bytes: IArray[Byte]): Bytes = bytes

  /**
    * Defensive copy of a mutable `Array`, so later mutation of the source cannot change these bytes.
    */
  def fromArray(bytes: Array[Byte]): Bytes = IArray.unsafeFromArray(bytes.clone())

  /**
    * One contiguous container from many, so a Pipeline's commands reach the socket as a single write.
    */
  def concat(parts: Seq[Bytes]): Bytes = concatBy(parts)(identity)

  /**
    * Like [[concat]], but reads each part from `items` via `payload`, sparing the caller a throwaway mapped collection.
    */
  def concatBy[A](items: Seq[A])(payload: A => Bytes): Bytes =
    items match {
      case Seq()     => empty
      case Seq(only) => payload(only)
      case _         =>
        var total  = 0
        items.foreach(item => total += arr(payload(item)).length)
        val out    = new Array[Byte](total)
        var offset = 0
        items.foreach { item =>
          val bytes = arr(payload(item))
          System.arraycopy(bytes, 0, out, offset, bytes.length)
          offset += bytes.length
        }
        IArray.unsafeFromArray(out)
    }

  extension (self: Bytes) {

    /**
      * The number of bytes.
      */
    def length: Int = arr(self).length

    /**
      * Content equality. The correct way to compare two `Bytes`; `==` compares references.
      */
    def sameBytes(that: Bytes): Boolean = Arrays.equals(arr(self), arr(that))

    /**
      * A content-based hash, consistent with [[sameBytes]] — equal content yields equal hashes.
      */
    def contentHashCode: Int = Arrays.hashCode(arr(self))

    /**
      * Decodes the bytes as UTF-8.
      */
    def asUtf8String: String = new String(arr(self), StandardCharsets.UTF_8)

    /**
      * A fresh mutable `Array` copy, safe for the caller to mutate.
      */
    def toArray: Array[Byte] = arr(self).clone()

    /**
      * Zero-copy view as an immutable `IArray`.
      */
    def toIArray: IArray[Byte] = self

    /**
      * No copy; callers must never mutate the result.
      */
    private[sage] def unsafeArray: Array[Byte] = arr(self)
  }

  private def arr(self: Bytes): Array[Byte] = self.asInstanceOf[Array[Byte]]
}

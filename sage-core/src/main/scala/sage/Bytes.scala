package sage

import java.nio.charset.StandardCharsets
import java.util.Arrays

/**
  * The Core's opaque, immutable byte container, used at every protocol and codec boundary.
  *
  * Content equality is explicit: use `sameBytes` (with `contentHashCode` as its companion). Universal `==` on Bytes compares references and
  * is unreliable — never use it (ADR-0004).
  */
opaque type Bytes = IArray[Byte]

object Bytes {

  val empty: Bytes = IArray.empty[Byte]

  /**
    * Encodes a string as its UTF-8 bytes.
    */
  def utf8(value: String): Bytes = IArray.unsafeFromArray(value.getBytes(StandardCharsets.UTF_8))

  /**
    * Wraps an immutable array. Zero-copy: IArray cannot be mutated by the caller.
    */
  def wrap(bytes: IArray[Byte]): Bytes = bytes

  /**
    * Copies a mutable array defensively.
    */
  def fromArray(bytes: Array[Byte]): Bytes = IArray.unsafeFromArray(bytes.clone())

  extension (self: Bytes) {

    def length: Int = arr(self).length

    /**
      * Content equality — the only reliable way to compare two Bytes.
      */
    def sameBytes(that: Bytes): Boolean = Arrays.equals(arr(self), arr(that))

    /**
      * Content-based hash, consistent with `sameBytes`.
      */
    def contentHashCode: Int = Arrays.hashCode(arr(self))

    /**
      * Decodes the bytes as UTF-8.
      */
    def asUtf8String: String = new String(arr(self), StandardCharsets.UTF_8)

    /**
      * Returns a defensive copy as a mutable array.
      */
    def toArray: Array[Byte] = arr(self).clone()

    /**
      * Returns the underlying immutable array. Zero-copy.
      */
    def toIArray: IArray[Byte] = self
  }

  private def arr(self: Bytes): Array[Byte] = self.asInstanceOf[Array[Byte]]
}

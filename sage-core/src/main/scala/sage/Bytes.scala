package sage

import java.nio.charset.StandardCharsets
import java.util.Arrays

/**
  * An opaque, immutable byte container. Universal `==` on Bytes compares references and is unreliable — use `sameBytes` (ADR-0004).
  */
opaque type Bytes = IArray[Byte]

object Bytes {

  val empty: Bytes = IArray.empty[Byte]

  def utf8(value: String): Bytes = IArray.unsafeFromArray(value.getBytes(StandardCharsets.UTF_8))

  def wrap(bytes: IArray[Byte]): Bytes = bytes

  def fromArray(bytes: Array[Byte]): Bytes = IArray.unsafeFromArray(bytes.clone())

  extension (self: Bytes) {

    def length: Int = arr(self).length

    def sameBytes(that: Bytes): Boolean = Arrays.equals(arr(self), arr(that))

    def contentHashCode: Int = Arrays.hashCode(arr(self))

    def asUtf8String: String = new String(arr(self), StandardCharsets.UTF_8)

    def toArray: Array[Byte] = arr(self).clone()

    def toIArray: IArray[Byte] = self

    /**
      * No copy; callers must never mutate the result.
      */
    private[sage] def unsafeArray: Array[Byte] = arr(self)
  }

  private def arr(self: Bytes): Array[Byte] = self.asInstanceOf[Array[Byte]]
}

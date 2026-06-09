package sage

import java.nio.charset.StandardCharsets
import java.util.Arrays

/**
  * An opaque, immutable byte container. Universal `==` on Bytes compares references and is unreliable — use `sameBytes`.
  */
opaque type Bytes = IArray[Byte]

object Bytes {

  val empty: Bytes = IArray.empty[Byte]

  def utf8(value: String): Bytes = IArray.unsafeFromArray(value.getBytes(StandardCharsets.UTF_8))

  def wrap(bytes: IArray[Byte]): Bytes = bytes

  def fromArray(bytes: Array[Byte]): Bytes = IArray.unsafeFromArray(bytes.clone())

  // One contiguous buffer from many: a Pipeline's commands are concatenated so they reach the socket as a single write.
  def concat(parts: Seq[Bytes]): Bytes = concatBy(parts)(identity)

  // like `concat`, but pulls each part from `items` via `payload`, sparing the caller a throwaway mapped collection on the batch path
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

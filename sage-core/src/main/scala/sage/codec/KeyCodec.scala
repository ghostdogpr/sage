package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Encodes/decodes a user type at a key position. Deliberately unrelated to [[ValueCodec]] so given resolution stays unambiguous.
  */
trait KeyCodec[A] {

  def encode(value: A): Bytes

  def decode(bytes: Bytes): Either[DecodeError, A]
}

object KeyCodec {

  given string: KeyCodec[String] = new KeyCodec[String] {

    def encode(value: String): Bytes = Bytes.utf8(value)

    def decode(bytes: Bytes): Either[DecodeError, String] = Right(bytes.asUtf8String)
  }
}

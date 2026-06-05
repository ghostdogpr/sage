package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Converts one user type to/from its wire bytes at a key position.
  *
  * Deliberately unrelated to [[ValueCodec]]: using a type as a key is an explicit opt-in, and the absence of subtyping keeps given
  * resolution unambiguous. A boundary converter, not a serialization framework.
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

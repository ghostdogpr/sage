package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Encodes/decodes a user type at a value position. Deliberately unrelated to [[KeyCodec]] — see the note there.
  */
trait ValueCodec[A] {

  def encode(value: A): Bytes

  def decode(bytes: Bytes): Either[DecodeError, A]
}

object ValueCodec {

  given string: ValueCodec[String] = new ValueCodec[String] {

    def encode(value: String): Bytes = Bytes.utf8(value)

    def decode(bytes: Bytes): Either[DecodeError, String] = Right(bytes.asUtf8String)
  }
}

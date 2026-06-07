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

  // no Double/Float/Boolean keys: float formatting is representation-sensitive, so two writers can silently address different keys

  given string: KeyCodec[String] = instance(Bytes.utf8, Primitives.decodeUtf8)

  given int: KeyCodec[Int] = instance(Primitives.encodeNumber, Primitives.decodeNumber("Int", _.toIntOption))

  given long: KeyCodec[Long] = instance(Primitives.encodeNumber, Primitives.decodeNumber("Long", _.toLongOption))

  given bytes: KeyCodec[Bytes] = instance(identity, Right(_))

  given byteArray: KeyCodec[Array[Byte]] = instance(Bytes.fromArray, raw => Right(raw.toArray))

  private def instance[A](enc: A => Bytes, dec: Bytes => Either[DecodeError, A]): KeyCodec[A] =
    new KeyCodec[A] {

      def encode(value: A): Bytes = enc(value)

      def decode(bytes: Bytes): Either[DecodeError, A] = dec(bytes)
    }
}

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

  given string: ValueCodec[String] = instance(Bytes.utf8, Primitives.decodeUtf8)

  given int: ValueCodec[Int] = instance(Primitives.encodeNumber, Primitives.decodeNumber("Int", _.toIntOption))

  given long: ValueCodec[Long] = instance(Primitives.encodeNumber, Primitives.decodeNumber("Long", _.toLongOption))

  given double: ValueCodec[Double] =
    instance(d => Bytes.utf8(Doubles.format(d)), Primitives.decodeNumber("Double", Doubles.parse))

  given float: ValueCodec[Float] =
    instance(f => Bytes.utf8(Doubles.formatFloat(f)), Primitives.decodeNumber("Float", Doubles.parseFloat))

  given boolean: ValueCodec[Boolean] = instance(Primitives.encodeBoolean, Primitives.decodeBoolean)

  given bytes: ValueCodec[Bytes] = instance(identity, Right(_))

  given byteArray: ValueCodec[Array[Byte]] = instance(Bytes.fromArray, raw => Right(raw.toArray))

  private def instance[A](enc: A => Bytes, dec: Bytes => Either[DecodeError, A]): ValueCodec[A] =
    new ValueCodec[A] {

      def encode(value: A): Bytes = enc(value)

      def decode(bytes: Bytes): Either[DecodeError, A] = dec(bytes)
    }
}

package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Encodes/decodes a user type at a value position. Deliberately unrelated to [[KeyCodec]] — see the note there.
  */
trait ValueCodec[A] { self =>

  def encode(value: A): Bytes

  def decode(bytes: Bytes): Either[DecodeError, A]

  /**
    * Derives a codec for `B` from a total, lossless mapping — the newtype case (`ValueCodec[Long].imap(UserId(_))(_.value)`). Decoding `B`
    * fails only where decoding `A` already does; use [[emap]] when the mapping into `B` can itself fail.
    */
  final def imap[B](f: A => B)(g: B => A): ValueCodec[B] =
    ValueCodec.from[B](b => self.encode(g(b)))(bytes => self.decode(bytes).map(f))

  /**
    * Derives a codec for `B` whose decode may fail — the JSON/structured case, where parsing the underlying `A` into `B` is partial. A
    * `Left` keeps the strict, no-coercion contract: bad input surfaces as a `DecodeError` rather than a coerced value.
    */
  final def emap[B](f: A => Either[DecodeError, B])(g: B => A): ValueCodec[B] =
    ValueCodec.from[B](b => self.encode(g(b)))(bytes => self.decode(bytes).flatMap(f))
}

object ValueCodec {

  def apply[A](using codec: ValueCodec[A]): ValueCodec[A] = codec

  /**
    * Builds a codec from an encode/decode pair. The decode returns `Either` so a custom codec rejects bad input the way the built-ins do,
    * rather than throwing.
    */
  def from[A](enc: A => Bytes)(dec: Bytes => Either[DecodeError, A]): ValueCodec[A] = instance(enc, dec)

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

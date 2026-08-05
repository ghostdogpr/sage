package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Encodes and decodes payload values. This type is separate from [[KeyCodec]], as explained there. Built-in codecs reject bytes that are not
  * in the canonical form for the requested type (`"x"` is not a `Long`, and `"2"` is not a `Boolean`). Custom codecs created with
  * [[ValueCodec.from]], [[imap]], or [[emap]] report failures with `Either`.
  */
trait ValueCodec[A] { self =>

  /**
    * Encodes `value` to its wire bytes.
    */
  def encode(value: A): Bytes

  /**
    * Decodes wire `bytes`, failing with a [[sage.SageException.DecodeError]] when they are not `A`'s canonical form.
    */
  def decode(bytes: Bytes): Either[DecodeError, A]

  /**
    * Derives a codec for `B` from a total, lossless mapping, such as `ValueCodec[Long].imap(UserId(_))(_.value)`. Use [[emap]] when converting
    * decoded `A` values to `B` can fail.
    */
  final def imap[B](f: A => B)(g: B => A): ValueCodec[B] =
    ValueCodec.from[B](b => self.encode(g(b)))(bytes => self.decode(bytes).map(f))

  /**
    * Derives a codec for `B` when converting a decoded `A` can fail, such as parsing structured JSON. Return `Left` for invalid input.
    */
  final def emap[B](f: A => Either[DecodeError, B])(g: B => A): ValueCodec[B] =
    ValueCodec.from[B](b => self.encode(g(b)))(bytes => self.decode(bytes).flatMap(f))
}

object ValueCodec {

  /**
    * Summons the `ValueCodec[A]` in scope.
    */
  def apply[A](using codec: ValueCodec[A]): ValueCodec[A] = codec

  /**
    * Builds a codec from encode and decode functions. The decoder returns `Either` for invalid input.
    */
  def from[A](enc: A => Bytes)(dec: Bytes => Either[DecodeError, A]): ValueCodec[A] = instance(enc, dec)

  /**
    * UTF-8 text; decoding rejects malformed UTF-8.
    */
  given string: ValueCodec[String] = instance(Bytes.utf8, Primitives.decodeUtf8)

  /**
    * Decimal `Int`; decoding rejects non-numeric or out-of-range input.
    */
  given int: ValueCodec[Int] = instance(Primitives.encodeInt, Primitives.decodeNumber("Int", Primitives.parseInt))

  /**
    * Decimal `Long`; decoding rejects non-numeric or out-of-range input.
    */
  given long: ValueCodec[Long] = instance(Primitives.encodeLong, Primitives.decodeNumber("Long", Primitives.parseLong))

  /**
    * `Double` in Redis's number format, including `inf`/`-inf`/`nan`.
    */
  given double: ValueCodec[Double] =
    instance(d => Bytes.utf8(Doubles.format(d)), Primitives.decodeNumber("Double", Doubles.parse))

  /**
    * `Float` in Redis's number format, including `inf`/`-inf`/`nan`.
    */
  given float: ValueCodec[Float] =
    instance(f => Bytes.utf8(Doubles.formatFloat(f)), Primitives.decodeNumber("Float", Doubles.parseFloat))

  /**
    * `1`/`0` on the wire; decoding accepts only those two tokens.
    */
  given boolean: ValueCodec[Boolean] = instance(Primitives.encodeBoolean, Primitives.decodeBoolean)

  /**
    * Raw [[sage.Bytes]], passed through unchanged in both directions.
    */
  given bytes: ValueCodec[Bytes] = instance(identity, Right(_))

  /**
    * Raw `Array[Byte]`, copied at the boundary in both directions (see [[sage.Bytes.fromArray]]/[[sage.Bytes.toArray]]).
    */
  given byteArray: ValueCodec[Array[Byte]] = instance(Bytes.fromArray, raw => Right(raw.toArray))

  private def instance[A](enc: A => Bytes, dec: Bytes => Either[DecodeError, A]): ValueCodec[A] =
    new ValueCodec[A] {

      def encode(value: A): Bytes = enc(value)

      def decode(bytes: Bytes): Either[DecodeError, A] = dec(bytes)
    }
}

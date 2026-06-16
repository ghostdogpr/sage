package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Encodes/decodes a user type at a value (payload) position. Deliberately unrelated to [[KeyCodec]] — see the note there. The built-in
  * codecs decode strictly: bytes that are not the type's canonical wire form fail with a [[sage.SageException.DecodeError]] rather than
  * being coerced (`"x"` is not a `Long`, `"2"` is not a `Boolean`). Custom codecs built with [[ValueCodec.from]]/[[imap]]/[[emap]] keep
  * that contract by returning `Either` rather than throwing.
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

  /**
    * Summons the `ValueCodec[A]` in scope.
    */
  def apply[A](using codec: ValueCodec[A]): ValueCodec[A] = codec

  /**
    * Builds a codec from an encode/decode pair. The decode returns `Either` so a custom codec rejects bad input the way the built-ins do,
    * rather than throwing.
    */
  def from[A](enc: A => Bytes)(dec: Bytes => Either[DecodeError, A]): ValueCodec[A] = instance(enc, dec)

  /**
    * UTF-8 text; decoding rejects malformed UTF-8.
    */
  given string: ValueCodec[String] = instance(Bytes.utf8, Primitives.decodeUtf8)

  /**
    * Decimal `Int`; decoding rejects non-numeric or out-of-range input.
    */
  given int: ValueCodec[Int] = instance(Primitives.encodeNumber, Primitives.decodeNumber("Int", Primitives.parseInt))

  /**
    * Decimal `Long`; decoding rejects non-numeric or out-of-range input.
    */
  given long: ValueCodec[Long] = instance(Primitives.encodeNumber, Primitives.decodeNumber("Long", Primitives.parseLong))

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

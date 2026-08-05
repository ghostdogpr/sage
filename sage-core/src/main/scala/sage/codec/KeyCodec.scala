package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Encodes and decodes keys and hash fields. Use [[ValueCodec]] for payloads. Keeping the two codec types separate avoids ambiguous givens.
  * Floating-point and Boolean key codecs are excluded because their representations can vary between writers. Cluster-slot hashing applies to
  * key positions ([[sage.commands.Command.keyIndices]]) but is not performed by this typeclass. Built-in codecs reject non-canonical bytes.
  */
trait KeyCodec[A] { self =>

  /**
    * Encodes `value` to its wire bytes.
    */
  def encode(value: A): Bytes

  /**
    * Decodes wire `bytes`, failing with a [[sage.SageException.DecodeError]] when they are not `A`'s canonical form.
    */
  def decode(bytes: Bytes): Either[DecodeError, A]

  /**
    * Derives a key codec for `B` from a total, lossless mapping, typically for a newtype. Keep both mappings canonical and total. Mapping a
    * representation-sensitive type, such as a floating-point value, can make different writers address different keys.
    */
  final def imap[B](f: A => B)(g: B => A): KeyCodec[B] =
    KeyCodec.from[B](b => self.encode(g(b)))(bytes => self.decode(bytes).map(f))

  /**
    * Derives a key codec for `B` when converting a decoded value can fail. Return `Left` for invalid input.
    */
  final def emap[B](f: A => Either[DecodeError, B])(g: B => A): KeyCodec[B] =
    KeyCodec.from[B](b => self.encode(g(b)))(bytes => self.decode(bytes).flatMap(f))
}

object KeyCodec {

  /**
    * Summons the `KeyCodec[A]` in scope.
    */
  def apply[A](using codec: KeyCodec[A]): KeyCodec[A] = codec

  /**
    * Builds a key codec from encode and decode functions. The decoder returns `Either` for invalid input.
    */
  def from[A](enc: A => Bytes)(dec: Bytes => Either[DecodeError, A]): KeyCodec[A] = instance(enc, dec)

  /**
    * UTF-8 text; decoding rejects malformed UTF-8.
    */
  given string: KeyCodec[String] = instance(Bytes.utf8, Primitives.decodeUtf8)

  /**
    * Decimal `Int`; decoding rejects non-numeric or out-of-range input.
    */
  given int: KeyCodec[Int] = instance(Primitives.encodeInt, Primitives.decodeNumber("Int", Primitives.parseInt))

  /**
    * Decimal `Long`; decoding rejects non-numeric or out-of-range input.
    */
  given long: KeyCodec[Long] = instance(Primitives.encodeLong, Primitives.decodeNumber("Long", Primitives.parseLong))

  /**
    * Raw [[sage.Bytes]], passed through unchanged in both directions.
    */
  given bytes: KeyCodec[Bytes] = instance(identity, Right(_))

  /**
    * Raw `Array[Byte]`, copied at the boundary in both directions (see [[sage.Bytes.fromArray]]/[[sage.Bytes.toArray]]).
    */
  given byteArray: KeyCodec[Array[Byte]] = instance(Bytes.fromArray, raw => Right(raw.toArray))

  private def instance[A](enc: A => Bytes, dec: Bytes => Either[DecodeError, A]): KeyCodec[A] =
    new KeyCodec[A] {

      def encode(value: A): Bytes = enc(value)

      def decode(bytes: Bytes): Either[DecodeError, A] = dec(bytes)
    }
}

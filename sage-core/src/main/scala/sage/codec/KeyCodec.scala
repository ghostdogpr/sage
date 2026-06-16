package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Encodes/decodes a user type at a key or hash-field position — an identifier into the keyspace or a hash, never a payload (use
  * [[ValueCodec]] for those). Deliberately unrelated to [[ValueCodec]] so given resolution stays unambiguous, and float/boolean types
  * are excluded because their formatting is representation-sensitive — two writers must not silently address different keys or fields.
  * Cluster-slot hashing is a property of key positions only ([[sage.commands.Command.keyIndices]]); this typeclass does no hashing itself.
  * Like [[ValueCodec]], the built-in codecs decode strictly — non-canonical bytes fail with a [[sage.SageException.DecodeError]].
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
    * Derives a key codec for `B` from a total, lossless mapping — typically a newtype over an existing key type. Caution: this is the
    * escape hatch around the deliberate absence of float/boolean key givens; mapping onto a representation-sensitive type reintroduces the
    * hazard that two writers silently address different keys, so keep `f`/`g` canonical and total.
    */
  final def imap[B](f: A => B)(g: B => A): KeyCodec[B] =
    KeyCodec.from[B](b => self.encode(g(b)))(bytes => self.decode(bytes).map(f))

  /**
    * Derives a key codec for `B` whose decode may fail. A `Left` keeps the strict, no-coercion contract.
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
    * Builds a key codec from an encode/decode pair. The decode returns `Either` so a custom codec rejects bad input rather than throwing.
    */
  def from[A](enc: A => Bytes)(dec: Bytes => Either[DecodeError, A]): KeyCodec[A] = instance(enc, dec)

  /**
    * UTF-8 text; decoding rejects malformed UTF-8.
    */
  given string: KeyCodec[String] = instance(Bytes.utf8, Primitives.decodeUtf8)

  /**
    * Decimal `Int`; decoding rejects non-numeric or out-of-range input.
    */
  given int: KeyCodec[Int] = instance(Primitives.encodeNumber, Primitives.decodeNumber("Int", Primitives.parseInt))

  /**
    * Decimal `Long`; decoding rejects non-numeric or out-of-range input.
    */
  given long: KeyCodec[Long] = instance(Primitives.encodeNumber, Primitives.decodeNumber("Long", Primitives.parseLong))

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

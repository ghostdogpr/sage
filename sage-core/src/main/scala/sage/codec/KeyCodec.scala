package sage.codec

import sage.Bytes
import sage.SageException.DecodeError

/**
  * Encodes/decodes a user type at a key or hash-field position — an identifier into the keyspace or a hash, never a payload (use
  * [[ValueCodec]] for those). Deliberately unrelated to [[ValueCodec]] so given resolution stays unambiguous, and float/boolean types
  * are excluded because their formatting is representation-sensitive — two writers must not silently address different keys or fields.
  * Cluster-slot hashing is a property of key positions only ([[sage.commands.Command.keyIndices]]); this typeclass does no hashing itself.
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

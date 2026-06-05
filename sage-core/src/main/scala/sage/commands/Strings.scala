package sage.commands

import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

/**
  * String-family commands.
  */
object Strings {

  /**
    * `GET key`: the value, or `None` if the key does not exist.
    */
  def get[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] = {
    val encodedKey = keyCodec.encode(key)
    Command(
      "GET",
      keys = Vector(encodedKey),
      args = Vector(encodedKey),
      decode = {
        case Frame.Null              => Right(None)
        case Frame.BulkString(value) => valueCodec.decode(value).map(Some(_))
        case other                   => Left(DecodeError("bulk string or null", Frame.describe(other)))
      }
    )
  }

  /**
    * `SET key value` (the plain two-argument form; options come with later slices).
    */
  def set[K, V](key: K, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Unit] = {
    val encodedKey = keyCodec.encode(key)
    Command(
      "SET",
      keys = Vector(encodedKey),
      args = Vector(encodedKey, valueCodec.encode(value)),
      decode = {
        case Frame.SimpleString("OK") => Right(())
        case other                    => Left(DecodeError("simple string 'OK'", Frame.describe(other)))
      }
    )
  }
}

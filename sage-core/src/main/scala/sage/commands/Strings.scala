package sage.commands

import sage.SageException.DecodeError
import sage.codec.{KeyCodec, ValueCodec}
import sage.protocol.Frame

object Strings {

  def get[K, V](key: K)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Option[V]] =
    Command(
      "GET",
      keyIndices = Command.FirstKey,
      args = Vector(keyCodec.encode(key)),
      decode = {
        case Frame.Null              => Right(None)
        case Frame.BulkString(value) => valueCodec.decode(value).map(Some(_))
        case other                   => Left(DecodeError("bulk string or null", Frame.describe(other)))
      }
    )

  def set[K, V](key: K, value: V)(using keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]): Command[Unit] =
    Command(
      "SET",
      keyIndices = Command.FirstKey,
      args = Vector(keyCodec.encode(key), valueCodec.encode(value)),
      decode = {
        case Frame.SimpleString("OK") => Right(())
        case other                    => Left(DecodeError("simple string 'OK'", Frame.describe(other)))
      }
    )
}

package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.codec.KeyCodec
import sage.protocol.Frame

object Connection {

  val multi: Command[Unit] = Command("MULTI", keyIndices = Command.NoKeys, args = Vector.empty, decode = Decode.ok)

  // the reply (an array of per-command results, or a null array on WATCH abort) is interpreted by the runtime, not this passthrough decoder
  val exec: Command[Frame] = Command("EXEC", keyIndices = Command.NoKeys, args = Vector.empty, decode = frame => Right(frame))

  val unwatch: Command[Unit] = Command("UNWATCH", keyIndices = Command.NoKeys, args = Vector.empty, decode = Decode.ok)

  def watch[K](first: K, rest: K*)(using keyCodec: KeyCodec[K]): Command[Unit] = {
    val keys = (first +: rest.toVector).map(keyCodec.encode)
    Command("WATCH", keyIndices = Vector.range(0, keys.length), args = keys, decode = Decode.ok)
  }

  def ping(message: Option[String] = None): Command[String] =
    Command(
      "PING",
      keyIndices = Command.NoKeys,
      args = message.map(Bytes.utf8).toVector,
      decode = {
        case Frame.SimpleString(value) => Right(value)
        case Frame.BulkString(value)   => Right(value.asUtf8String)
        case other                     => Left(DecodeError("simple or bulk string", Frame.describe(other)))
      }
    )

  /**
    * The protocol handshake. Unknown reply entries are ignored for forward compatibility.
    */
  def hello(auth: Option[(String, String)] = None): Command[HelloReply] =
    Command(
      "HELLO",
      keyIndices = Command.NoKeys,
      args = Bytes.utf8("3") +: auth.toVector.flatMap { case (username, password) => Vector("AUTH", username, password).map(Bytes.utf8) },
      decode = HelloReply.decode
    )
}

package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame

object Connection {

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

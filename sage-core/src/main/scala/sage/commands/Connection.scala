package sage.commands

import sage.Bytes
import sage.SageException.DecodeError
import sage.protocol.Frame

/**
  * Connection-family commands.
  */
object Connection {

  /**
    * `PING` (replies `PONG`) or `PING message` (echoes the message).
    */
  def ping(message: Option[String] = None): Command[String] =
    Command(
      "PING",
      keys = Vector.empty,
      args = message.map(Bytes.utf8).toVector,
      decode = {
        case Frame.SimpleString(value) => Right(value)
        case Frame.BulkString(value)   => Right(value.asUtf8String)
        case other                     => Left(DecodeError("simple or bulk string", Frame.describe(other)))
      }
    )

  /**
    * `HELLO 3`, optionally with `AUTH username password` (the protocol handshake, ADR-0002). Decodes only the fields the handshake and
    * telemetry need; unknown reply entries are ignored for forward compatibility.
    */
  def hello(auth: Option[(String, String)] = None): Command[HelloReply] =
    Command(
      "HELLO",
      keys = Vector.empty,
      args = Bytes.utf8("3") +: auth.toVector.flatMap { case (username, password) => Vector("AUTH", username, password).map(Bytes.utf8) },
      decode = HelloReply.decode
    )
}

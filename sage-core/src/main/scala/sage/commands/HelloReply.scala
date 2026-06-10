package sage.commands

import sage.SageException.DecodeError
import sage.protocol.Frame

final case class HelloReply(server: String, version: String, proto: Int, role: String)

object HelloReply {

  private[commands] def decode(frame: Frame): Either[DecodeError, HelloReply] =
    frame match {
      case Frame.Map(entries) =>
        val fields = entries.collect {
          case (Frame.BulkString(key), value)   => key.asUtf8String -> value
          case (Frame.SimpleString(key), value) => key              -> value
        }.toMap
        for {
          server  <- string(fields, "server")
          version <- string(fields, "version")
          proto   <- proto3(fields)
          role    <- string(fields, "role")
        } yield HelloReply(server, version, proto, role)
      case other              =>
        Left(DecodeError("map", Frame.describe(other)))
    }

  private def string(fields: Map[String, Frame], name: String): Either[DecodeError, String] =
    fields.get(name) match {
      case Some(Frame.BulkString(value))   => Right(value.asUtf8String)
      case Some(Frame.SimpleString(value)) => Right(value)
      case Some(other)                     => Left(DecodeError(s"string for '$name'", Frame.describe(other)))
      case None                            => Left(DecodeError(s"map entry '$name'", "nothing"))
    }

  // RESP3 is the only supported protocol, so any other proto value is a decode failure.
  private def proto3(fields: Map[String, Frame]): Either[DecodeError, Int] =
    fields.get("proto") match {
      case Some(Frame.Integer(3))     => Right(3)
      case Some(Frame.Integer(value)) => Left(DecodeError("proto 3", s"proto $value"))
      case Some(other)                => Left(DecodeError("integer for 'proto'", Frame.describe(other)))
      case None                       => Left(DecodeError("map entry 'proto'", "nothing"))
    }
}

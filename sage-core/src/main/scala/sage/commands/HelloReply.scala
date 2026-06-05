package sage.commands

import sage.SageException.DecodeError
import sage.protocol.Frame

final case class HelloReply(server: String, version: String, proto: Int, role: String)

object HelloReply {

  private[commands] def decode(frame: Frame): Either[DecodeError, HelloReply] =
    frame match {
      case Frame.Map(entries) =>
        val fields = entries.flatMap {
          case (Frame.BulkString(key), value)   => Some(key.asUtf8String -> value)
          case (Frame.SimpleString(key), value) => Some(key -> value)
          case _                                => None
        }.toMap
        for {
          server  <- string(fields, "server")
          version <- string(fields, "version")
          proto   <- integer(fields, "proto")
          role    <- string(fields, "role")
        } yield HelloReply(server, version, proto.toInt, role)
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

  private def integer(fields: Map[String, Frame], name: String): Either[DecodeError, Long] =
    fields.get(name) match {
      case Some(Frame.Integer(value)) => Right(value)
      case Some(other)                => Left(DecodeError(s"integer for '$name'", Frame.describe(other)))
      case None                       => Left(DecodeError(s"map entry '$name'", "nothing"))
    }
}

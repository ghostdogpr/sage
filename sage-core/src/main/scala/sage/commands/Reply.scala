package sage.commands

import sage.SageException
import sage.SageException.ServerError
import sage.protocol.Frame

/**
  * Converts a raw reply frame into a command's typed result. Error frames are intercepted here, at the top level only: errors nested in
  * aggregates (an `EXEC` reply) still reach decoders.
  */
object Reply {

  def run[Out](command: Command[Out], frame: Frame): Either[SageException, Out] =
    frame match {
      case Frame.SimpleError(message) => Left(ServerError(message))
      case Frame.BulkError(message)   => Left(ServerError(message.asUtf8String))
      case other                      => command.decode(other)
    }
}

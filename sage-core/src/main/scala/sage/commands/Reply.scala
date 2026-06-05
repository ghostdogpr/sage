package sage.commands

import sage.SageException
import sage.SageException.ServerError
import sage.protocol.Frame

/**
  * The single choke-point converting a raw top-level reply frame into a command's typed result.
  *
  * Error frames are intercepted here — at the top level only — so per-command decoders stay mechanical and never repeat the error branch.
  * Error frames nested inside aggregates (e.g. per-command results in an `EXEC` reply) flow through to the decoders that want them.
  */
object Reply {

  def run[Out](command: Command[Out], frame: Frame): Either[SageException, Out] =
    frame match {
      case Frame.SimpleError(message) => Left(ServerError(message))
      case Frame.BulkError(message)   => Left(ServerError(message.asUtf8String))
      case other                      => command.decode(other)
    }
}

package sage.commands

import scala.util.{Failure, Try}
import scala.util.control.NonFatal

import sage.SageException
import sage.SageException.{DecodeError, ServerError}
import sage.protocol.Frame

/**
  * Converts a raw reply frame into a command's typed result. Error frames are intercepted here, at the top level only: errors nested in
  * aggregates (an `EXEC` reply) still reach decoders.
  */
private[sage] object Reply {

  /**
    * The decode boundary every transport uses: a throwing codec is caught and wrapped as a [[DecodeError]] (keeping the cause) rather than
    * escaping as a raw throwable.
    */
  def decode[Out](command: Command[Out], frame: Frame): Try[Out] =
    try
      frame match {
        case Frame.SimpleError(message) => Failure(ServerError.of(message))
        case Frame.BulkError(message)   => Failure(ServerError.of(message.asUtf8String))
        case other                      => command.decode(other).toTry
      }
    catch {
      case error: SageException => Failure(error)
      case NonFatal(error)      => Failure(DecodeError.fromThrowable(error))
    }
}

package sage.commands

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import sage.SageException
import sage.SageException.{DecodeError, ServerError}
import sage.protocol.Frame

/**
  * Converts a raw reply frame into a command's typed result. Error frames are intercepted here, at the top level only: errors nested in
  * aggregates (an `EXEC` reply) still reach decoders.
  */
private[sage] object Reply {

  def run[Out](command: Command[Out], frame: Frame): Either[SageException, Out] =
    frame match {
      case Frame.SimpleError(message) => Left(ServerError.of(message))
      case Frame.BulkError(message)   => Left(ServerError.of(message.asUtf8String))
      case other                      => command.decode(other)
    }

  /**
    * The decode boundary every transport uses: a throwing codec is caught and wrapped as a [[DecodeError]] (keeping the cause) rather than
    * escaping as a raw throwable.
    */
  def decode[Out](command: Command[Out], frame: Frame): Try[Out] =
    try
      run(command, frame) match {
        case Right(value) => Success(value)
        case Left(error)  => Failure(error)
      }
    catch {
      case error: SageException => Failure(error)
      case NonFatal(error)      => Failure(DecodeError.fromThrowable(error))
    }
}

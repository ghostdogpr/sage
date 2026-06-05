package sage

/**
  * Every sage failure, in one sealed hierarchy so users can match exhaustively (ADR-0009). Not an enum: case types appear standalone in
  * signatures, and enum case constructors widen to the enum type.
  */
sealed abstract class SageException(message: String) extends Exception(message)

object SageException {

  /**
    * Malformed RESP3 on the wire; the connection must be discarded.
    */
  final case class ProtocolError(message: String) extends SageException(message)

  final case class DecodeError(expected: String, actual: String) extends SageException(s"expected $expected, got $actual")

  final case class ServerError(message: String) extends SageException(message)

  final case class ConnectionLost(mayHaveExecuted: Boolean)
    extends SageException(
      if (mayHaveExecuted) "connection lost with the command in flight: it may have executed"
      else "connection lost before the command was sent"
    )

  final case class NotConnected() extends SageException("not connected")

  final case class CrossSlot(message: String) extends SageException(message)

  final case class TimedOut(message: String) extends SageException(message)
}

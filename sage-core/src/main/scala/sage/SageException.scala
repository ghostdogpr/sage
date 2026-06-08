package sage

/**
  * Every sage failure, in one sealed hierarchy so users can match exhaustively. Not an enum: case types appear standalone in
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

  /**
    * The server rejected `HELLO 3`: it predates RESP3 (Redis < 6.0) or is a RESP2-only proxy.
    */
  final case class UnsupportedServer(message: String) extends SageException(message)

  final case class CrossSlot(message: String) extends SageException(message)

  final case class TimedOut(message: String) extends SageException(message)

  /**
    * A transaction was discarded server-side because a command could not be queued (`EXECABORT`): nothing ran. Distinct from an
    * execution-phase error, which leaves the other commands committed (Redis does not roll back) and surfaces per-position.
    */
  final case class TransactionDiscarded(message: String) extends SageException(message)
}

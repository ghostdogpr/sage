package sage

/**
  * The single sealed hierarchy for every sage failure, so users can handle classes of failure exhaustively.
  *
  * All cases live here in the core — including the connection-level ones only the runtime raises — because `sealed` confines a hierarchy to
  * one file (ADR-0009). Every case is pure data; defining them requires no I/O.
  */
sealed abstract class SageException(message: String) extends Exception(message)

object SageException {

  /**
    * Malformed RESP3 on the wire. The connection can no longer be trusted and must be discarded.
    */
  final case class ProtocolError(message: String) extends SageException(message)

  /**
    * A reply arrived but could not be converted to the expected type.
    */
  final case class DecodeError(expected: String, actual: String) extends SageException(s"expected $expected, got $actual")

  /**
    * An error reply from the server.
    */
  final case class ServerError(message: String) extends SageException(message)

  /**
    * The connection dropped while the command was in flight: it may or may not have executed (ADR-0006).
    */
  final case class ConnectionLost(mayHaveExecuted: Boolean)
    extends SageException(
      if (mayHaveExecuted) "connection lost with the command in flight: it may have executed"
      else "connection lost before the command was sent"
    )

  /**
    * The command was submitted while the client was disconnected; it was not sent (fail fast, ADR-0006).
    */
  final case class NotConnected() extends SageException("not connected")

  /**
    * A Transaction's keys span multiple cluster slots; it cannot be executed atomically.
    */
  final case class CrossSlot(message: String) extends SageException(message)

  /**
    * An operation did not complete in time (e.g. the connection watchdog gave up on a node).
    */
  final case class TimedOut(message: String) extends SageException(message)
}

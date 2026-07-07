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

  /**
    * A reply could not be decoded into the expected type: the wire value was well-formed RESP3 but not the shape the command's decoder or a
    * codec required (the built-in codecs decode strictly). `expected` names the shape that was wanted, `actual` what arrived.
    */
  final case class DecodeError(expected: String, actual: String) extends SageException(s"expected $expected, got $actual")

  object DecodeError {

    def fromThrowable(error: Throwable): DecodeError = {
      val wrapped = DecodeError("a value the codec could decode", s"the codec threw $error")
      wrapped.initCause(error)
      wrapped
    }
  }

  /**
    * An error reply from the server. `code` is the leading token Redis/Valkey put on every error (`WRONGTYPE`, `NOSCRIPT`, `BUSYGROUP`,
    * the generic `ERR`, …), split out so callers can branch on it — `case ServerError("WRONGTYPE", _)` — without parsing the message
    * themselves. `detail` is the rest; for a single-token error it is empty. Build from a raw wire message with [[ServerError.of]].
    */
  final case class ServerError(code: String, detail: String) extends SageException(if (detail.isEmpty) code else s"$code $detail")

  object ServerError {

    def of(raw: String): ServerError =
      raw.indexOf(' ') match {
        case -1 => ServerError(raw, "")
        case i  => ServerError(raw.substring(0, i), raw.substring(i + 1))
      }
  }

  /**
    * The initial connection could not be established (host unreachable, refused, or connect timeout). Distinct from [[ConnectionLost]], a
    * live connection dropping around a command.
    */
  final case class ConnectionFailed(message: String) extends SageException(message)

  /**
    * The connection dropped around this command. `mayHaveExecuted` is true when it was already in flight — the server may or may not have
    * applied it, so a non-idempotent command is not safe to blindly retry; false means it was never sent and retrying is safe.
    */
  final case class ConnectionLost(mayHaveExecuted: Boolean)
    extends SageException(
      if (mayHaveExecuted) "connection lost with the command in flight: it may have executed"
      else "connection lost before the command was sent"
    )

  /**
    * The client is not connected: it was never started, or it has been closed.
    */
  final case class NotConnected() extends SageException("not connected")

  /**
    * The server rejected `HELLO 3`: it predates RESP3 (Redis < 6.0) or is a RESP2-only proxy.
    */
  final case class UnsupportedServer(message: String) extends SageException(message)

  /**
    * TLS could not be established: the certificate was rejected (wrong trust material or a hostname mismatch), or the configured trust
    * material itself was unusable.
    */
  final case class TlsError(message: String) extends SageException(message)

  /**
    * A multi-key command or transaction touched keys in more than one cluster slot, which the server cannot serve atomically.
    */
  final case class CrossSlot(message: String) extends SageException(message)

  /**
    * A blocking command or transaction waited past `dedicatedPool.acquireTimeout` for a free pooled connection. Not a per-command timeout.
    */
  final case class TimedOut(message: String) extends SageException(message)

  /**
    * A transaction was discarded server-side because a command could not be queued (`EXECABORT`): nothing ran. Distinct from an
    * execution-phase error, which leaves the other commands committed (Redis does not roll back) and surfaces per-position.
    */
  final case class TransactionDiscarded(message: String) extends SageException(message)

  /**
    * `cached` was given a command that cannot be safely cached: a write, or a read with no key. A keyless read could never be evicted by
    * an invalidation push (only by TTL), so it is rejected rather than silently allowed to go stale.
    */
  final case class NotCacheable(message: String) extends SageException(message)
}

package sage

/**
  * The sealed hierarchy for Sage failures. The standalone case types can appear in signatures and support exhaustive matching.
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
    * the generic `ERR`, …). Callers can match the code directly, as in `case ServerError("WRONGTYPE", _)`. `detail` contains the remaining
    * text and is empty for single-token errors. Build from a raw server message with [[ServerError.of]].
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
    * The connection dropped around this command. When `mayHaveExecuted` is true, the command was in flight and may have run. Retry
    * non-idempotent commands only when the value is false.
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
    * The server cannot serve what this client requires: it rejected `HELLO 3` (predates RESP3, or is a RESP2-only proxy), or a cluster
    * client was pointed at a server that is not part of a formed cluster.
    */
  final case class UnsupportedServer(message: String) extends SageException(message)

  /**
    * TLS could not be established: the certificate was rejected (wrong trust material or a hostname mismatch), or the configured trust
    * material itself was unusable.
    */
  final case class TlsError(message: String) extends SageException(message)

  /**
    * An unsupported multi-key command or a transaction touched keys in more than one cluster slot, which the server cannot serve atomically.
    * Supported multi-slot commands are handled transparently outside transactions and therefore do not produce this error solely for spanning
    * slots.
    */
  final case class CrossSlot(message: String) extends SageException(message)

  /**
    * A wait exceeded its configured limit. This applies when a blocking command or transaction waits longer than
    * `dedicatedPool.acquireTimeout` for a pooled connection, or when a topology probe (`ROLE` or `CLUSTER SLOTS`) exceeds `connectTimeout`.
    * Regular commands do not use this timeout.
    */
  final case class TimedOut(message: String) extends SageException(message)

  /**
    * The server discarded a transaction because a command could not be queued (`EXECABORT`). The transaction did not run. Execution-phase
    * errors are different: Redis commits the other commands and reports the error for its individual position.
    */
  final case class TransactionDiscarded(message: String) extends SageException(message)

  /**
    * `cached` received a write or a read without a key. The server cannot invalidate a keyless read when data changes, which could leave a
    * stale cached value.
    */
  final case class NotCacheable(message: String) extends SageException(message)

  /**
    * An argument rejected by the API: an invalid configuration or rate-limit policy, a blocking command in a pipeline or transaction,
    * or a command a cluster client cannot serve as routed (an all-masters command in a cluster pipeline, a cluster-wide result in a
    * single-node transaction, declared key positions falling outside a hand-built command's arguments). This indicates a programming error
    * that the caller should fix.
    */
  final case class InvalidArgument(message: String) extends SageException(message)
}

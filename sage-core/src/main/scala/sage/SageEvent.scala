package sage

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import sage.cluster.Node

/**
  * A runtime observability signal reported to a [[SageListener]]: a command completion, connection transition, cache result, or topology change.
  * The sealed hierarchy supports exhaustive matching. Events omit command arguments and payloads, keeping credentials and user values out of
  * listeners. Pub/sub `Message` values and `StreamEntry` records are separate types.
  */
sealed trait SageEvent

object SageEvent {

  /**
    * Reports a completed user command with its name, final node (`None` for standalone and all-master commands), client-observed duration, and
    * outcome. The duration includes cluster redirects and retries. A locally served cached read produces only [[Cache.Hit]]. A cache miss
    * produces [[Cache.Miss]] and a `CommandCompleted` after the server request finishes.
    */
  final case class CommandCompleted(name: String, node: Option[Node], duration: FiniteDuration, outcome: Outcome) extends SageEvent

  /**
    * The multiplexed connection's lifecycle. `Connected` is reported for the initial connection and every successful reconnect.
    * `Disconnected` is reported when a live connection is lost unexpectedly and the runtime begins reconnecting. Failed reconnect attempts
    * report [[Connection.ReconnectFailed]] without reporting another `Disconnected`. Graceful close is not reported. `node` identifies the server for
    * cluster and master-replica clients and is `None` for standalone.
    */
  sealed trait Connection extends SageEvent {
    def node: Option[Node]
  }

  object Connection {
    final case class Connected(node: Option[Node])    extends Connection
    final case class Disconnected(node: Option[Node]) extends Connection

    /**
      * Reports a failed reconnect attempt and its cause, such as a rejected password, unsupported server, or invalid TLS material. The runtime
      * continues retrying with backoff. Error values omit credentials. Initial connection failures are reported directly to the caller instead.
      */
    final case class ReconnectFailed(node: Option[Node], error: Throwable) extends Connection

    /**
      * An attempt to establish a connection to a specific node, or to qualify it during topology discovery, failed. Names the address and cause,
      * and may accompany a failure returned to the caller or one handled internally. Distinct from [[ReconnectFailed]], which concerns restoring
      * an already-established connection. Repeated failures for the same node are collapsed until it establishes a pooled connection.
      */
    final case class ConnectFailed(node: Option[Node], error: Throwable) extends Connection
  }

  /**
    * A client-side caching outcome for a `cached` read, by command name. `Hit` is a read served without a server round trip — either from a
    * stored entry or by coalescing onto an in-flight fetch; `Miss` is a read that issued the server fetch.
    */
  sealed trait Cache extends SageEvent {
    def command: String
  }

  object Cache {
    final case class Hit(command: String)  extends Cache
    final case class Miss(command: String) extends Cache
  }

  /**
    * The cluster's slot-owning master set changed (failover, or scaling a shard in or out). Reported only when that set actually differs,
    * not on every topology refresh. A reshard that moves slots between the same masters does not report this event.
    */
  final case class TopologyChanged(masters: Vector[Node]) extends SageEvent
}

/**
  * A command's terminal result, stripped of its value: it either succeeded or failed with the error the caller saw.
  */
enum Outcome {
  case Succeeded
  case Failed(error: Throwable)
}

object Outcome {

  /**
    * Converts a completed `Try` to [[Outcome.Succeeded]] or to [[Outcome.Failed]] with its error.
    */
  def of(result: Try[?]): Outcome =
    result match {
      case Success(_) => Succeeded
      case Failure(e) => Failed(e)
    }
}

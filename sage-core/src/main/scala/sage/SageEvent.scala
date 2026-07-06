package sage

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import sage.cluster.Node

/**
  * A runtime observability signal reported to a [[SageListener]]: a command completion, a connection lifecycle transition, a cache hit or
  * miss, or a topology change. A sealed hierarchy so listeners match exhaustively. An Event carries no command arguments or payloads, so
  * secrets (`HELLO`/`AUTH` credentials, user values) never reach a listener. Distinct from a `Message` (a pub/sub delivery) and a
  * `StreamEntry` (a record in a Stream) — those are never Events.
  */
sealed trait SageEvent

object SageEvent {

  /**
    * One logical user command settled: its name, the node that produced the final result (`None` on standalone, or for an all-masters
    * command), the client-observed duration (from acceptance to settlement, folding in any cluster redirects/retries), and its outcome. A
    * cached read that hits the local cache yields only a [[Cache.Hit]] and no `CommandCompleted` — it touched no server; a cached miss yields
    * a [[Cache.Miss]] and, when its server fetch settles, a `CommandCompleted`.
    */
  final case class CommandCompleted(name: String, node: Option[Node], duration: FiniteDuration, outcome: Outcome) extends SageEvent

  /**
    * The Multiplexed Connection's lifecycle. `Connected` fires on the initial connect and on every successful reconnect; `Disconnected`
    * fires when a live connection is lost unexpectedly and the runtime begins reconnecting. Graceful close and individual reconnect attempts
    * are not reported. `node` is `Some` in cluster (the master this connection serves) and `None` on standalone.
    */
  sealed trait Connection extends SageEvent {
    def node: Option[Node]
  }

  object Connection {
    final case class Connected(node: Option[Node])    extends Connection
    final case class Disconnected(node: Option[Node]) extends Connection

    /**
      * A reconnect attempt failed to establish, carrying the cause so a permanent failure (a rejected password, an unsupported server, bad
      * TLS material) is visible rather than looking like an endless generic reconnect. The runtime still backs off and retries — the server
      * side may recover. The error carries no credentials (a `WRONGPASS` names no password). Not fired for the initial connect, whose failure
      * the caller already sees.
      */
    final case class ReconnectFailed(node: Option[Node], error: Throwable) extends Connection
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
    * not on every topology refresh; a reshard that moves slots between the same masters does not fire it.
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
    * The outcome of a settled `Try`: [[Outcome.Succeeded]] on success, [[Outcome.Failed]] carrying the error otherwise.
    */
  def of(result: Try[?]): Outcome =
    result match {
      case Success(_) => Succeeded
      case Failure(e) => Failed(e)
    }
}

package sage.client

import java.nio.file.Path
import javax.net.ssl.SSLContext

import scala.concurrent.duration.*

/**
  * Exponential reconnect backoff with full jitter (a random wait in `[0, base]`), spreading the reconnect storm across clients.
  */
final case class BackoffConfig(
  initialDelay: FiniteDuration = 50.millis,
  maxDelay: FiniteDuration = 5.seconds,
  multiplier: Double = 2.0
)

/**
  * Idle-PING liveness check. `pingTimeout` is a death detector, not a latency SLA — size it above the slowest healthy reply.
  */
final case class WatchdogConfig(
  pingInterval: FiniteDuration = 60.seconds,
  pingTimeout: FiniteDuration = 30.seconds,
  enabled: Boolean = true
)

/**
  * The on-demand pool of Dedicated Connections for blocking commands. `acquireTimeout` bounds only the wait for a free slot, never a
  * command's own block timeout; idle connections are evicted after `idleTimeout` (`Duration.Inf` keeps them forever).
  */
final case class DedicatedPoolConfig(
  maxConnections: Int = 8,
  acquireTimeout: FiniteDuration = 5.seconds,
  idleTimeout: Duration = 30.seconds
)

/**
  * Credentials sent via `HELLO 3 AUTH`. Legacy `requirepass` is the default user with a password, hence the `"default"` username.
  */
final case class AuthConfig(password: String, username: String = "default") {
  // keep the secret out of logs and error messages that print a SageConfig
  override def toString: String = s"AuthConfig(username=$username, password=<redacted>)"
}

/**
  * Where TLS finds the certificates it trusts. The handshake verifies the server's hostname in every mode except [[TrustSource.Insecure]].
  */
sealed trait TrustSource
object TrustSource {

  case object System extends TrustSource

  final case class TrustStore(path: Path, password: Option[String] = None) extends TrustSource {
    // keep the keystore password out of logs and error messages that print a SageConfig
    override def toString: String = s"TrustStore($path, password=${password.fold("None")(_ => "<redacted>")})"
  }

  final case class Pem(path: Path) extends TrustSource

  // the escape hatch, and the path for mutual TLS via the caller's own key managers
  final case class Custom(context: SSLContext) extends TrustSource

  // development only: trusts every certificate and skips hostname verification, so it is open to machine-in-the-middle attacks
  case object Insecure extends TrustSource
}

final case class TlsConfig(trust: TrustSource = TrustSource.System)

/**
  * A server address. In cluster mode the seeds are contacted to discover the topology; thereafter the cluster's own reported node
  * addresses are used.
  */
final case class Endpoint(host: String, port: Int = 6379)

/**
  * Cluster tuning. `maxRedirects` bounds how many `MOVED`/`ASK` hops a single command follows before failing (the same default as
  * lettuce); `minRefreshInterval` throttles topology refreshes so a redirect storm triggers at most one `CLUSTER SLOTS` per window.
  */
final case class ClusterConfig(
  maxRedirects: Int = 5,
  minRefreshInterval: FiniteDuration = 5.seconds
)

/**
  * Standalone connects to one server (`SageConfig.host`/`port`); cluster discovers its topology from `seeds` and routes every command to
  * the owning node. The client type is the same either way — only this selects the runtime.
  */
enum Topology {
  case Standalone
  case Cluster(seeds: Vector[Endpoint], config: ClusterConfig = ClusterConfig())
}

final case class SageConfig(
  host: String = "localhost",
  port: Int = 6379,
  connectTimeout: FiniteDuration = 10.seconds,
  reconnect: BackoffConfig = BackoffConfig(),
  watchdog: WatchdogConfig = WatchdogConfig(),
  closeTimeout: FiniteDuration = 5.seconds,
  dedicatedPool: DedicatedPoolConfig = DedicatedPoolConfig(),
  auth: Option[AuthConfig] = None,
  tls: Option[TlsConfig] = None,
  topology: Topology = Topology.Standalone
)

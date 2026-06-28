package sage.client

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import javax.net.ssl.SSLContext

import scala.concurrent.duration.*

import sage.{CommandTracer, SageListener}

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

  /**
    * Server-trust only: the trust store supplies the certificates the client verifies the server against, never client key material. A
    * server that demands a client certificate (mutual TLS) fails the handshake under this source — use [[Custom]] to supply key managers.
    */
  final case class TrustStore(path: Path, password: Option[String] = None) extends TrustSource {
    // keep the keystore password out of logs and error messages that print a SageConfig
    override def toString: String = s"TrustStore($path, password=${password.fold("None")(_ => "<redacted>")})"
  }

  /**
    * Server-trust only, like [[TrustStore]]: a PEM of trusted certificates, never client key material. Mutual TLS must go through [[Custom]].
    */
  final case class Pem(path: Path) extends TrustSource

  // the escape hatch, and the path for mutual TLS via the caller's own key managers
  final case class Custom(context: SSLContext) extends TrustSource

  // development only: trusts every certificate and skips hostname verification, so it is open to machine-in-the-middle attacks
  case object Insecure extends TrustSource
}

final case class TlsConfig(trust: TrustSource = TrustSource.System)

/**
  * Pub/sub tuning. `bufferSize` bounds each subscription's message buffer; when it fills, the reader draining the Subscription Connection
  * blocks, so TCP backpressures the publisher (lossless). A slow consumer then stalls its peer subscriptions, never command traffic.
  */
final case class PubSubConfig(bufferSize: Int = 128)

/**
  * Client-side caching tuning. When `enabled`, the Multiplexed Connection enables RESP3 opt-in tracking at bootstrap and `cached` reads are
  * served locally; `maxBytes` caps the approximate retained size of each connection generation's cache, evicting least-recently-used
  * entries so a single large value can never blow the budget. Set `enabled = false` for environments where ACLs or a proxy permit `HELLO`
  * and ordinary commands but deny `CLIENT TRACKING` — `cached` then runs the read without caching, keeping the call portable.
  */
final case class CacheConfig(enabled: Boolean = true, maxBytes: Long = 64L * 1024 * 1024)

/**
  * A server address. The standalone address and each cluster seed are both an Endpoint. In cluster mode the seeds are contacted to discover
  * the topology; thereafter the cluster's own reported node addresses are used.
  */
final case class Endpoint(host: String = "localhost", port: Int = 6379)

/**
  * Cluster tuning. `maxRedirects` bounds how many `MOVED`/`ASK` hops a single command follows before failing (the same default as
  * lettuce); `minRefreshInterval` throttles topology refreshes so a redirect storm triggers at most one `CLUSTER SLOTS` per window.
  */
final case class ClusterConfig(
  maxRedirects: Int = 5,
  minRefreshInterval: FiniteDuration = 5.seconds
)

/**
  * Master-replica tuning. `minRefreshInterval` throttles role re-discovery (`ROLE`/`INFO replication`) so a burst of `READONLY`s or
  * reconnects triggers at most one discovery per window. There is no periodic poll: roles refresh only on reconnect and on a `READONLY`
  * from the presumed master.
  */
final case class MasterReplicaConfig(
  minRefreshInterval: FiniteDuration = 5.seconds
)

/**
  * Which Node a read-only command may run on, the same setting for both cluster and master-replica deployments. Only read-only,
  * non-blocking commands are eligible; writes, blocking reads, transactions, and `cached` reads always go to the master regardless of the
  * policy. `Master` (the default) is today's behavior — every command to the master. `MasterPreferred` falls back to a replica only when the
  * master is unreachable; `Replica` reads only from replicas and fails when none is reachable; `ReplicaPreferred` reads from a replica, else
  * the master. Reads served by a replica may lag the master — that staleness is the policy's accepted contract.
  */
enum ReadFrom {
  case Master, MasterPreferred, Replica, ReplicaPreferred
}

/**
  * Standalone connects to the one server at `endpoint`; cluster discovers its topology from `seeds` and routes every command to the owning
  * node; master-replica discovers one master and its replicas from `seeds` (by asking each its `ROLE`) and routes reads per the Read Policy.
  * The address lives here, inside the topology, so there is one place it can come from. The client type is the same in every case — only
  * this selects the runtime.
  */
enum Topology {
  case Standalone(endpoint: Endpoint = Endpoint())
  case Cluster(seeds: Vector[Endpoint], config: ClusterConfig = ClusterConfig())
  case MasterReplica(seeds: Vector[Endpoint], config: MasterReplicaConfig = MasterReplicaConfig())
}

/**
  * The full client configuration. Sub-tuning lives in the dedicated config types referenced below; the fields here are the top-level
  * knobs, all with sensible defaults so `SageConfig()` connects to a local standalone server.
  *
  * @param connectTimeout how long to wait for a connection (and its `HELLO 3` setup) to complete before failing
  * @param reconnect      exponential reconnect backoff — see [[BackoffConfig]]
  * @param watchdog       idle-connection liveness checking — see [[WatchdogConfig]]
  * @param closeTimeout   how long [[sage.client.internal.Client.close]] waits for in-flight commands to drain before forcing the close
  * @param dedicatedPool  the pool backing blocking commands and transactions — see [[DedicatedPoolConfig]]
  * @param pubsub         pub/sub buffering — see [[PubSubConfig]]
  * @param clientCache    client-side caching — see [[CacheConfig]]
  * @param auth           credentials for `HELLO 3 AUTH`; `None` connects unauthenticated — see [[AuthConfig]]
  * @param tls            TLS settings; `None` connects in plaintext — see [[TlsConfig]]
  * @param topology       standalone, cluster, or master-replica, and where to find the server(s) — see [[Topology]]
  * @param readFrom       which node read-only commands may run on — see [[ReadFrom]]
  * @param database       the logical keyspace `SELECT`ed once at setup and fixed for the client's lifetime (re-issued on every reconnect and
  *                       every connection); never changed by a runtime command, since that would move the keyspace under every fiber sharing
  *                       a connection. Must be 0 for a cluster topology, which has only database 0
  * @param clientName     sets `CLIENT SETNAME`, visible in `CLIENT LIST`/`CLIENT INFO`; the library name and version are announced automatically
  * @param listeners      observers of runtime [[sage.SageEvent]]s — see [[sage.SageListener]]
  * @param tracer         an optional distributed tracer, driven synchronously on the command path so its spans nest under the caller's active
  *                       span — see [[sage.CommandTracer]]. `None` (the default) emits no spans
  */
final case class SageConfig(
  connectTimeout: FiniteDuration = 10.seconds,
  reconnect: BackoffConfig = BackoffConfig(),
  watchdog: WatchdogConfig = WatchdogConfig(),
  closeTimeout: FiniteDuration = 5.seconds,
  dedicatedPool: DedicatedPoolConfig = DedicatedPoolConfig(),
  pubsub: PubSubConfig = PubSubConfig(),
  clientCache: CacheConfig = CacheConfig(),
  auth: Option[AuthConfig] = None,
  tls: Option[TlsConfig] = None,
  topology: Topology = Topology.Standalone(),
  readFrom: ReadFrom = ReadFrom.Master,
  database: Int = 0,
  clientName: Option[String] = None,
  listeners: Vector[SageListener] = Vector.empty,
  tracer: Option[CommandTracer] = None
)

object SageConfig {

  /**
    * Parses a `redis://`/`rediss://` connection URI into a config. `rediss` selects TLS with system trust. Userinfo becomes auth
    * (`redis://user:pass@…`, or `redis://:pass@…` for the default user), percent-decoded so a managed-service password like `p%40ss`
    * authenticates as `p@ss`. A single host yields a standalone topology; comma-separated hosts
    * (`redis://h1:6379,h2:6380`) yield cluster seeds. A `/<db>` path sets `database`, rejected for a cluster URI (cluster has only db 0).
    * Other tuning stays programmatic: `fromUri(…).map(_.copy(watchdog = …))`. Returns the problem as a `Left` rather than throwing; there
    * is intentionally no way to select insecure TLS from a URI.
    */
  def fromUri(uri: String): Either[String, SageConfig] = {
    def fail[A](msg: String): Either[String, A] = Left(s"invalid redis URI '$uri': $msg")

    uri.split("://", 2) match {
      case Array(scheme, rest) =>
        for {
          tls       <- scheme.toLowerCase match {
                         case "redis"  => Right(None)
                         case "rediss" => Right(Some(TlsConfig()))
                         case other    => fail[Option[TlsConfig]](s"unsupported scheme '$other' (expected redis or rediss)")
                       }
          _         <- if (rest.contains("?")) fail[Unit]("query parameters are not supported; set other options via .copy") else Right(())
          slash      = rest.indexOf('/')
          authority  = if (slash < 0) rest else rest.substring(0, slash)
          pathPart   = if (slash < 0) "" else rest.substring(slash + 1)
          at         = authority.lastIndexOf('@')
          userinfo   = if (at < 0) "" else authority.substring(0, at)
          auth      <- if (userinfo.isEmpty) Right(None) else parseAuth(uri, userinfo)
          endpoints <- parseEndpoints(uri, if (at < 0) authority else authority.substring(at + 1))
          db        <- if (pathPart.isEmpty) Right(0)
                       else pathPart.toIntOption.filter(_ >= 0).toRight(s"invalid redis URI '$uri': invalid database '$pathPart'")
          topology   = endpoints match {
                         case Vector(one) => Topology.Standalone(one)
                         case seeds       => Topology.Cluster(seeds)
                       }
          _         <- topology match {
                         case _: Topology.Cluster if db != 0 => fail[Unit]("cluster URIs cannot select a database (cluster has only db 0)")
                         case _                              => Right(())
                       }
        } yield SageConfig(topology = topology, auth = auth, tls = tls, database = db)
      case _                   => fail("expected redis:// or rediss://")
    }
  }

  // split on the first literal ':' (a ':' inside a credential is percent-encoded as %3A), then decode each half
  private def parseAuth(uri: String, userinfo: String): Either[String, Option[AuthConfig]] =
    userinfo.indexOf(':') match {
      case -1 => percentDecode(uri, "password", userinfo).map(pw => Some(AuthConfig(password = pw)))
      case i  =>
        for {
          user <- percentDecode(uri, "username", userinfo.substring(0, i))
          pw   <- percentDecode(uri, "password", userinfo.substring(i + 1))
        } yield Some(AuthConfig(password = pw, username = if (user.isEmpty) "default" else user))
    }

  // RFC 3986 percent-decoding of a single URI component: decode %XX byte by byte, pass everything else (including '+') through literally —
  // '+' is form-encoding's space, not a URI component's, so java.net.URLDecoder is wrong here. %XX bytes are reassembled and read as UTF-8.
  private def percentDecode(uri: String, label: String, component: String): Either[String, String] =
    if (component.indexOf('%') < 0) Right(component)
    else {
      val out  = new ByteArrayOutputStream(component.length)
      var i    = 0
      var fail = false
      while (i < component.length && !fail)
        component.charAt(i) match {
          case '%' if i + 2 < component.length && isHex(component.charAt(i + 1)) && isHex(component.charAt(i + 2)) =>
            out.write(Integer.parseInt(component.substring(i + 1, i + 3), 16)); i += 3
          case '%'                                                                                                 => fail = true
          case c if c < 128                                                                                        => out.write(c.toInt); i += 1
          case c                                                                                                   => out.write(c.toString.getBytes(StandardCharsets.UTF_8)); i += 1
        }
      if (fail) Left(s"invalid redis URI '$uri': malformed percent-encoding in $label")
      else Right(new String(out.toByteArray, StandardCharsets.UTF_8))
    }

  private def isHex(c: Char): Boolean = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')

  private def parseEndpoints(uri: String, hosts: String): Either[String, Vector[Endpoint]] =
    hosts.split(",").toVector.foldRight(Right(Vector.empty): Either[String, Vector[Endpoint]]) { (hostPort, acc) =>
      acc.flatMap { rest =>
        val colon           = hostPort.lastIndexOf(':')
        val (host, portStr) = if (colon < 0) (hostPort, "") else (hostPort.substring(0, colon), hostPort.substring(colon + 1))
        if (host.isEmpty) Left(s"invalid redis URI '$uri': empty host in '$hostPort'")
        else if (portStr.isEmpty) Right(Endpoint(host) +: rest)
        else
          portStr.toIntOption.filter(p => p >= 1 && p <= 65535) match {
            case Some(port) => Right(Endpoint(host, port) +: rest)
            case None       => Left(s"invalid redis URI '$uri': invalid port in '$hostPort'")
          }
      }
    }
}

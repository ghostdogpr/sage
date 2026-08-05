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
  * Connection liveness check. The client reconnects when its oldest pending command has waited for `pingTimeout`. When the connection is idle,
  * it sends a PING after `pingInterval`, and that PING uses the same timeout. Set `pingTimeout` above the slowest healthy reply time.
  */
final case class WatchdogConfig(
  pingInterval: FiniteDuration = 60.seconds,
  pingTimeout: FiniteDuration = 30.seconds,
  enabled: Boolean = true
)

/**
  * The on-demand pool of dedicated connections for blocking commands. `acquireTimeout` limits how long a command waits for a free pool slot.
  * The command's own blocking timeout is configured separately. Idle connections are removed after `idleTimeout`; `Duration.Inf` keeps them.
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
    * Uses the trust store to verify server certificates. It does not provide a client certificate. For mutual TLS, use [[Custom]] with key
    * managers.
    */
  final case class TrustStore(path: Path, password: Option[String] = None) extends TrustSource {
    // keep the keystore password out of logs and error messages that print a SageConfig
    override def toString: String = s"TrustStore($path, password=${password.fold("None")(_ => "<redacted>")})"
  }

  /**
    * Uses trusted certificates from a PEM file to verify the server. It does not provide a client certificate; use [[Custom]] for mutual TLS.
    */
  final case class Pem(path: Path) extends TrustSource

  // the escape hatch, and the path for mutual TLS via the caller's own key managers
  final case class Custom(context: SSLContext) extends TrustSource

  // development only: trusts every certificate and skips hostname verification, so it is open to machine-in-the-middle attacks
  case object Insecure extends TrustSource
}

final case class TlsConfig(trust: TrustSource = TrustSource.System)

/**
  * Pub/sub tuning. Each subscription buffers up to `bufferSize` messages. When the buffer is full, the connection pauses reading until the
  * consumer catches up. Sage does not discard messages because this local buffer is full, but messages published while the connection is
  * down can still be lost. Other subscriptions on that connection also wait. Commands use separate connections and are unaffected.
  */
final case class PubSubConfig(bufferSize: Int = 128)

/**
  * Client-side caching tuning. When `enabled`, the Multiplexed Connection enables RESP3 opt-in tracking at bootstrap and `cached` reads are
  * served locally; `maxBytes` caps the approximate retained size of each connection generation's cache, evicting least-recently-used
  * entries. Values larger than the entire budget are not cached. Set `enabled = false` for environments where ACLs or a proxy permit `HELLO`
  * and ordinary commands but deny `CLIENT TRACKING` — `cached` then runs the read without caching, keeping the call portable.
  */
final case class CacheConfig(enabled: Boolean = true, maxBytes: Long = 64L * 1024 * 1024)

/**
  * A server address. The standalone address and each cluster seed are both an Endpoint. In cluster mode the seeds are contacted to discover
  * the topology; thereafter the cluster's own reported node addresses are used.
  */
final case class Endpoint(host: String = "localhost", port: Int = 6379)

/**
  * Cluster tuning. `maxRedirects` limits how many `MOVED` or `ASK` replies a command follows before failing. `minRefreshInterval` limits
  * refreshes triggered by redirects or unowned slots to once per interval. A retry that requires updated topology, such as one after a
  * failover, refreshes immediately and is still limited by `maxRedirects`.
  *
  * `topologyRefreshInterval` polls `CLUSTER SLOTS` on a timer and is off by default. Redirects and faults trigger refreshes for changes that
  * affect commands. Polling can also discover changes that do not cause a command failure, such as a replica added to a healthy shard.
  */
final case class ClusterConfig(
  maxRedirects: Int = 5,
  minRefreshInterval: FiniteDuration = 5.seconds,
  topologyRefreshInterval: Option[FiniteDuration] = None
)

/**
  * Master-replica tuning. `minRefreshInterval` limits role re-discovery (`ROLE`/`INFO replication`) to once per interval during a burst of
  * `READONLY` replies, reconnects, or replica-preferred reads when no replica is known. `topologyRefreshInterval` also runs discovery on a
  * timer and is off by default. Polling can find a replica added while the known nodes remain healthy.
  */
final case class MasterReplicaConfig(
  minRefreshInterval: FiniteDuration = 5.seconds,
  topologyRefreshInterval: Option[FiniteDuration] = None
)

/**
  * Controls where eligible reads run in cluster and master-replica deployments. Eligible reads are read-only and non-blocking. Writes,
  * blocking reads, transactions, and cached reads use the master.
  *
  * `Master` always uses the master. `MasterPreferred` tries the master first and uses a replica if the master is unavailable. `Replica` uses
  * replicas and fails if none is available. `ReplicaPreferred` tries replicas first and then the master. A replica may return older data than
  * the master.
  */
enum ReadFrom {
  case Master, MasterPreferred, Replica, ReplicaPreferred
}

/**
  * Selects the server topology. `Standalone` connects to one endpoint. `Cluster` discovers nodes from its seeds and routes commands by key.
  * `MasterReplica` discovers node roles from its seeds, sends writes to the master, and applies [[ReadFrom]] to eligible reads.
  */
enum Topology {
  case Standalone(endpoint: Endpoint = Endpoint())
  case Cluster(seeds: Vector[Endpoint], config: ClusterConfig = ClusterConfig())
  case MasterReplica(seeds: Vector[Endpoint], config: MasterReplicaConfig = MasterReplicaConfig())
}

/**
  * The full client configuration. The dedicated configuration sections below contain settings for individual features. The fields here are
  * the top-level settings, with defaults that let `SageConfig()` connect to a local standalone server.
  *
  * @param connectTimeout how long to wait for a connection (and its `HELLO 3` setup) to complete before failing
  * @param reconnect      exponential reconnect backoff — see [[BackoffConfig]]
  * @param watchdog       connection liveness checking — see [[WatchdogConfig]]
  * @param closeTimeout   how long [[sage.client.internal.Client.close]] waits for in-flight commands to finish before forcing the close
  * @param dedicatedPool  the pool backing blocking commands and transactions — see [[DedicatedPoolConfig]]
  * @param pubsub         pub/sub buffering — see [[PubSubConfig]]
  * @param clientCache    client-side caching — see [[CacheConfig]]
  * @param auth           credentials for `HELLO 3 AUTH`; `None` connects unauthenticated — see [[AuthConfig]]
  * @param tls            TLS settings; `None` connects in plaintext — see [[TlsConfig]]
  * @param topology       standalone, cluster, or master-replica, and where to find the server(s) — see [[Topology]]
  * @param readFrom       which node read-only commands may run on — see [[ReadFrom]]
  * @param database       the logical keyspace selected during connection setup and selected again for every reconnect and new connection. It
  *                       remains fixed for the client's lifetime because connections are shared by concurrent operations. Valkey 9+ supports
  *                       numbered databases in cluster mode; Redis and older Valkey versions reject a non-zero database during setup
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
    * (`redis://h1:6379,h2:6380`) yield cluster seeds. A `/<db>` path sets `database`; when the endpoints form a cluster, the server must support
    * numbered cluster databases (Valkey 9+). Other tuning stays programmatic: `fromUri(…).map(_.copy(watchdog = …))`. Returns the problem as a
    * `Left` rather than throwing; there is intentionally no way to select insecure TLS from a URI.
    */
  def fromUri(uri: String): Either[String, SageConfig] = {
    val shown                                   = redactCredentials(uri)
    def fail[A](msg: String): Either[String, A] = Left(s"invalid redis URI '$shown': $msg")

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
          auth      <- if (userinfo.isEmpty) Right(None) else parseAuth(shown, userinfo)
          endpoints <- parseEndpoints(shown, if (at < 0) authority else authority.substring(at + 1))
          db        <- if (pathPart.isEmpty) Right(0)
                       else pathPart.toIntOption.filter(_ >= 0).toRight(s"invalid redis URI '$shown': invalid database '$pathPart'")
          topology   = endpoints match {
                         case Vector(one) => Topology.Standalone(one)
                         case seeds       => Topology.Cluster(seeds)
                       }
        } yield SageConfig(topology = topology, auth = auth, tls = tls, database = db)
      case _                   => fail("expected redis:// or rediss://")
    }
  }

  private def redactCredentials(uri: String): String =
    uri.lastIndexOf('@') match {
      case -1 => uri
      case at =>
        val prefix    = uri.substring(0, at)
        val schemeEnd = prefix.indexOf("://") match {
          case -1 => 0
          case i  => i + 3
        }
        val userinfo  = prefix.substring(schemeEnd)
        val redacted  = userinfo.indexOf(':') match {
          case -1 => "<redacted>"
          case i  => s"${userinfo.substring(0, i)}:<redacted>"
        }
        s"${prefix.substring(0, schemeEnd)}$redacted${uri.substring(at)}"
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
            out.write(Integer.parseInt(component.substring(i + 1, i + 3), 16))
            i += 3
          case '%'                                                                                                 => fail = true
          case c if c < 128                                                                                        =>
            out.write(c.toInt)
            i += 1
          case c                                                                                                   =>
            out.write(c.toString.getBytes(StandardCharsets.UTF_8))
            i += 1
        }
      if (fail) Left(s"invalid redis URI '$uri': malformed percent-encoding in $label")
      else Right(new String(out.toByteArray, StandardCharsets.UTF_8))
    }

  private def isHex(c: Char): Boolean = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')

  private def parseEndpoints(uri: String, hosts: String): Either[String, Vector[Endpoint]] =
    // limit -1 keeps trailing empty tokens (a stray comma), so a missing seed reaches parseEndpoint and is rejected rather than dropped
    hosts.split(",", -1).toVector.foldRight(Right(Vector.empty): Either[String, Vector[Endpoint]]) { (token, acc) =>
      acc.flatMap(rest => parseEndpoint(uri, token).map(_ +: rest))
    }

  // an IPv6 literal must be bracketed (`[::1]`/`[::1]:6379`), brackets stripped from the host; a bare `::1` or empty port is rejected, not guessed
  private def parseEndpoint(uri: String, token: String): Either[String, Endpoint] = {
    def fail(reason: String): Either[String, Endpoint]                    = Left(s"invalid redis URI '$uri': $reason in '$token'")
    def withPort(host: String, portStr: String): Either[String, Endpoint] =
      if (portStr.isEmpty) fail("empty port")
      else
        portStr.toIntOption.filter(p => p >= 1 && p <= 65535) match {
          case Some(port) => Right(Endpoint(host, port))
          case None       => fail("invalid port")
        }
    if (token.startsWith("[")) {
      val close = token.indexOf(']')
      if (close < 0) fail("unterminated IPv6 literal")
      else {
        val host = token.substring(1, close)
        val tail = token.substring(close + 1)
        if (host.isEmpty) fail("empty host")
        else if (tail.isEmpty) Right(Endpoint(host))
        else if (tail.startsWith(":")) withPort(host, tail.substring(1))
        else fail("expected ':port' after the IPv6 literal")
      }
    } else
      token.indexOf(':') match {
        case -1    => if (token.isEmpty) fail("empty host") else Right(Endpoint(token))
        case colon =>
          val host = token.substring(0, colon)
          if (host.isEmpty) fail("empty host") else withPort(host, token.substring(colon + 1))
      }
  }
}

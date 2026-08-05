package sage.examples.ce

import cats.effect.IO

import sage.*
import sage.backend.*

/**
  * Connects to a TLS-enabled server that requires an ACL username and password. This example uses the system trust store. Use
  * `TrustSource.Pem` or `TrustStore` for a private certificate authority, and use `Insecure` only during development. The example expects the
  * server to listen on port 6380 with the `app` user configured.
  */
object TlsExample {

  private val config =
    SageConfig(
      topology = Topology.Standalone(Endpoint("localhost", 6380)),
      tls = Some(TlsConfig(TrustSource.System)),
      auth = Some(AuthConfig(username = "app", password = "app-secret"))
    )

  def run: IO[String] =
    SageClient.resource(config).use(_.ping())
}
